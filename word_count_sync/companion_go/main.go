// Go companion for the word_count_sync scenario: the reader and the counter from
// companion/main.cpp re-expressed with the Flow Go SDK.
//
// Both computations are registered here; the pipeline binary is the stock flow_server. The
// behaviour mirrors the C++ functions exactly:
//
//   - textRead (hosted by TSwiftOrderedSourceCompanionComputation) splits each input text
//     line into whitespace-separated words and emits one message per word. It is a swift
//     source, so it must be deterministic — strings.Fields is.
//   - wordCount (hosted by TTransformCompanionComputation) drops stop words, routes words
//     shorter than min_word_length into the "skipped" stream (written by the sync sink
//     inside the epoch transaction), and counts the rest in the external state "/state"
//     backed by the word_counts table.
//
// As in the Python variant, the stop words arrive through the computation's spec parameters
// (read via rt.Parameters), not through a companion-hosted resource — the Go SDK registers
// computations and streams only, it has no counterpart of the C++ TPipeline::AddResource.
//
// The server may call the functions concurrently across requests (the documented contract),
// so they keep no mutable state; every call works only on its own message and collectors.
//
// The same binary is also the launcher: with no Flow env vars set, pipeline.Run() acts as the
// runner — it injects the registered stream schemas into the spec, points the CompanionManager
// resource at itself, ships itself into the worker's vanilla job, and execs flow_server.
package main

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"

	"go.ytsaurus.tech/yt/go/flow"
)

// externalStateName is the external state of the counter, backed by the word_counts table.
const externalStateName = "/state"

// textMessage is an input row of the reader's queue source: one line of text.
type textMessage struct {
	flow.YSONMessage
	Text string `yson:"text"`
}

// wordMessage is on the "words" stream: one word of an input line.
type wordMessage struct {
	flow.YSONMessage
	Word string `yson:"word"`
}

// skippedMessage is on the "skipped" stream: a word too short to count, with its length.
type skippedMessage struct {
	flow.YSONMessage
	Word   string `yson:"word"`
	Length int64  `yson:"length"`
}

// textRead splits each input text message into words and emits one message per word.
type textRead struct{}

var _ flow.RowFunction = (*textRead)(nil)

func (*textRead) OnMessage(
	ctx context.Context,
	rt flow.Runtime,
	msg flow.ExtendedMessage,
	out flow.OutputCollector,
) error {
	var input textMessage
	if err := msg.ConvertTo(&input); err != nil {
		return err
	}

	for _, word := range strings.Fields(input.Text) {
		output := flow.NewYSONMessage[wordMessage]("words")
		output.Word = word
		message, err := flow.ConvertFrom(rt, output)
		if err != nil {
			return err
		}
		out.AddMessage(message)
	}
	return nil
}

// wordCount counts word occurrences in external state. Words from the stop_words parameter
// are dropped entirely; of the rest, words shorter than min_word_length are skipped and
// emitted into the "skipped" stream, whose sink writes them into the skipped-words table
// inside the same epoch transaction that commits the counts.
type wordCount struct{}

var _ flow.RowFunction = (*wordCount)(nil)

func (*wordCount) OnMessage(
	ctx context.Context,
	rt flow.Runtime,
	msg flow.ExtendedMessage,
	out flow.OutputCollector,
) error {
	var input wordMessage
	if err := msg.ConvertTo(&input); err != nil {
		return err
	}

	var stopWords []string
	if rt.Parameters().Has("stop_words") {
		if err := rt.Parameters().Get("stop_words", &stopWords); err != nil {
			return err
		}
	}
	if slices.Contains(stopWords, input.Word) {
		return nil
	}

	var minWordLength int64
	if rt.Parameters().Has("min_word_length") {
		if err := rt.Parameters().Get("min_word_length", &minWordLength); err != nil {
			return err
		}
	}
	if int64(len(input.Word)) < minWordLength {
		output := flow.NewYSONMessage[skippedMessage]("skipped")
		output.Word = input.Word
		output.Length = int64(len(input.Word))
		message, err := flow.ConvertFrom(rt, output)
		if err != nil {
			return err
		}
		out.AddMessage(message)
		return nil
	}

	state, err := flow.OpenExternalState(rt, externalStateName, msg)
	if err != nil {
		return err
	}
	// A key with no count yet is not always an absent row: live, the state manager hands
	// the companion a present row with the key columns set and "count" null, so the null
	// check must be per column, as in the C++ variant's optional<i64>.value_or(0).
	var count int64
	if row, ok := state.Get(); ok && row.Has("count") {
		if count, err = row.Int64("count"); err != nil {
			return err
		}
	}
	// Only "count" is set; the state manager fills the key columns from the grouping key,
	// as in the C++ variant.
	row, err := state.Builder().Set("count", count+1).Finish()
	if err != nil {
		return err
	}
	return state.Set(row)
}

func main() {
	pipeline := flow.NewPipeline()
	pipeline.AddStreams(
		flow.NewYSONStream[wordMessage]("words"),
		flow.NewYSONStream[skippedMessage]("skipped"),
	)
	pipeline.Add(
		flow.NewRowSourceComputation("reader", &textRead{}),
		flow.NewRowComputation("counter", &wordCount{}),
	)

	if err := pipeline.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "word_count_sync_go: %v\n", err)
		os.Exit(1)
	}
}
