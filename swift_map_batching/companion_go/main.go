// Go companion for the swift_map_batching scenario: the batcher and writer from
// companion/main.cpp re-expressed with the Flow Go SDK.
//
// Both computations are registered here; only the reader stays native in the stock
// flow_server worker. The behaviour mirrors the C++ functions exactly:
//
//   - batcher (hosted by TSwiftMapCompanionComputation) merges one key's messages of the
//     epoch into a single output carrying their comma-joined event ids. The merged output has
//     as many parents as the key had messages, which the swift map accepts only under
//     allow_batching_with_relaxed_guarantees.
//   - writer (hosted by TTransformCompanionComputation) explodes a batched message back into
//     one row per event id, tagging each with the size of the batch it came out of.
//
// As in the Python SDK — and unlike the C++ keyed-batch adapter — OnMessages gets the
// request's whole message batch, keys mixed, with all of it as the default parent set. So the
// batcher groups by key itself and binds each merged output to exactly its key group through
// the collector out.WithParentIDs returns.
//
// Two determinism obligations of swift hosting, and how they are met:
//
//   - The server may call user functions concurrently across requests (the documented
//     contract), so the functions keep no mutable state — every call works only on its own
//     batch and its own collectors.
//   - Within a request the output must be a pure function of the batch, groups and parents
//     in a reproducible order — and Go map iteration is randomized. The grouping therefore
//     never ranges over a map: keys are emitted in first-appearance order kept in a slice,
//     and each group's parent ids and event ids stay in batch order.
//
// The same binary is also the launcher: with no Flow env vars set, pipeline.Run() acts as the
// runner — it injects the registered stream schemas into the spec, points the CompanionManager
// resource at itself, ships itself into the worker's vanilla job, and execs flow_server.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"go.ytsaurus.tech/yt/go/flow"
)

// eventMessage is an input on the "event_in" stream: one event routed by group_key.
type eventMessage struct {
	flow.YSONMessage
	EventID  int64  `yson:"event_id"`
	GroupKey uint64 `yson:"group_key"`
}

// batchedMessage is an output on the "event_batched" stream: the comma-joined event ids of
// one key's epoch batch.
type batchedMessage struct {
	flow.YSONMessage
	EventIDs string `yson:"event_ids"`
}

// sinkEventMessage is an output on the "sink_event" stream: one row per event, tagged with
// the size of the batch it came out of — the only place the merging is visible downstream.
type sinkEventMessage struct {
	flow.YSONMessage
	EventID   int64 `yson:"event_id"`
	BatchSize int64 `yson:"batch_size"`
}

// batcher merges the epoch's messages of one key into a single output message carrying their
// event ids.
type batcher struct{}

var _ flow.BatchFunction = (*batcher)(nil)

func (*batcher) OnMessages(
	ctx context.Context,
	rt flow.Runtime,
	msgs []flow.ExtendedMessage,
	out flow.OutputCollector,
) error {
	// First-appearance order in a slice, never a map range: a swift replay of the same batch
	// must produce the same groups with the same parent sequences, or the merged outputs get
	// fresh MessageIds and the downstream dedup misses them.
	var order []uint64
	groups := map[uint64][]flow.ExtendedMessage{}
	for _, msg := range msgs {
		key, err := msg.Key.Uint64("group_key")
		if err != nil {
			return err
		}
		if _, ok := groups[key]; !ok {
			order = append(order, key)
		}
		groups[key] = append(groups[key], msg)
	}

	for _, key := range order {
		group := groups[key]
		parentIDs := make([]string, len(group))
		eventIDs := make([]string, len(group))
		for i, msg := range group {
			var input eventMessage
			if err := msg.ConvertTo(&input); err != nil {
				return err
			}
			parentIDs[i] = msg.ID
			eventIDs[i] = strconv.FormatInt(input.EventID, 10)
		}

		output := flow.NewYSONMessage[batchedMessage]("event_batched")
		output.EventIDs = strings.Join(eventIDs, ",")
		message, err := flow.ConvertFrom(rt, output)
		if err != nil {
			return err
		}
		// A fresh collector per key group: its parents are that key's messages, nothing else.
		// The default collector (parents = the whole mixed-key batch) stays unused.
		out.WithParentIDs(parentIDs...).AddMessage(message)
	}
	return nil
}

// writer explodes a batched message back into one message per event id.
type writer struct{}

var _ flow.RowFunction = (*writer)(nil)

func (*writer) OnMessage(
	ctx context.Context,
	rt flow.Runtime,
	msg flow.ExtendedMessage,
	out flow.OutputCollector,
) error {
	var input batchedMessage
	if err := msg.ConvertTo(&input); err != nil {
		return err
	}

	var eventIDs []int64
	for _, token := range strings.Split(input.EventIDs, ",") {
		if token == "" {
			continue
		}
		eventID, err := strconv.ParseInt(token, 10, 64)
		if err != nil {
			return err
		}
		eventIDs = append(eventIDs, eventID)
	}

	for _, eventID := range eventIDs {
		output := flow.NewYSONMessage[sinkEventMessage]("sink_event")
		output.EventID = eventID
		output.BatchSize = int64(len(eventIDs))
		message, err := flow.ConvertFrom(rt, output)
		if err != nil {
			return err
		}
		out.AddMessage(message)
	}
	return nil
}

func main() {
	pipeline := flow.NewPipeline()
	pipeline.AddStreams(
		flow.NewYSONStream[eventMessage]("event_in"),
		flow.NewYSONStream[batchedMessage]("event_batched"),
		flow.NewYSONStream[sinkEventMessage]("sink_event"),
	)
	pipeline.Add(
		flow.NewBatchComputation("batcher", &batcher{}),
		flow.NewRowComputation("writer", &writer{}),
	)

	if err := pipeline.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "swift_map_batching_go: %v\n", err)
		os.Exit(1)
	}
}
