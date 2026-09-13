// Offline proof of the word-counting logic through flowtest.Harness — no cluster needed.
// The reader's split order, the stop-word filtering, the skipped-words emission and the
// counting over prior external state are pinned here; the live run then only re-checks the
// same asserts against the real tables.
package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/flow"
	"go.ytsaurus.tech/yt/go/flow/flowtest"
)

func newReaderHarness(t *testing.T) *flowtest.Harness {
	return flowtest.New(t, flow.NewRowSourceComputation("reader", &textRead{}), flowtest.Options{
		Streams: map[string]flow.Schema{
			"queue": flowtest.Schema("text:string"),
			"words": flowtest.Schema("word:string"),
		},
	})
}

func newCounterHarness(t *testing.T) *flowtest.Harness {
	return flowtest.New(t, flow.NewRowComputation("counter", &wordCount{}), flowtest.Options{
		Streams: map[string]flow.Schema{
			"words":   flowtest.Schema("word:string"),
			"skipped": flowtest.Schema("word:string", "length:int64"),
		},
		KeySchema: flowtest.Schema("hash:uint64", "word:string"),
		ExternalStates: map[string]flow.Schema{
			externalStateName: flowtest.Schema("hash:uint64", "word:string", "count:int64"),
		},
		Parameters: map[string]any{
			"min_word_length": 4,
			"stop_words":      []string{"flow", "to"},
		},
	})
}

func wordsOf(t *testing.T, r *flowtest.Response, streamID string) []string {
	t.Helper()

	var words []string
	for _, m := range r.MessagesOn(streamID) {
		word, err := m.Payload.String("word")
		require.NoError(t, err)
		words = append(words, word)
	}
	return words
}

func wordKey(h *flowtest.Harness, word string) flow.Payload {
	// The live pipeline keys by farm_hash(word); any deterministic stand-in works offline
	// because the word itself is part of the key.
	return h.Key(flowtest.Row{"hash": uint64(len(word)), "word": word})
}

func TestReaderSplitsInOrder(t *testing.T) {
	h := newReaderHarness(t)

	r := h.Process(h.Message("queue", flowtest.Row{"text": "hello to a world"}))

	require.Equal(t, []string{"hello", "to", "a", "world"}, wordsOf(t, r, "words"))
}

func TestReaderSkipsEmptyTokens(t *testing.T) {
	h := newReaderHarness(t)

	r := h.Process(h.Message("queue", flowtest.Row{"text": " flow\tis  on\nit "}))

	require.Equal(t, []string{"flow", "is", "on", "it"}, wordsOf(t, r, "words"))
}

func TestStopWordIsDroppedEntirely(t *testing.T) {
	h := newCounterHarness(t)

	r := h.Process(
		h.KeyedMessage("words", wordKey(h, "flow"), flowtest.Row{"word": "flow"}),
		h.KeyedMessage("words", wordKey(h, "to"), flowtest.Row{"word": "to"}),
	)

	require.Empty(t, r.Messages())
	require.False(t, r.ExternalStateWritten(externalStateName))
}

func TestShortWordIsSkippedNotCounted(t *testing.T) {
	h := newCounterHarness(t)
	key := wordKey(h, "is")

	r := h.Process(h.KeyedMessage("words", key, flowtest.Row{"word": "is"}))

	skipped := r.MessagesOn("skipped")
	require.Len(t, skipped, 1)
	length, err := skipped[0].Payload.Int64("length")
	require.NoError(t, err)
	require.EqualValues(t, 2, length)
	require.False(t, r.ExternalStateWritten(externalStateName))
}

func TestCountingStartsAtOne(t *testing.T) {
	h := newCounterHarness(t)
	key := wordKey(h, "hello")

	r := h.Process(h.KeyedMessage("words", key, flowtest.Row{"word": "hello"}))

	require.Empty(t, r.Messages())
	require.Equal(t, flowtest.Row{"count": int64(1)}, r.ExternalStateRow(externalStateName, key))
}

// TestCountingToleratesNullCount pins the live shape of "no count yet": the state manager
// hands the companion a present row with the key columns set and "count" null — not an
// absent row, which is how the harness models an unseeded key. The first live deploy
// looped on `flow: null value: column "count"` until the counter learned this.
func TestCountingToleratesNullCount(t *testing.T) {
	h := newCounterHarness(t)
	key := wordKey(h, "hello")
	h.PutExternalState(externalStateName, key, flowtest.Row{"word": "hello"})

	r := h.Process(h.KeyedMessage("words", key, flowtest.Row{"word": "hello"}))

	count := r.ExternalStateRow(externalStateName, key)["count"]
	require.EqualValues(t, 1, count)
}

func TestCountingAccumulatesOverPriorState(t *testing.T) {
	h := newCounterHarness(t)
	key := wordKey(h, "hello")
	h.PutExternalState(externalStateName, key, flowtest.Row{"word": "hello", "count": 41})

	r := h.Process(h.KeyedMessage("words", key, flowtest.Row{"word": "hello"}))

	count := r.ExternalStateRow(externalStateName, key)["count"]
	require.EqualValues(t, 42, count)
}

// TestScenarioEndToEnd pipes the scenario's two input lines through the reader and feeds
// every produced word to the counter, then asserts exactly the two tables the live run is
// verified by: word_counts == {hello:1, world:1}, skipped_words == {a:1, is:2, it:2, on:2}.
func TestScenarioEndToEnd(t *testing.T) {
	reader := newReaderHarness(t)
	counter := newCounterHarness(t)

	counts := map[string]int64{}
	skipped := map[string]int64{}
	for _, line := range []string{"hello to a world", "flow is on it"} {
		words := wordsOf(t, reader.Process(reader.Message("queue", flowtest.Row{"text": line})), "words")

		var batch []flow.Input
		for _, word := range words {
			batch = append(batch, counter.KeyedMessage("words", wordKey(counter, word), flowtest.Row{"word": word}))
		}
		r := counter.Process(batch...)

		for _, m := range r.MessagesOn("skipped") {
			word, err := m.Payload.String("word")
			require.NoError(t, err)
			length, err := m.Payload.Int64("length")
			require.NoError(t, err)
			skipped[word] = length
		}
		for _, word := range words {
			if row := r.ExternalStateRow(externalStateName, wordKey(counter, word)); row != nil {
				counts[word] = row["count"].(int64)
			}
		}
	}

	require.Equal(t, map[string]int64{"hello": 1, "world": 1}, counts)
	require.Equal(t, map[string]int64{"a": 1, "is": 2, "it": 2, "on": 2}, skipped)
}
