// Offline proof of the reducer logic through flowtest.Harness — no cluster needed. The
// per-key counting, the re-emit, the accumulation over prior state, the first-batch shape
// (no state row yet — the internal-state analogue of the external-state null-column case),
// and the deterministic first-appearance grouping order are pinned here; the live run then
// only re-checks the same asserts against the real tables.
package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/flow"
	"go.ytsaurus.tech/yt/go/flow/flowtest"
)

func eventSchema() flow.Schema {
	return flowtest.Schema("key:string", "data:string")
}

// keySchema mirrors the live grouping key farm_hash(key), key.
func keySchema() flow.Schema {
	return flowtest.Schema("hash:uint64", "key:string")
}

// eventKey stands in for the live farm_hash: any deterministic hash works offline because
// the key itself is part of the grouping key.
func eventKey(h *flowtest.Harness, key string) flow.Payload {
	return h.Key(flowtest.Row{"hash": uint64(len(key)), "key": key})
}

func newReducerHarness(t *testing.T) *flowtest.Harness {
	return flowtest.New(t, flow.NewBatchComputation("Reducer", &reduce{}), flowtest.Options{
		Streams: map[string]flow.Schema{
			"event": eventSchema(),
			"out":   eventSchema(),
		},
		KeySchema:      keySchema(),
		InternalStates: []string{stateName},
	})
}

func rowsOf(t *testing.T, r *flowtest.Response, streamID string) []flowtest.Row {
	t.Helper()

	var rows []flowtest.Row
	for _, m := range r.MessagesOn(streamID) {
		rows = append(rows, flowtest.ToRow(m.Payload))
	}
	return rows
}

func stateOf(t *testing.T, r *flowtest.Response, key flow.Payload) reducerState {
	t.Helper()

	var state reducerState
	require.True(t, r.InternalStateYSON(stateName, key, &state))
	return state
}

// TestReducerCountsAndReemits pins the whole per-message contract on a mixed-key batch:
// every message re-emitted with its payload intact, and per key count == group size with
// last_data from the group's last message.
func TestReducerCountsAndReemits(t *testing.T) {
	h := newReducerHarness(t)
	alpha := eventKey(h, "alpha")
	beta := eventKey(h, "be")

	r := h.Process(
		h.KeyedMessage("event", alpha, flowtest.Row{"key": "alpha", "data": "a1"}),
		h.KeyedMessage("event", beta, flowtest.Row{"key": "be", "data": "b1"}),
		h.KeyedMessage("event", alpha, flowtest.Row{"key": "alpha", "data": "a2"}),
	)

	// Output is grouped in first-appearance key order, input order within a key.
	require.Equal(t, []flowtest.Row{
		{"key": "alpha", "data": "a1"},
		{"key": "alpha", "data": "a2"},
		{"key": "be", "data": "b1"},
	}, rowsOf(t, r, "out"))

	require.Equal(t, reducerState{Count: 2, LastData: "a2"}, stateOf(t, r, alpha))
	require.Equal(t, reducerState{Count: 1, LastData: "b1"}, stateOf(t, r, beta))
}

// TestReducerStartsFromZeroWithoutState pins the first-batch shape: a key with no state
// entry yet (the internal-state analogue of the external-state "present row, null column"
// case) must start from the zero value, not fail.
func TestReducerStartsFromZeroWithoutState(t *testing.T) {
	h := newReducerHarness(t)
	key := eventKey(h, "fresh")

	r := h.Process(h.KeyedMessage("event", key, flowtest.Row{"key": "fresh", "data": "d1"}))

	require.Equal(t, reducerState{Count: 1, LastData: "d1"}, stateOf(t, r, key))
}

func TestReducerAccumulatesOverPriorState(t *testing.T) {
	h := newReducerHarness(t)
	key := eventKey(h, "seen")
	h.PutInternalStateYSON(stateName, key, reducerState{Count: 190, LastData: "old"})

	r := h.Process(
		h.KeyedMessage("event", key, flowtest.Row{"key": "seen", "data": "new1"}),
		h.KeyedMessage("event", key, flowtest.Row{"key": "seen", "data": "new2"}),
	)

	require.Equal(t, reducerState{Count: 192, LastData: "new2"}, stateOf(t, r, key))
}

// TestReducerLeavesForeignKeysUntouched pins that only the keys of the batch are written:
// a replayed epoch must not disturb other keys' states.
func TestReducerLeavesForeignKeysUntouched(t *testing.T) {
	h := newReducerHarness(t)
	touched := eventKey(h, "touched")
	foreign := eventKey(h, "foreign")
	h.PutInternalStateYSON(stateName, foreign, reducerState{Count: 7, LastData: "keep"})

	r := h.Process(h.KeyedMessage("event", touched, flowtest.Row{"key": "touched", "data": "d"}))

	require.Equal(t, 1, r.InternalStateLen(stateName))
	_, written := r.InternalStateRaw(stateName, foreign)
	require.False(t, written)
}
