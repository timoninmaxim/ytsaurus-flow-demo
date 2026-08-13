// Offline proof of the cycle logic through flowtest.Harness — no cluster needed. The routing
// rules of every computation, the reducer's counting over prior external state, the live
// null-count row shape, and a full simulation of the cycle ending at count == 1000 are
// pinned here; the live run then only re-checks the same asserts against the real tables.
package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/flow"
	"go.ytsaurus.tech/yt/go/flow/flowtest"
)

// dataSchema is the one stream schema of the scenario.
func dataSchema() flow.Schema {
	return flowtest.Schema("data:string")
}

func keySchema() flow.Schema {
	return flowtest.Schema("hash:uint64", "data:string")
}

// dataKey mirrors the live grouping key farm_hash(data), data; any deterministic stand-in
// for the hash works offline because the data itself is part of the key.
func dataKey(h *flowtest.Harness, data string) flow.Payload {
	return h.Key(flowtest.Row{"hash": uint64(len(data)), "data": data})
}

// newPassthroughHarness builds a harness for one of the four cycle computations, with its
// live passthrough_rules from the spec. The live sleep_per_message parameters are omitted:
// they only pace the pipeline and would stretch the 1000-message simulation into seconds.
func newPassthroughHarness(t *testing.T, id string, streams []string, rules map[string]string) *flowtest.Harness {
	streamSchemas := map[string]flow.Schema{}
	for _, stream := range streams {
		streamSchemas[stream] = dataSchema()
	}
	return flowtest.New(t, flow.NewRowComputation(id, &passthrough{}), flowtest.Options{
		Streams:    streamSchemas,
		KeySchema:  keySchema(),
		Parameters: map[string]any{"passthrough_rules": rules},
	})
}

func newTransformAHarness(t *testing.T) *flowtest.Harness {
	return newPassthroughHarness(t, "transform_a",
		[]string{"reader_output", "sb1", "ta1", "ta2"},
		map[string]string{"reader_output": "ta1", "sb1": "ta2"})
}

func newReducerHarness(t *testing.T) *flowtest.Harness {
	return flowtest.New(t, flow.NewBatchComputation("reducer", &reduce{}), flowtest.Options{
		Streams:   map[string]flow.Schema{"ta2": dataSchema()},
		KeySchema: keySchema(),
		ExternalStates: map[string]flow.Schema{
			externalStateName: flowtest.Schema("hash:uint64", "data:string", "count:int64"),
		},
	})
}

func dataOf(t *testing.T, r *flowtest.Response, streamID string) []string {
	t.Helper()

	var values []string
	for _, m := range r.MessagesOn(streamID) {
		data, err := m.Payload.String("data")
		require.NoError(t, err)
		values = append(values, data)
	}
	return values
}

func TestReaderRepublishesData(t *testing.T) {
	h := flowtest.New(t, flow.NewRowSourceComputation("reader", &readData{}), flowtest.Options{
		Streams: map[string]flow.Schema{
			"queue":         dataSchema(),
			"reader_output": dataSchema(),
		},
	})

	r := h.Process(h.Message("queue", flowtest.Row{"data": "payload"}))

	require.Equal(t, []string{"payload"}, dataOf(t, r, "reader_output"))
}

// TestTransformARoutesByInputStream pins the heart of the cycle: the same computation sends
// a fresh message once around the loop and releases a returned one to the reducer.
func TestTransformARoutesByInputStream(t *testing.T) {
	h := newTransformAHarness(t)
	key := dataKey(h, "payload")

	r := h.Process(
		h.KeyedMessage("reader_output", key, flowtest.Row{"data": "payload"}),
		h.KeyedMessage("sb1", key, flowtest.Row{"data": "payload"}),
	)

	require.Equal(t, []string{"payload"}, dataOf(t, r, "ta1"))
	require.Equal(t, []string{"payload"}, dataOf(t, r, "ta2"))
}

func TestPassthroughWithoutRuleFails(t *testing.T) {
	// swift_map_a maps only ta1; a message on any other stream must be an error, not a drop.
	h := newPassthroughHarness(t, "swift_map_a",
		[]string{"ta1", "sa1"},
		map[string]string{})

	err := h.ProcessError(h.KeyedMessage("ta1", dataKey(h, "payload"), flowtest.Row{"data": "payload"}))

	require.ErrorContains(t, err, `no passthrough rule for input stream "ta1"`)
}

func TestReducerCountsTheBatch(t *testing.T) {
	h := newReducerHarness(t)
	key := dataKey(h, "payload")

	r := h.Process(
		h.KeyedMessage("ta2", key, flowtest.Row{"data": "payload"}),
		h.KeyedMessage("ta2", key, flowtest.Row{"data": "payload"}),
		h.KeyedMessage("ta2", key, flowtest.Row{"data": "payload"}),
	)

	require.Empty(t, r.Messages())
	require.Equal(t, flowtest.Row{"count": int64(3)}, r.ExternalStateRow(externalStateName, key))
}

// TestReducerToleratesNullCount pins the live shape of "no count yet": the state manager
// hands the companion a present row with the key columns set and "count" null — not an
// absent row, which is how the harness models an unseeded key. The count must be read with
// a per-column Has check, as in the C++ variant's optional<i64>.value_or(0).
func TestReducerToleratesNullCount(t *testing.T) {
	h := newReducerHarness(t)
	key := dataKey(h, "payload")
	h.PutExternalState(externalStateName, key, flowtest.Row{"data": "payload"})

	r := h.Process(h.KeyedMessage("ta2", key, flowtest.Row{"data": "payload"}))

	count := r.ExternalStateRow(externalStateName, key)["count"]
	require.EqualValues(t, 1, count)
}

func TestReducerAccumulatesOverPriorState(t *testing.T) {
	h := newReducerHarness(t)
	key := dataKey(h, "payload")
	h.PutExternalState(externalStateName, key, flowtest.Row{"data": "payload", "count": 990})

	r := h.Process(
		h.KeyedMessage("ta2", key, flowtest.Row{"data": "payload"}),
		h.KeyedMessage("ta2", key, flowtest.Row{"data": "payload"}),
	)

	count := r.ExternalStateRow(externalStateName, key)["count"]
	require.EqualValues(t, 992, count)
}

// TestCycleSimulationEndsAtExactCount drives the scenario's 1000 identical messages through
// all six computations, wired exactly as the spec wires them, in batches of 30 (the live
// max_rows_per_batch): reader → transform_a → swift_map_a → transform_b → swift_map_b →
// transform_a (the cycle closes) → reducer. The state table analogue must end with exactly
// one key whose count is 1000 — the live assertion.
func TestCycleSimulationEndsAtExactCount(t *testing.T) {
	reader := flowtest.New(t, flow.NewRowSourceComputation("reader", &readData{}), flowtest.Options{
		Streams: map[string]flow.Schema{
			"queue":         dataSchema(),
			"reader_output": dataSchema(),
		},
	})
	transformA := newTransformAHarness(t)
	swiftMapA := newPassthroughHarness(t, "swift_map_a",
		[]string{"ta1", "sa1"}, map[string]string{"ta1": "sa1"})
	transformB := newPassthroughHarness(t, "transform_b",
		[]string{"sa1", "tb1"}, map[string]string{"sa1": "tb1"})
	swiftMapB := newPassthroughHarness(t, "swift_map_b",
		[]string{"tb1", "sb1"}, map[string]string{"tb1": "sb1"})
	reducer := newReducerHarness(t)

	const total = 1000
	const batchSize = 30

	// forward re-keys a computation's output messages and feeds them to the next harness.
	forward := func(h *flowtest.Harness, r *flowtest.Response, streamID string) []flow.Input {
		var batch []flow.Input
		for _, data := range dataOf(t, r, streamID) {
			batch = append(batch, h.KeyedMessage(streamID, dataKey(h, data), flowtest.Row{"data": data}))
		}
		return batch
	}

	var finalCount flowtest.Row
	key := dataKey(reducer, "payload")
	for start := 0; start < total; start += batchSize {
		var queued []flow.Input
		for i := start; i < start+batchSize && i < total; i++ {
			queued = append(queued, reader.Message("queue", flowtest.Row{"data": "payload"}))
		}

		read := reader.Process(queued...)
		once := transformA.Process(forward(transformA, read, "reader_output")...)
		require.Empty(t, dataOf(t, once, "ta2"), "a fresh message must go around the loop, not to the reducer")

		slowedA := swiftMapA.Process(forward(swiftMapA, once, "ta1")...)
		crossed := transformB.Process(forward(transformB, slowedA, "sa1")...)
		slowedB := swiftMapB.Process(forward(swiftMapB, crossed, "tb1")...)
		returned := transformA.Process(forward(transformA, slowedB, "sb1")...)
		require.Empty(t, dataOf(t, returned, "ta1"), "a returned message must be released, not looped again")

		reduced := reducer.Process(forward(reducer, returned, "ta2")...)
		finalCount = reduced.ExternalStateRow(externalStateName, key)
	}

	require.Equal(t, flowtest.Row{"count": int64(total)}, finalCount)
}
