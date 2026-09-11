package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/flow"
	"go.ytsaurus.tech/yt/go/flow/flowtest"
)

func newBatcherHarness(t *testing.T) *flowtest.Harness {
	return flowtest.New(t, flow.NewBatchComputation("batcher", &batcher{}), flowtest.Options{
		Streams: map[string]flow.Schema{
			"event_in":      flowtest.Schema("event_id:int64", "group_key:uint64"),
			"event_batched": flowtest.Schema("event_ids:string"),
		},
		KeySchema: flowtest.Schema("hash:uint64", "group_key:uint64"),
	})
}

func newWriterHarness(t *testing.T) *flowtest.Harness {
	return flowtest.New(t, flow.NewRowComputation("writer", &writer{}), flowtest.Options{
		Streams: map[string]flow.Schema{
			"event_batched": flowtest.Schema("event_ids:string"),
			"sink_event":    flowtest.Schema("event_id:int64", "batch_size:int64"),
		},
		KeySchema: flowtest.Schema("hash:uint64"),
	})
}

func event(t *testing.T, h *flowtest.Harness, eventID int64, groupKey uint64) flow.ExtendedMessage {
	t.Helper()
	key := h.Key(flowtest.Row{"hash": groupKey, "group_key": groupKey})
	return h.KeyedMessage("event_in", key, flowtest.Row{"event_id": eventID, "group_key": groupKey})
}

func TestOneKeyMergesIntoOneMessage(t *testing.T) {
	h := newBatcherHarness(t)
	msgs := []flow.ExtendedMessage{
		event(t, h, 0, 7),
		event(t, h, 10, 7),
		event(t, h, 20, 7),
	}

	r := h.Process(msgs[0], msgs[1], msgs[2])

	groups := r.Groups()
	require.Len(t, groups, 1, "one key must merge into exactly one output group")
	require.Equal(t, []string{msgs[0].ID, msgs[1].ID, msgs[2].ID}, groups[0].ParentIDs,
		"the merged output must be parented by exactly the key's messages, in batch order")
	require.Equal(t, []flowtest.Row{{"event_ids": "0,10,20"}}, r.Rows())
}

func TestMixedKeysSplitIntoPerKeyGroups(t *testing.T) {
	// The load-bearing part of the port: OnMessages gets the whole mixed-key batch, so the
	// key grouping and the per-group parent scoping are the batcher's own work.
	h := newBatcherHarness(t)
	msgs := []flow.ExtendedMessage{
		event(t, h, 0, 0),
		event(t, h, 1, 1),
		event(t, h, 10, 0),
		event(t, h, 11, 1),
		event(t, h, 21, 1),
	}

	r := h.Process(msgs[0], msgs[1], msgs[2], msgs[3], msgs[4])

	groups := r.Groups()
	require.Len(t, groups, 2, "two keys must produce two output groups, nothing cross-key")
	require.Equal(t, []string{msgs[0].ID, msgs[2].ID}, groups[0].ParentIDs)
	require.Equal(t, []string{msgs[1].ID, msgs[3].ID, msgs[4].ID}, groups[1].ParentIDs)
	require.Equal(t, []flowtest.Row{{"event_ids": "0,10"}, {"event_ids": "1,11,21"}}, r.Rows())
}

func TestGroupOrderIsFirstAppearanceOrder(t *testing.T) {
	// Swift hosting demands a replay-stable output: same groups, same parent sequences, same
	// order. The grouping must not depend on Go's randomized map iteration, so a batch that
	// interleaves keys starting with the higher one must come out in first-appearance order.
	h := newBatcherHarness(t)
	msgs := []flow.ExtendedMessage{
		event(t, h, 9, 9),
		event(t, h, 4, 4),
		event(t, h, 19, 9),
		event(t, h, 14, 4),
	}

	r := h.Process(msgs[0], msgs[1], msgs[2], msgs[3])

	groups := r.Groups()
	require.Len(t, groups, 2)
	require.Equal(t, []string{msgs[0].ID, msgs[2].ID}, groups[0].ParentIDs,
		"key 9 appeared first, its group must come first")
	require.Equal(t, []string{msgs[1].ID, msgs[3].ID}, groups[1].ParentIDs)
	require.Equal(t, []flowtest.Row{{"event_ids": "9,19"}, {"event_ids": "4,14"}}, r.Rows())
}

func TestWriterExplodesTheBatch(t *testing.T) {
	h := newWriterHarness(t)

	r := h.Process(h.Message("event_batched", flowtest.Row{"event_ids": "5,6,7"}))

	require.Equal(t, []flowtest.Row{
		{"event_id": int64(5), "batch_size": int64(3)},
		{"event_id": int64(6), "batch_size": int64(3)},
		{"event_id": int64(7), "batch_size": int64(3)},
	}, r.Rows())
}

func TestWriterKeepsSingletonBatches(t *testing.T) {
	h := newWriterHarness(t)

	r := h.Process(h.Message("event_batched", flowtest.Row{"event_ids": "42"}))

	require.Equal(t, []flowtest.Row{
		{"event_id": int64(42), "batch_size": int64(1)},
	}, r.Rows())
}

func TestRoundTripPreservesTheEventSet(t *testing.T) {
	// The scenario's core assert in miniature: whatever the batcher merges, the writer
	// restores the exact event_id set, each row carrying its batch size.
	batcherHarness := newBatcherHarness(t)
	writerHarness := newWriterHarness(t)

	var inputs []flow.Input
	for eventID := int64(0); eventID < 10; eventID++ {
		inputs = append(inputs, event(t, batcherHarness, eventID, uint64(eventID%3)))
	}

	merged := batcherHarness.Process(inputs...)

	got := map[int64]int64{}
	for _, row := range merged.Rows() {
		exploded := writerHarness.Process(
			writerHarness.Message("event_batched", flowtest.Row{"event_ids": row["event_ids"]}))
		for _, out := range exploded.Rows() {
			got[out["event_id"].(int64)] = out["batch_size"].(int64)
		}
	}

	// Keys 0 (events 0,3,6,9) and 1/2 (three events each): 10 events, batch sizes 4/3/3.
	require.Equal(t, map[int64]int64{
		0: 4, 3: 4, 6: 4, 9: 4,
		1: 3, 4: 3, 7: 3,
		2: 3, 5: 3, 8: 3,
	}, got)
}
