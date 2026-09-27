package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/flow"
	"go.ytsaurus.tech/yt/go/flow/flowtest"
)

func newHarness(t *testing.T) *flowtest.Harness {
	return flowtest.New(t, flow.NewRowComputation("tester", &visitTester{}), flowtest.Options{
		Streams: map[string]flow.Schema{
			"keys":   flowtest.Schema("key:string", "payload:string"),
			"visits": flowtest.Schema("key:string", "payload:string", "visit_index:int64"),
		},
		KeySchema:      flowtest.Schema("hash:uint64", "key:string"),
		InternalStates: []string{userStateName},
	})
}

func keyOf(t *testing.T, h *flowtest.Harness, key string) flow.Payload {
	t.Helper()
	return h.Key(flowtest.Row{"hash": uint64(0), "key": key})
}

func TestMessageStoresThePayloadSilently(t *testing.T) {
	h := newHarness(t)
	key := keyOf(t, h, "k_000")

	r := h.Process(h.KeyedMessage("keys", key, flowtest.Row{"key": "k_000", "payload": "v1_0"}))

	require.Empty(t, r.Messages(), "a message must produce no output")
	var state userState
	require.True(t, r.InternalStateYSON(userStateName, key, &state))
	require.Equal(t, userState{Payload: "v1_0"}, state)
}

func TestVisitEmitsTheStoredPayload(t *testing.T) {
	h := newHarness(t)
	key := keyOf(t, h, "k_000")

	h.Process(h.KeyedMessage("keys", key, flowtest.Row{"key": "k_000", "payload": "v1_0"}))
	r := h.Process(h.Visit(key))

	require.Equal(t, []flowtest.Row{
		{"key": "k_000", "payload": "v1_0", "visit_index": int64(1)},
	}, r.Rows())
}

func TestVisitOfAnUnseededKeyIsSilent(t *testing.T) {
	h := newHarness(t)

	r := h.Process(h.Visit(keyOf(t, h, "k_missing")))

	require.Empty(t, r.Messages(), "a visit of a key with no state must produce no output")
}

func TestLatestVisitCarriesTheLatestPayload(t *testing.T) {
	// The scenario's core assert: v1, a visit, then v2, then the final visit — the visit with
	// the highest visit_index carries the v2 payload.
	h := newHarness(t)
	key := keyOf(t, h, "k_000")

	h.Process(h.KeyedMessage("keys", key, flowtest.Row{"key": "k_000", "payload": "v1_0"}))
	first := h.Process(h.Visit(key))
	h.Process(h.KeyedMessage("keys", key, flowtest.Row{"key": "k_000", "payload": "v2_0"}))
	final := h.Process(h.Visit(key))

	require.Equal(t, []flowtest.Row{
		{"key": "k_000", "payload": "v1_0", "visit_index": int64(1)},
	}, first.Rows())
	require.Equal(t, []flowtest.Row{
		{"key": "k_000", "payload": "v2_0", "visit_index": int64(2)},
	}, final.Rows())
}

func TestPayloadUpdateKeepsTheVisitCounter(t *testing.T) {
	h := newHarness(t)
	key := keyOf(t, h, "k_000")

	h.Process(h.KeyedMessage("keys", key, flowtest.Row{"key": "k_000", "payload": "v1_0"}))
	h.Process(h.Visit(key))
	r := h.Process(h.KeyedMessage("keys", key, flowtest.Row{"key": "k_000", "payload": "v2_0"}))

	var state userState
	require.True(t, r.InternalStateYSON(userStateName, key, &state))
	require.Equal(t, userState{Payload: "v2_0", VisitIndex: 1}, state)
}

func TestKeysAreVisitedApart(t *testing.T) {
	h := newHarness(t)
	a := keyOf(t, h, "k_000")
	b := keyOf(t, h, "k_001")

	h.Process(
		h.KeyedMessage("keys", a, flowtest.Row{"key": "k_000", "payload": "v1_0"}),
		h.KeyedMessage("keys", b, flowtest.Row{"key": "k_001", "payload": "v1_1"}),
	)
	r := h.Process(h.Visit(a), h.Visit(b))

	require.ElementsMatch(t, []flowtest.Row{
		{"key": "k_000", "payload": "v1_0", "visit_index": int64(1)},
		{"key": "k_001", "payload": "v1_1", "visit_index": int64(1)},
	}, r.Rows())
}

func TestMessageAndVisitInOneBatch(t *testing.T) {
	// Messages are dispatched before visits within a batch, so the visit sees the fresh payload.
	h := newHarness(t)
	key := keyOf(t, h, "k_000")

	r := h.Process(
		h.KeyedMessage("keys", key, flowtest.Row{"key": "k_000", "payload": "v1_0"}),
		h.Visit(key),
	)

	require.Equal(t, []flowtest.Row{
		{"key": "k_000", "payload": "v1_0", "visit_index": int64(1)},
	}, r.Rows())
}
