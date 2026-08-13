// Offline proof of the accumulate-and-join logic through flowtest.Harness — no cluster
// needed. The accumulation over empty, null-column and prior state, the join's read path and
// its read-only nature, and every miss shape the joiner can meet are pinned here; the live
// run then only re-checks the same asserts against the real tables.
package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/flow"
	"go.ytsaurus.tech/yt/go/flow/flowtest"
)

func newAccumulatorHarness(t *testing.T) *flowtest.Harness {
	return flowtest.New(t, flow.NewRowComputation("accumulator", &accumulator{}), flowtest.Options{
		Streams: map[string]flow.Schema{
			"events": flowtest.Schema("UserId:string", "Amount:int64"),
			"users":  flowtest.Schema("UserId:string", "Bucket:uint64"),
		},
		KeySchema: flowtest.Schema("Hash:uint64", "UserId:string"),
		ExternalStates: map[string]flow.Schema{
			userTotalStateName: flowtest.Schema("Hash:uint64", "UserId:string", "Total:int64"),
		},
	})
}

func newJoinerHarness(t *testing.T) *flowtest.Harness {
	return flowtest.New(t, flow.NewBatchComputation("joiner", &joiner{}), flowtest.Options{
		Streams: map[string]flow.Schema{
			"users":   flowtest.Schema("UserId:string", "Bucket:uint64"),
			"results": flowtest.Schema("UserId:string", "Total:int64"),
		},
		KeySchema: flowtest.Schema("Hash:uint64", "UserId:string"),
		JoinedExternalStates: map[string]flow.Schema{
			userTotalStateName: flowtest.Schema("Hash:uint64", "UserId:string", "Total:int64"),
		},
	})
}

func userKey(h *flowtest.Harness, userID string) flow.Payload {
	// The live pipeline keys by farm_hash(UserId); any deterministic stand-in works offline
	// because the user id itself is part of the key.
	return h.Key(flowtest.Row{"Hash": uint64(len(userID)), "UserId": userID})
}

func eventOf(h *flowtest.Harness, userID string, amount int64) flow.ExtendedMessage {
	return h.KeyedMessage("events", userKey(h, userID), flowtest.Row{"UserId": userID, "Amount": amount})
}

func userOf(h *flowtest.Harness, userID string) flow.ExtendedMessage {
	return h.KeyedMessage("users", userKey(h, userID), flowtest.Row{"UserId": userID, "Bucket": uint64(0)})
}

func totalsOf(t *testing.T, r *flowtest.Response) map[string]int64 {
	t.Helper()

	totals := map[string]int64{}
	for _, m := range r.MessagesOn("results") {
		userID, err := m.Payload.String("UserId")
		require.NoError(t, err)
		total, err := m.Payload.Int64("Total")
		require.NoError(t, err)
		totals[userID] = total
	}
	return totals
}

func TestAccumulatorStartsFromEmpty(t *testing.T) {
	h := newAccumulatorHarness(t)
	key := userKey(h, "user-0")

	r := h.Process(eventOf(h, "user-0", 10))

	require.Equal(t, flowtest.Row{"Total": int64(10)}, r.ExternalStateRow(userTotalStateName, key))
	require.Equal(t, []flowtest.Row{{"UserId": "user-0", "Bucket": uint64(0)}}, r.Rows())
}

// TestAccumulatorToleratesNullTotal pins the live shape of "no total yet": the state manager
// hands the companion a present row with the key columns set and "Total" null — not an
// absent row, which is how the harness models an unseeded key. The per-column Has check in
// the accumulator is what keeps the live run off the retryable null-value error the
// word_count_sync Go variant looped on.
func TestAccumulatorToleratesNullTotal(t *testing.T) {
	h := newAccumulatorHarness(t)
	key := userKey(h, "user-0")
	h.PutExternalState(userTotalStateName, key, flowtest.Row{"UserId": "user-0"})

	r := h.Process(eventOf(h, "user-0", 10))

	require.EqualValues(t, 10, r.ExternalStateRow(userTotalStateName, key)["Total"])
}

func TestAccumulatorAccumulatesOverPriorState(t *testing.T) {
	h := newAccumulatorHarness(t)
	key := userKey(h, "user-0")
	h.PutExternalState(userTotalStateName, key, flowtest.Row{"UserId": "user-0", "Total": 30})

	r := h.Process(eventOf(h, "user-0", 12))

	require.EqualValues(t, 42, r.ExternalStateRow(userTotalStateName, key)["Total"])
}

func TestJoinerEmitsStoredTotalAndNeverWritesBack(t *testing.T) {
	h := newJoinerHarness(t)
	key := userKey(h, "user-1")
	h.PutJoinedExternalState(userTotalStateName, key, flowtest.Row{"UserId": "user-1", "Total": 20})

	r := h.Process(userOf(h, "user-1"))

	require.Equal(t, map[string]int64{"user-1": 20}, totalsOf(t, r))
	// "Never writes back" is a type-level fact — the joined accessor has no write methods,
	// and "/user_total" is not even declared as an owned external state here — so the run
	// can only leave the joined row exactly as it was seeded.
	require.Equal(t, flowtest.Row{"UserId": "user-1", "Total": int64(20)},
		r.JoinedExternalStateRow(userTotalStateName, key))
}

// TestJoinerReportsAllNullRowAsMissing pins the live shape of "no row in user_totals": the
// worker ships a present row of the table's width with every value column null, and the
// joiner must answer -1, not fail.
func TestJoinerReportsAllNullRowAsMissing(t *testing.T) {
	h := newJoinerHarness(t)
	key := userKey(h, "user-9")
	h.PutJoinedExternalState(userTotalStateName, key, flowtest.Row{"UserId": "user-9"})

	r := h.Process(userOf(h, "user-9"))

	require.Equal(t, map[string]int64{"user-9": -1}, totalsOf(t, r))
}

// TestJoinerToleratesUnshippedState covers the harness-only shape: with no row seeded for
// any batch key, the harness omits the joined state from the request entirely and
// OpenJoinedExternalState returns ErrStateNotRead — where the live worker would ship an
// all-null row instead. The joiner answers -1 either way.
func TestJoinerToleratesUnshippedState(t *testing.T) {
	h := newJoinerHarness(t)

	r := h.Process(userOf(h, "user-9"))

	require.Equal(t, map[string]int64{"user-9": -1}, totalsOf(t, r))
}

func TestJoinerReportsKeyAbsentFromShippedStates(t *testing.T) {
	h := newJoinerHarness(t)
	h.PutJoinedExternalState(userTotalStateName, userKey(h, "user-1"), flowtest.Row{"UserId": "user-1", "Total": 20})

	r := h.Process(userOf(h, "user-1"), userOf(h, "user-9"))

	require.Equal(t, map[string]int64{"user-1": 20, "user-9": -1}, totalsOf(t, r))
}

// TestScenarioEndToEnd pipes the scenario's four input events through the accumulator and
// feeds its emitted users to the joiner over the state rows the accumulator produced, then
// asserts exactly what the live run is verified by: results == user-0..3 → 10/20/30/40 with
// no -1 sentinel.
func TestScenarioEndToEnd(t *testing.T) {
	acc := newAccumulatorHarness(t)
	join := newJoinerHarness(t)

	var events []flow.Input
	for i, amount := range []int64{10, 20, 30, 40} {
		events = append(events, eventOf(acc, fmt.Sprintf("user-%d", i), amount))
	}
	r := acc.Process(events...)

	var users []flow.Input
	for _, m := range r.MessagesOn("users") {
		userID, err := m.Payload.String("UserId")
		require.NoError(t, err)
		state := r.ExternalStateRow(userTotalStateName, userKey(acc, userID))
		require.NotNil(t, state)
		join.PutJoinedExternalState(userTotalStateName, userKey(join, userID),
			flowtest.Row{"UserId": userID, "Total": state["Total"]})
		users = append(users, userOf(join, userID))
	}

	totals := totalsOf(t, join.Process(users...))
	require.Equal(t, map[string]int64{"user-0": 10, "user-1": 20, "user-2": 30, "user-3": 40}, totals)
}
