// Offline proof of the injection logic through flowtest.Harness — no cluster needed.
// The bounded raise-then-pass behaviour of both failure shapes (returned error and panic),
// the per-row budget isolation and the passthrough are pinned here; the live run then only
// re-checks the engine-telemetry asserts.
//
// The failure budget is process-local by design (see main.go), so every test uses its own
// unique "data" values instead of resetting the shared map — exactly how live fail rows
// stay independent.
package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"go.ytsaurus.tech/yt/go/flow"
	"go.ytsaurus.tech/yt/go/flow/flowtest"
)

const (
	testFailKey      = "1100"
	testPanicKey     = "1101"
	testFailComment  = "TELEMETRY_DEMO_INTENTIONAL_FAIL"
	testFailAttempts = 3
)

func newReaderHarness(t *testing.T) *flowtest.Harness {
	return flowtest.New(t, flow.NewRowSourceComputation("reader", &read{}), flowtest.Options{
		Streams: map[string]flow.Schema{
			"queue": flowtest.Schema("key:string", "data:string"),
			"data":  flowtest.Schema("key:string", "data:string"),
		},
		Parameters: map[string]any{
			"fail_key":      testFailKey,
			"panic_key":     testPanicKey,
			"fail_comment":  testFailComment,
			"fail_attempts": testFailAttempts,
		},
	})
}

func newProcessorHarness(t *testing.T) *flowtest.Harness {
	return flowtest.New(t, flow.NewRowComputation("processor", &drop{}), flowtest.Options{
		Streams: map[string]flow.Schema{
			"data": flowtest.Schema("key:string", "data:string"),
		},
		KeySchema: flowtest.Schema("hash:uint64", "key:string"),
		Parameters: map[string]any{
			"sleep_per_message_ms": 1,
		},
	})
}

func row(key, data string) flowtest.Row {
	return flowtest.Row{"key": key, "data": data}
}

func TestOrdinaryKeyPassesThrough(t *testing.T) {
	h := newReaderHarness(t)

	r := h.Process(h.Message("queue", row("7", "payload-passthrough")))

	messages := r.MessagesOn("data")
	require.Len(t, messages, 1)
	key, err := messages[0].Payload.String("key")
	require.NoError(t, err)
	require.Equal(t, "7", key)
	data, err := messages[0].Payload.String("data")
	require.NoError(t, err)
	require.Equal(t, "payload-passthrough", data)
}

func TestFailKeyErrsExactlyFailAttemptsTimesThenPasses(t *testing.T) {
	h := newReaderHarness(t)
	input := h.Message("queue", row(testFailKey, "fail-budget-row"))

	for attempt := 1; attempt <= testFailAttempts; attempt++ {
		err := h.ProcessError(input)
		require.ErrorContains(t, err, testFailComment, "attempt %d", attempt)
		require.ErrorContains(t, err, "Got fail key "+testFailKey, "attempt %d", attempt)
	}

	r := h.Process(input)
	require.Len(t, r.MessagesOn("data"), 1)
}

func TestPanicKeyPanicsExactlyFailAttemptsTimesThenPasses(t *testing.T) {
	h := newReaderHarness(t)
	input := h.Message("queue", row(testPanicKey, "panic-budget-row"))

	// The recover here stands in for the SDK server's: flowtest calls the computation
	// directly, while live the panic is recovered in ProcessBatch and reported over gRPC.
	processRecovering := func() (recovered any) {
		defer func() { recovered = recover() }()
		h.Process(input)
		return nil
	}

	for attempt := 1; attempt <= testFailAttempts; attempt++ {
		recovered := processRecovering()
		require.NotNil(t, recovered, "attempt %d", attempt)
		require.Contains(t, fmt.Sprint(recovered), testFailComment, "attempt %d", attempt)
		require.Contains(t, fmt.Sprint(recovered), "Got panic key "+testPanicKey, "attempt %d", attempt)
	}

	require.Nil(t, processRecovering())
}

func TestEachFailRowGetsItsOwnBudget(t *testing.T) {
	h := newReaderHarness(t)

	require.ErrorContains(t,
		h.ProcessError(h.Message("queue", row(testFailKey, "budget-row-a"))),
		testFailComment)
	// A different fail row starts its own count even though the first is mid-budget.
	require.ErrorContains(t,
		h.ProcessError(h.Message("queue", row(testFailKey, "budget-row-b"))),
		testFailComment)
}

func TestProcessorDropsEverything(t *testing.T) {
	h := newProcessorHarness(t)
	key := h.Key(flowtest.Row{"hash": uint64(1), "key": "7"})

	r := h.Process(h.KeyedMessage("data", key, row("7", "dropped-row")))

	require.Empty(t, r.Messages())
}
