// Go companion for the working_pipeline_telemetry scenario: the reader and the processor
// from pipeline/main.cpp re-expressed with the Flow Go SDK.
//
// Both computations are registered here; the same binary is also the launcher, as in the
// other companion_go variants. The scenario's subject is unchanged — the *engine's*
// telemetry about a working pipeline with a periodically injected failure — but the moving
// parts around the failure differ from the C++ variant, because the stock server has
// neither TRandomSource nor the custom computations:
//
//   - read (hosted by TSwiftOrderedSourceCompanionComputation) forwards each input-queue
//     row into the "data" stream, except that it fails on rows whose key equals the
//     spec-injected fail_key (returning an error from OnMessage, with fail_comment in the
//     message) or panic_key (calling panic — the SDK server recovers it and reports it over
//     the same gRPC path, so both failure shapes of Go user code are exercised).
//   - drop (hosted by TTransformCompanionComputation) consumes the stream and drops it,
//     sleeping sleep_per_message_ms per message so its input buffer visibly holds data.
//
// The failure must be transient: the input is a queue, so a row that failed forever would
// be re-read forever and poison the pipeline (a companion error is retried, first by the
// worker's gRPC retry loop, then by the restarted job). Hence fail_attempts: the failure
// repeats per unique row (keyed by its "data" value, process-local count) exactly
// fail_attempts times and then lets the row pass. The worker's retry budget is
// invocation_count + 1 attempts, so with the spec's backoff/invocation_count = 5 a
// fail_attempts of 8 exhausts the first budget (six failures — one genuine job failure
// fires), and the restarted job's re-read spends the remaining two failures inside its own
// budget and passes. The pass-after-N depends on process-local history — the companion
// process is per worker and survives job restarts — which is the same trade the C++
// variant makes when its restarted job draws fresh random keys.
package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"go.ytsaurus.tech/yt/go/flow"
)

// dataMessage is both an input row of the reader's queue source and a row of the "data"
// stream: the two carry the same columns.
type dataMessage struct {
	flow.YSONMessage
	Key  string `yson:"key"`
	Data string `yson:"data"`
}

// failCounts is the process-local failure budget, keyed by the fail row's unique "data"
// value. Only fail_key/panic_key rows ever get an entry. The server may call the
// functions concurrently across requests, hence the lock.
var failCounts = struct {
	sync.Mutex
	counts map[string]int64
}{counts: map[string]int64{}}

// takeFailAttempt counts one more failure for the row and reports whether the budget
// still has room — false means the row has failed enough and must pass.
func takeFailAttempt(data string, failAttempts int64) (int64, bool) {
	failCounts.Lock()
	defer failCounts.Unlock()

	count := failCounts.counts[data]
	if count >= failAttempts {
		return count, false
	}
	failCounts.counts[data] = count + 1
	return count + 1, true
}

// read forwards each input row into the "data" stream, failing on the fail/panic keys.
type read struct{}

var _ flow.RowFunction = (*read)(nil)

func (*read) OnMessage(
	ctx context.Context,
	rt flow.Runtime,
	msg flow.ExtendedMessage,
	out flow.OutputCollector,
) error {
	var input dataMessage
	if err := msg.ConvertTo(&input); err != nil {
		return err
	}

	var failAttempts int64
	if rt.Parameters().Has("fail_attempts") {
		if err := rt.Parameters().Get("fail_attempts", &failAttempts); err != nil {
			return err
		}
	}

	var comment string
	if rt.Parameters().Has("fail_comment") {
		if err := rt.Parameters().Get("fail_comment", &comment); err != nil {
			return err
		}
	}

	var failKey string
	if rt.Parameters().Has("fail_key") {
		if err := rt.Parameters().Get("fail_key", &failKey); err != nil {
			return err
		}
	}
	if failKey != "" && input.Key == failKey {
		if attempt, ok := takeFailAttempt(input.Data, failAttempts); ok {
			fmt.Fprintf(os.Stderr, "read: failing on fail key (data: %s, attempt: %d)\n",
				input.Data, attempt)
			return fmt.Errorf("Got fail key %s. Comment: %s", input.Key, comment)
		}
	}

	var panicKey string
	if rt.Parameters().Has("panic_key") {
		if err := rt.Parameters().Get("panic_key", &panicKey); err != nil {
			return err
		}
	}
	if panicKey != "" && input.Key == panicKey {
		if attempt, ok := takeFailAttempt(input.Data, failAttempts); ok {
			fmt.Fprintf(os.Stderr, "read: panicking on panic key (data: %s, attempt: %d)\n",
				input.Data, attempt)
			panic(fmt.Sprintf("Got panic key %s. Comment: %s", input.Key, comment))
		}
	}

	output := flow.NewYSONMessage[dataMessage]("data")
	output.Key = input.Key
	output.Data = input.Data
	message, err := flow.ConvertFrom(rt, output)
	if err != nil {
		return err
	}
	out.AddMessage(message)
	return nil
}

// drop consumes the stream and drops it, sleeping sleep_per_message_ms per message.
type drop struct{}

var _ flow.RowFunction = (*drop)(nil)

func (*drop) OnMessage(
	ctx context.Context,
	rt flow.Runtime,
	msg flow.ExtendedMessage,
	out flow.OutputCollector,
) error {
	var sleepMs int64
	if rt.Parameters().Has("sleep_per_message_ms") {
		if err := rt.Parameters().Get("sleep_per_message_ms", &sleepMs); err != nil {
			return err
		}
	}
	if sleepMs > 0 {
		time.Sleep(time.Duration(sleepMs) * time.Millisecond)
	}
	return nil
}

func main() {
	pipeline := flow.NewPipeline()
	pipeline.AddStreams(
		flow.NewYSONStream[dataMessage]("data"),
	)
	pipeline.Add(
		flow.NewRowSourceComputation("reader", &read{}),
		flow.NewRowComputation("processor", &drop{}),
	)

	if err := pipeline.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "working_pipeline_telemetry_go: %v\n", err)
		os.Exit(1)
	}
}
