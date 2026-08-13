// Go companion for the transform_high_throughput scenario: the per-key-state reducer from
// pipeline/main.cpp re-expressed with the Flow Go SDK.
//
// Only the "Reducer" computation is registered here — the reader is the stock server's
// native TSwiftPassthroughOrderedSourceComputation over a queue and never calls the
// companion, so the Go code sits exactly where the C++ user code sat: on the transform
// path. The behaviour mirrors NYT::NFlow::NDemo::TReducer (and the Python variant's
// Reducer) exactly:
//
//   - per message, bump "count" and remember the payload as "last_data" in the per-key
//     internal state "state" (declared in the spec's parameters/internal_states, persisted
//     into the pipeline's built-in states table by the engine);
//   - re-emit the message into the "out" stream, which the async queue sink writes out.
//
// The C++ variant's keyed-batch host groups the epoch's input by key before invoking the
// function; the Go BatchFunction gets the request's whole batch with keys mixed, so the
// grouping happens here, in first-appearance order (never ranging over a map — the server
// may replay a batch and the output order must be deterministic). Applying a group of size
// N as count += N with last_data from the group's last message leaves the same state a
// per-message loop would — messages within a key preserve their order.
//
// The same binary is also the launcher: with no Flow env vars set, pipeline.Run() acts as
// the runner — it injects the registered stream schemas into the spec, points the
// CompanionManager resource at itself, ships itself into the worker's vanilla job, and
// execs flow_server.
package main

import (
	"context"
	"fmt"
	"os"

	"go.ytsaurus.tech/yt/go/flow"
)

// stateName is the per-key internal state of the reducer, persisted by the engine into the
// pipeline's built-in states table.
const stateName = "state"

// eventMessage is the one row shape of the whole scenario: the input events and the
// re-emitted output both carry "key" and "data" string columns.
type eventMessage struct {
	flow.YSONMessage
	Key  string `yson:"key"`
	Data string `yson:"data"`
}

// reducerState mirrors the C++ variant's state value: a YSON map {count; last_data}. The
// engine stores companion internal state as opaque bytes, so in the states table it lands
// as a YSON payload string rather than the C++ variant's structured map.
type reducerState struct {
	Count    int64  `yson:"count"`
	LastData string `yson:"last_data"`
}

// reduce is the transform-path benchmark body: per-key state read-modify-write plus a
// passthrough re-emit, so one message loads the whole transform write path.
type reduce struct{}

var _ flow.BatchFunction = (*reduce)(nil)

func (*reduce) OnMessages(
	ctx context.Context,
	rt flow.Runtime,
	msgs []flow.ExtendedMessage,
	out flow.OutputCollector,
) error {
	var order []string
	groups := map[string][]flow.ExtendedMessage{}
	for _, msg := range msgs {
		key, err := msg.Key.String("key")
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

		state, err := flow.OpenYSONState[reducerState](rt, stateName, group[0])
		if err != nil {
			return err
		}

		var lastData string
		for _, msg := range group {
			var input eventMessage
			if err := msg.ConvertTo(&input); err != nil {
				return err
			}

			output := flow.NewYSONMessage[eventMessage]("out")
			output.Key = input.Key
			output.Data = input.Data
			message, err := flow.ConvertFrom(rt, output)
			if err != nil {
				return err
			}
			out.AddMessage(message)

			lastData = input.Data
		}

		// Value() creates the zero state for a key seen the first time; the SDK persists
		// the value at request flush only if it changed.
		value := state.Value()
		value.Count += int64(len(group))
		value.LastData = lastData
	}
	return nil
}

func main() {
	pipeline := flow.NewPipeline()
	pipeline.AddStreams(
		flow.NewYSONStream[eventMessage]("event"),
		flow.NewYSONStream[eventMessage]("out"),
	)
	pipeline.Add(flow.NewBatchComputation("Reducer", &reduce{}))

	if err := pipeline.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "transform_high_throughput_go: %v\n", err)
		os.Exit(1)
	}
}
