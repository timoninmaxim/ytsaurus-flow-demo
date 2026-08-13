// Go companion for the computation_cycles_and_buffers scenario: the six computations from
// companion/main.cpp re-expressed with the Flow Go SDK.
//
// All six computations are registered here; the pipeline binary is the stock flow_server.
// The behaviour mirrors the C++ functions exactly:
//
//   - reader (hosted by TSwiftOrderedSourceCompanionComputation) republishes the "data"
//     column of each input queue row into "reader_output".
//   - transform_a/swift_map_a/transform_b/swift_map_b are one passthrough function under
//     four computation ids: each forwards a message to the output stream its *input* stream
//     maps to under the "passthrough_rules" parameter, sleeping "sleep_per_message"
//     milliseconds first. The cycle itself is pure spec topology (input/output stream ids and
//     streams_dependency); the function only picks the output stream, and transform_a is
//     where the routing matters — reader_output goes once around the loop (→ ta1) and sb1
//     releases the message to the reducer on the way back (→ ta2).
//   - reducer (hosted by TTransformCompanionComputation) adds the size of each key's batch
//     to a per-key count in the external state "/state" backed by the state table.
//
// The two computations the spec hosts with TSwiftMapCompanionComputation must be
// deterministic: the passthrough is — its output depends only on the input message and the
// spec parameters, and the sleep does not shape the output. The server may call the
// functions concurrently across requests (the documented contract), so they keep no mutable
// state; every call works only on its own inputs and collectors. The reducer's grouping
// never ranges over a map — keys are processed in first-appearance order kept in a slice.
//
// The same binary is also the launcher: with no Flow env vars set, pipeline.Run() acts as
// the runner — it injects the registered stream schemas into the spec, points the
// CompanionManager resource at itself, ships itself into the worker's vanilla job, and execs
// flow_server.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.ytsaurus.tech/yt/go/flow"
)

// externalStateName is the external state of the reducer, backed by the state table.
const externalStateName = "/state"

// dataMessage is the one row shape of the whole scenario: the input queue rows and all six
// streams carry a single "data" string column.
type dataMessage struct {
	flow.YSONMessage
	Data string `yson:"data"`
}

// readData reads the input queue and republishes the "data" column into "reader_output".
type readData struct{}

var _ flow.RowFunction = (*readData)(nil)

func (*readData) OnMessage(
	ctx context.Context,
	rt flow.Runtime,
	msg flow.ExtendedMessage,
	out flow.OutputCollector,
) error {
	var input dataMessage
	if err := msg.ConvertTo(&input); err != nil {
		return err
	}

	output := flow.NewYSONMessage[dataMessage]("reader_output")
	output.Data = input.Data
	message, err := flow.ConvertFrom(rt, output)
	if err != nil {
		return err
	}
	out.AddMessage(message)
	return nil
}

// passthrough forwards every message to the output stream its input stream maps to. All four
// computations of the cycle are this one function under different parameters; which of them
// is a transform and which is a swift map is decided by the host class in the spec. An input
// stream missing from the rules is an error: the topology of this scenario is the point, so
// a message with nowhere to go must be reported rather than dropped.
type passthrough struct{}

var _ flow.RowFunction = (*passthrough)(nil)

func (*passthrough) OnMessage(
	ctx context.Context,
	rt flow.Runtime,
	msg flow.ExtendedMessage,
	out flow.OutputCollector,
) error {
	// Artificial per-message delay, by input stream, in milliseconds: slows the cycle down
	// enough for the buffers between its computations to fill. Does not shape the output, so
	// swift determinism holds.
	var sleeps map[string]int64
	if rt.Parameters().Has("sleep_per_message") {
		if err := rt.Parameters().Get("sleep_per_message", &sleeps); err != nil {
			return err
		}
	}
	if ms := sleeps[msg.StreamID]; ms > 0 {
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}

	var rules map[string]string
	if rt.Parameters().Has("passthrough_rules") {
		if err := rt.Parameters().Get("passthrough_rules", &rules); err != nil {
			return err
		}
	}
	outputStream, ok := rules[msg.StreamID]
	if !ok {
		return fmt.Errorf("no passthrough rule for input stream %q", msg.StreamID)
	}

	var input dataMessage
	if err := msg.ConvertTo(&input); err != nil {
		return err
	}

	output := flow.NewYSONMessage[dataMessage](outputStream)
	output.Data = input.Data
	message, err := flow.ConvertFrom(rt, output)
	if err != nil {
		return err
	}
	out.AddMessage(message)
	return nil
}

// reduce counts, per key, how many messages came out of the cycle. The count in the external
// state table is the scenario's assertion: it must equal the number of input messages
// exactly. As in the other Go ports — and unlike the C++ keyed-batch adapter — OnMessages
// gets the request's whole batch with keys mixed, so it groups by key itself, in
// first-appearance order.
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
		key, err := msg.Key.String("data")
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

		state, err := flow.OpenExternalState(rt, externalStateName, group[0])
		if err != nil {
			return err
		}
		// The C++ variant sleeps 10 ms per message here; kept — it is part of what makes the
		// pipeline slow enough for the cut-buffers pause to catch it mid-flight.
		time.Sleep(time.Duration(len(group)) * 10 * time.Millisecond)

		// A key with no count yet is not always an absent row: live, the state manager hands
		// the companion a present row with the key columns set and "count" null, so the null
		// check must be per column, as in the C++ variant's optional<i64>.value_or(0).
		var count int64
		if row, ok := state.Get(); ok && row.Has("count") {
			if count, err = row.Int64("count"); err != nil {
				return err
			}
		}
		// Only "count" is set; the state manager fills the key columns from the grouping key,
		// as in the C++ variant.
		row, err := state.Builder().Set("count", count+int64(len(group))).Finish()
		if err != nil {
			return err
		}
		if err := state.Set(row); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	pipeline := flow.NewPipeline()
	pipeline.AddStreams(
		flow.NewYSONStream[dataMessage]("reader_output"),
		flow.NewYSONStream[dataMessage]("ta1"),
		flow.NewYSONStream[dataMessage]("sa1"),
		flow.NewYSONStream[dataMessage]("tb1"),
		flow.NewYSONStream[dataMessage]("sb1"),
		flow.NewYSONStream[dataMessage]("ta2"),
	)
	pipeline.Add(
		flow.NewRowSourceComputation("reader", &readData{}),
		flow.NewRowComputation("transform_a", &passthrough{}),
		flow.NewRowComputation("swift_map_a", &passthrough{}),
		flow.NewRowComputation("transform_b", &passthrough{}),
		flow.NewRowComputation("swift_map_b", &passthrough{}),
		flow.NewBatchComputation("reducer", &reduce{}),
	)

	if err := pipeline.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "computation_cycles_go: %v\n", err)
		os.Exit(1)
	}
}
