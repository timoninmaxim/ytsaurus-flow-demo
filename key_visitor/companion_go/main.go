// Go companion for the key_visitor scenario: the visit tester from ../pipeline/main.cpp
// re-expressed with the Flow Go SDK.
//
// Only the "tester" computation is registered here — the native "key_reader" runs in-process
// in the stock flow_server worker and never calls the companion. The behaviour mirrors
// NYT::NFlow::NDemo::TVisitTesterFunction exactly:
//
//   - on a message, store its payload in the per-key internal state "user_state"
//     (declared in the spec's parameters/internal_states);
//   - on a visit, emit the *stored* payload together with a per-key visit counter,
//     or nothing if the key has no state yet.
//
// The engine drives everything else (key tracking, the periodic sweep, the final pass after
// the finite source drains) identically to the C++ and Python variants: visits are injected
// into this process through the companion protocol, together with the states of the visited
// keys, and dispatched to OnVisit because visitTester implements flow.RowVisitFunction.
//
// The same binary is also the launcher: with no Flow env vars set, pipeline.Run() acts as the
// runner — it injects the registered stream schemas into the spec, points the CompanionManager
// resource at itself, ships itself into the worker's vanilla job, and execs flow_server.
package main

import (
	"context"
	"fmt"
	"os"

	"go.ytsaurus.tech/yt/go/flow"
)

const userStateName = "user_state"

// keyMessage is an input on the "keys" stream: an arbitrary payload for a key.
type keyMessage struct {
	flow.YSONMessage
	Key     string `yson:"key"`
	Payload string `yson:"payload"`
}

// visitMessage is an output on the "visits" stream: one row per key per visit, carrying the
// stored payload and a monotonically increasing visit_index so the check can tell visits apart.
type visitMessage struct {
	flow.YSONMessage
	Key        string `yson:"key"`
	Payload    string `yson:"payload"`
	VisitIndex int64  `yson:"visit_index"`
}

// userState is the per-key internal state of the tester.
type userState struct {
	Payload    string `yson:"payload"`
	VisitIndex int64  `yson:"visit_index"`
}

// visitTester stores each key's payload in internal per-key state on a message, and on a visit
// emits a visitMessage with the stored payload and an incremented visit index.
type visitTester struct{}

var (
	_ flow.RowFunction      = (*visitTester)(nil)
	_ flow.RowVisitFunction = (*visitTester)(nil)
)

func (*visitTester) OnMessage(
	ctx context.Context,
	rt flow.Runtime,
	msg flow.ExtendedMessage,
	out flow.OutputCollector,
) error {
	var input keyMessage
	if err := msg.ConvertTo(&input); err != nil {
		return err
	}

	state, err := flow.OpenYSONState[userState](rt, userStateName, msg)
	if err != nil {
		return err
	}
	// Value() keeps the previously stored visit_index; only the payload changes.
	state.Value().Payload = input.Payload
	return nil
}

func (*visitTester) OnVisit(
	ctx context.Context,
	rt flow.Runtime,
	visit flow.Visit,
	out flow.OutputCollector,
) error {
	state, err := flow.OpenYSONState[userState](rt, userStateName, visit)
	if err != nil {
		return err
	}
	if state.Empty() {
		return nil
	}

	key, err := visit.Key.String("key")
	if err != nil {
		return err
	}

	value := state.Value()
	value.VisitIndex++

	output := flow.NewYSONMessage[visitMessage]("visits")
	output.Key = key
	output.Payload = value.Payload
	output.VisitIndex = value.VisitIndex

	message, err := flow.ConvertFrom(rt, output)
	if err != nil {
		return err
	}
	out.AddMessage(message)
	return nil
}

func main() {
	pipeline := flow.NewPipeline()
	pipeline.AddStreams(
		flow.NewYSONStream[keyMessage]("keys"),
		flow.NewYSONStream[visitMessage]("visits"),
	)
	pipeline.Add(flow.NewRowComputation("tester", &visitTester{}))

	if err := pipeline.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "key_visitor_go: %v\n", err)
		os.Exit(1)
	}
}
