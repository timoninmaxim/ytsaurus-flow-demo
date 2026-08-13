// Go companion for the state_joiner scenario: the accumulator and the joiner from
// companion/main.cpp re-expressed with the Flow Go SDK.
//
// Both computations are registered here; the reader stays native in the stock flow_server
// worker, exactly as in the C++ and Python variants. The behaviour mirrors the C++ functions:
//
//   - accumulator (hosted by TTransformCompanionComputation) sums each user's Amount into
//     the owned external state "/user_total" backed by the user_totals table, and forwards
//     the user id (with a constant bucket) into the "users" stream.
//   - joiner (hosted by TTransformCompanionComputation) reads the same table back through
//     the read-only joined external state "/user_total" and emits the stored total for each
//     incoming user into the "results" stream. It is a BatchFunction — the Go counterpart of
//     the IBatchProcessFunction granularity the upstream test uses. The batch arrives keys
//     mixed, which is fine here: the join key is the message's own key (join_on = {}) and
//     each message is handled on its own.
//
// The joined accessor has no write methods at all — "joiners never write back" is enforced
// by the Go type system, where C++ distinguishes TMutableStateKeyClient from
// TJoinedStateKeyClient and Python raises ReadOnlyExternalStateError.
//
// The server may call the functions concurrently across requests (the documented contract),
// so they keep no mutable state; every call works only on its own inputs and collectors.
//
// The same binary is also the launcher: with no Flow env vars set, pipeline.Run() acts as the
// runner — it injects the registered stream schemas into the spec, points the CompanionManager
// resource at itself, ships itself into the worker's vanilla job, and execs flow_server.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"go.ytsaurus.tech/yt/go/flow"
)

// userTotalStateName names both sides of the join: the accumulator's owned external state
// and the joiner's read-only joined external state, backed by the same user_totals table.
const userTotalStateName = "/user_total"

// eventMessage is an input on the "events" stream: one amount for one user.
type eventMessage struct {
	flow.YSONMessage
	UserID string `yson:"UserId"`
	Amount int64  `yson:"Amount"`
}

// userMessage is on the "users" stream: a user whose total should be joined and emitted.
// Bucket is unused and kept only for fidelity to the upstream stream schema.
type userMessage struct {
	flow.YSONMessage
	UserID string `yson:"UserId"`
	Bucket uint64 `yson:"Bucket"`
}

// resultMessage is on the "results" stream: the total the joiner read for one user.
type resultMessage struct {
	flow.YSONMessage
	UserID string `yson:"UserId"`
	Total  int64  `yson:"Total"`
}

// accumulator sums each user's Amount into the owned external state "/user_total" and
// forwards the user id into the "users" stream.
type accumulator struct{}

var _ flow.RowFunction = (*accumulator)(nil)

func (*accumulator) OnMessage(
	ctx context.Context,
	rt flow.Runtime,
	msg flow.ExtendedMessage,
	out flow.OutputCollector,
) error {
	var input eventMessage
	if err := msg.ConvertTo(&input); err != nil {
		return err
	}

	state, err := flow.OpenExternalState(rt, userTotalStateName, msg)
	if err != nil {
		return err
	}
	// A user with no total yet is not always an absent row: live, the state manager hands
	// the companion a present row with the key columns set and "Total" null, so the null
	// check must be per column, as in the C++ variant's optional<i64>.value_or(0).
	var total int64
	if row, ok := state.Get(); ok && row.Has("Total") {
		if total, err = row.Int64("Total"); err != nil {
			return err
		}
	}
	// Only "Total" is set; the state manager fills the key columns from the grouping key,
	// as in the C++ variant.
	row, err := state.Builder().Set("Total", total+input.Amount).Finish()
	if err != nil {
		return err
	}
	if err := state.Set(row); err != nil {
		return err
	}

	output := flow.NewYSONMessage[userMessage]("users")
	output.UserID = input.UserID
	output.Bucket = 0
	message, err := flow.ConvertFrom(rt, output)
	if err != nil {
		return err
	}
	out.AddMessage(message)
	return nil
}

// joiner reads the accumulator's totals through the read-only joined external state
// "/user_total" and emits one result per incoming user.
type joiner struct{}

var _ flow.BatchFunction = (*joiner)(nil)

func (*joiner) OnMessages(
	ctx context.Context,
	rt flow.Runtime,
	msgs []flow.ExtendedMessage,
	out flow.OutputCollector,
) error {
	for _, msg := range msgs {
		var input userMessage
		if err := msg.ConvertTo(&input); err != nil {
			return err
		}

		// A key with no row in the joined table arrives as an all-null state row, a state
		// not shipped with the batch as ErrStateNotRead. Report either as -1 instead of
		// failing: an error in a companion is retried forever, whereas a sentinel in the
		// output table makes a broken join visible at a glance. Same policy as the C++
		// variant's two-miss collapse.
		total := int64(-1)
		state, err := flow.OpenJoinedExternalState(rt, userTotalStateName, msg)
		if err != nil && !errors.Is(err, flow.ErrStateNotRead) {
			return err
		}
		if err == nil {
			if row, ok := state.Get(); ok && row.Has("Total") {
				if total, err = row.Int64("Total"); err != nil {
					return err
				}
			}
		}

		output := flow.NewYSONMessage[resultMessage]("results")
		output.UserID = input.UserID
		output.Total = total
		message, err := flow.ConvertFrom(rt, output)
		if err != nil {
			return err
		}
		out.AddMessage(message)
	}
	return nil
}

func main() {
	pipeline := flow.NewPipeline()
	pipeline.AddStreams(
		flow.NewYSONStream[eventMessage]("events"),
		flow.NewYSONStream[userMessage]("users"),
		flow.NewYSONStream[resultMessage]("results"),
	)
	pipeline.Add(
		flow.NewRowComputation("accumulator", &accumulator{}),
		flow.NewBatchComputation("joiner", &joiner{}),
	)

	if err := pipeline.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "state_joiner_go: %v\n", err)
		os.Exit(1)
	}
}
