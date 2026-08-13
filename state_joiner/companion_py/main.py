"""Python companion for the state_joiner scenario: the accumulator and the joiner from
companion/main.cpp re-expressed with the Flow Python companion SDK.

Both computations are registered here; the pipeline binary is the stock flow_server and the
reader stays the stock C++ source computation. The behaviour mirrors the C++ functions exactly:

- ``Accumulator`` (hosted by ``TTransformCompanionComputation``) sums each user's "Amount"
  into the per-user external state "/user_total" (the user_totals table) and forwards the
  user id, with a constant bucket, into the "users" stream.
- ``Joiner`` (hosted by ``TTransformCompanionComputation``) reads that same table back
  through the read-only joined external state "/user_total" and emits the stored total for
  each incoming user into the "results" stream. It is a ``BatchFunction``, the Python
  counterpart of the C++ ``IBatchProcessFunction`` the upstream test uses; the join key is
  the message's own key (``join_on = {}`` in the spec), so no grouping is needed here.

The scenario's original subject — ``state_joiners`` over another computation's internal
state — is not expressible in any companion language (see the README); this variant, like
the C++ one, demonstrates the external-state form of the same join.
"""

import logging

from yt.yt.flow.library.python.companion import BatchFunction, Pipeline, RowFunction

logging.basicConfig(level=logging.INFO)

USER_TOTAL = "/user_total"


class Accumulator(RowFunction):
    def on_message(self, message, output, ctx):
        state = ctx.external_state(USER_TOTAL, message)
        total = state.get("Total") or 0
        # Only set() persists external state; mutating the read value would be lost.
        state.set(state.to_builder().set("Total", total + message.payload["Amount"]).finish())

        builder = ctx.message_builder("users")
        builder.set("UserId", message.payload["UserId"])
        builder.set("Bucket", 0)
        output.add_message(builder.finish())


class Joiner(BatchFunction):
    def on_messages(self, messages, output, ctx):
        for message in messages:
            joined = ctx.joined_external_state(USER_TOTAL, message)
            # A key with no row in the joined table arrives as an all-null payload of the
            # table's width, so the accessor exists and get() comes back empty. Report that
            # as -1 instead of throwing: an exception thrown in a companion is retried
            # forever, whereas a sentinel in the output table makes a broken join visible
            # at a glance.
            total = joined.get("Total")

            builder = ctx.message_builder("results")
            builder.set("UserId", message.payload["UserId"])
            builder.set("Total", total if total is not None else -1)
            output.add_message(builder.finish())


def main():
    pipeline = Pipeline()
    pipeline.add("accumulator", Accumulator())
    pipeline.add("joiner", Joiner())
    pipeline.run()


if __name__ == "__main__":
    main()
