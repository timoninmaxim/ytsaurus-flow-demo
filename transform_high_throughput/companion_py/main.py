"""Python companion for the transform_high_throughput scenario: the per-key-state reducer
from pipeline/main.cpp re-expressed with the Flow Python companion SDK.

Only the "Reducer" computation is registered here — the reader is the stock server's native
``TSwiftPassthroughOrderedSourceComputation`` over a queue and never calls the companion, so
the Python code sits exactly where the C++ user code sat: on the transform path. The
behaviour mirrors ``NYT::NFlow::NDemo::TReducer`` exactly:

- per message, bump ``count`` and remember the payload as ``last_data`` in the per-key
  internal state "state" (declared in the spec's ``parameters/internal_states``, persisted
  into the pipeline's built-in ``states`` table by the engine);
- re-emit the message into the output stream, which the async queue sink writes out.

The C++ variant's keyed-batch host groups the epoch's input by key before invoking the
function; the Python ``BatchFunction`` gets the request's whole message batch with keys
mixed, so the grouping by key happens here. Applying a group of size N as ``count += N``
with ``last_data`` from the group's last message leaves the same state a per-message loop
would — messages within a key preserve their order.
"""

import logging

from yt.yt.flow.library.python.companion import BatchFunction, Pipeline

logging.basicConfig(level=logging.INFO)

STATE_NAME = "state"


class Reducer(BatchFunction):
    def on_messages(self, messages, output, ctx):
        groups = {}
        for message in messages:
            groups.setdefault(message.key["key"], []).append(message)

        for group in groups.values():
            state = ctx.state(STATE_NAME, group[0])
            data = state.get_or_default({"count": 0, "last_data": ""})
            data["count"] += len(group)
            data["last_data"] = group[-1].payload["data"]
            # Only set() persists the change; mutating the read value would be lost.
            state.set(data)

            for message in group:
                builder = ctx.message_builder("out")
                builder.set("key", message.payload["key"])
                builder.set("data", message.payload["data"])
                output.add_message(builder.finish())


def main():
    pipeline = Pipeline()
    pipeline.add("Reducer", Reducer())
    pipeline.run()


if __name__ == "__main__":
    main()
