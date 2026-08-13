"""Python companion for the swift_map_batching scenario: the batcher and writer from
companion/main.cpp re-expressed with the Flow Python companion SDK.

Both computations are registered here; only the reader stays native in the stock
flow_server worker. The behaviour mirrors the C++ functions exactly:

- ``Batcher`` (hosted by ``TSwiftMapCompanionComputation``) merges one key's messages of the
  epoch into a single output carrying their comma-joined event ids. The merged output has as
  many parents as the key had messages, which the swift map accepts only under
  ``allow_batching_with_relaxed_guarantees``.
- ``Writer`` (hosted by ``TTransformCompanionComputation``) explodes a batched message back
  into one row per event id, tagging each with the size of the batch it came out of.

One difference against the C++ SDK is load-bearing: there the keyed-batch adapter groups the
epoch's input by key before invoking ``ProcessKey``, and the per-key parents come from the
host. The Python ``BatchFunction`` gets the request's whole message batch, keys mixed, with
all of it as the default parent set — so ``Batcher`` does the key grouping itself and binds
each merged output to exactly its key group via ``output.set_parent_ids``. The grouping is
deterministic in the batch (insertion order), as swift hosting requires.
"""

import logging

from yt.yt.flow.library.python.companion import BatchFunction, Pipeline, RowFunction

logging.basicConfig(level=logging.INFO)


class Batcher(BatchFunction):
    def on_messages(self, messages, output, ctx):
        groups = {}
        for message in messages:
            groups.setdefault(message.key["group_key"], []).append(message)

        for group in groups.values():
            # A fresh collector per key group: its parents are that key's messages, nothing
            # else. The default collector (parents = the whole mixed-key batch) stays unused.
            group_output = output.set_parent_ids([m.message_id for m in group])
            builder = ctx.message_builder("event_batched")
            builder.set("event_ids", ",".join(str(m.payload["event_id"]) for m in group))
            group_output.add_message(builder.finish())


class Writer(RowFunction):
    def on_message(self, message, output, ctx):
        event_ids = [int(token) for token in message.payload["event_ids"].split(",") if token]
        for event_id in event_ids:
            builder = ctx.message_builder("sink_event")
            builder.set("event_id", event_id)
            builder.set("batch_size", len(event_ids))
            output.add_message(builder.finish())


def main():
    pipeline = Pipeline()
    pipeline.add("batcher", Batcher())
    pipeline.add("writer", Writer())
    pipeline.run()


if __name__ == "__main__":
    main()
