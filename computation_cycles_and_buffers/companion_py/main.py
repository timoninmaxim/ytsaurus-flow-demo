"""Python companion for the computation_cycles_and_buffers scenario: all six computations of
the cycle from companion/main.cpp re-expressed with the Flow Python companion SDK.

Every computation is registered here; the pipeline binary is the stock flow_server, and the
cycle itself stays where it always was — in the spec's stream topology. The behaviour mirrors
the C++ functions exactly:

- ``ReadData`` (hosted by ``TSwiftOrderedSourceCompanionComputation``) republishes the "data"
  column of each input-queue row. It is a swift source, so it must be deterministic — a field
  copy is.
- ``CyclePassthrough`` backs four computations under four ids, exactly as the C++
  ``TCyclePassthroughFunction`` does: it forwards every message to the output stream its
  *input* stream maps to (``passthrough_rules``), sleeping ``sleep_per_message`` milliseconds
  first. Which of the four is a transform and which is a swift map is decided by the host
  class in the spec, not here; the two swift-map-hosted instances are deterministic (the rule
  lookup is per message, no iteration order is involved — the sleep changes timing only).
- ``ReduceCount`` (hosted by ``TTransformCompanionComputation``) adds the size of each key's
  message group to the "count" column of the external state "/state", mirroring the C++
  ``TReduceFunction::ProcessKey``. The C++ keyed-batch adapter groups the epoch's input by
  key before invoking it; the Python ``BatchFunction`` gets the request's whole message batch
  with keys mixed, so the grouping by key happens here.

One structural difference against the C++ variant: the routing tables travel in the
computations' spec ``parameters`` (read via ``ctx.parameters``), not in
``processing_function_parameters`` — the Python SDK reads the former. The values are the
same maps, moved one level up.
"""

import logging
import time

from yt.yt.flow.library.python.companion import BatchFunction, Pipeline, RowFunction

logging.basicConfig(level=logging.INFO)


def _to_str(value):
    return value.decode("utf-8") if isinstance(value, bytes) else value


class ReadData(RowFunction):
    def on_message(self, message, output, ctx):
        builder = ctx.message_builder("reader_output")
        builder.set("data", message.payload["data"])
        output.add_message(builder.finish())


class CyclePassthrough(RowFunction):
    def on_message(self, message, output, ctx):
        sleeps = ctx.parameters.get("sleep_per_message", {})
        sleep_ms = {_to_str(k): v for k, v in sleeps.items()}.get(message.stream_id, 0)
        if sleep_ms:
            time.sleep(sleep_ms / 1000.0)

        rules = {_to_str(k): _to_str(v) for k, v in ctx.parameters.get("passthrough_rules", {}).items()}
        # An input stream missing here is an error: the topology of this scenario is the
        # point, so a message with nowhere to go must be reported rather than dropped.
        # (Beware: an exception from a process function is retried forever.)
        if message.stream_id not in rules:
            raise RuntimeError(f'No passthrough rule for input stream "{message.stream_id}"')

        builder = ctx.message_builder(rules[message.stream_id])
        builder.set("data", message.payload["data"])
        output.add_message(builder.finish())


class ReduceCount(BatchFunction):
    def on_messages(self, messages, output, ctx):
        groups = {}
        for message in messages:
            groups.setdefault(message.key["data"], []).append(message)

        for group in groups.values():
            state = ctx.external_state("/state", group[0])
            time.sleep(0.010 * len(group))
            count = state.get("count") or 0
            # Only set() persists external state; mutating the read value would be lost.
            state.set(state.to_builder().set("count", count + len(group)).finish())


def main():
    pipeline = Pipeline()
    pipeline.add("reader", ReadData(), source=True)
    # One function type under four computation ids, differing only in parameters — the
    # Python counterpart of registering TCyclePassthroughFunction four times.
    pipeline.add("transform_a", CyclePassthrough())
    pipeline.add("swift_map_a", CyclePassthrough())
    pipeline.add("transform_b", CyclePassthrough())
    pipeline.add("swift_map_b", CyclePassthrough())
    pipeline.add("reducer", ReduceCount())
    pipeline.run()


if __name__ == "__main__":
    main()
