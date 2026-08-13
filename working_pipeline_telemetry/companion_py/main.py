"""Python companion for the working_pipeline_telemetry scenario: the reader and the processor
from pipeline/main.cpp re-expressed with the Flow Python companion SDK.

Both computations are registered here; the pipeline binary is the stock flow_server. The
scenario's subject is unchanged — the *engine's* telemetry about a working pipeline with a
periodically injected failure — but the moving parts around the failure differ from the C++
variant, because the stock server has neither TRandomSource nor the custom computations:

- ``Read`` (hosted by ``TSwiftOrderedSourceCompanionComputation``) forwards each input-queue
  row into the "data" stream, except that it raises on rows whose key equals the
  spec-injected ``fail_key``, with ``fail_comment`` in the exception message.
- ``Drop`` (hosted by ``TTransformCompanionComputation``) consumes the stream and drops it,
  sleeping ``sleep_per_message_ms`` per message so its input buffer visibly holds data.

The failure must be transient: the input is a queue, so a row that failed forever would be
re-read forever and poison the pipeline (a companion exception is retried, first by the
worker's gRPC retry loop, then by the restarted job). Hence ``fail_attempts``: the raise
repeats per unique row (keyed by its "data" value, process-local count) exactly
``fail_attempts`` times and then lets the row pass. The worker's retry budget is
``invocation_count + 1`` attempts (the initial call plus that many retries), so with the
spec's ``backoff/invocation_count = 5`` a ``fail_attempts`` of 8 exhausts the first budget
(six raises — one genuine job failure fires), and the restarted job's re-read spends the
remaining two raises inside its own budget and passes. The pass-after-N depends on
process-local history, which is the same trade the C++ variant makes when its restarted
job draws fresh random keys.
"""

import logging

from yt.yt.flow.library.python.companion import Pipeline, RowFunction

logging.basicConfig(level=logging.INFO)

log = logging.getLogger(__name__)


def _to_str(value):
    return value.decode("utf-8") if isinstance(value, bytes) else value


# Raise counts per fail row, keyed by the row's unique "data" value. Process-local: the
# companion process is per worker and survives job restarts, so the count keeps growing
# across the retries of the same row. Only fail_key rows ever get an entry.
_raise_counts = {}


class Read(RowFunction):
    def on_message(self, message, output, ctx):
        params = {_to_str(k): v for k, v in ctx.parameters.items()}
        key = _to_str(message.payload["key"])
        data = _to_str(message.payload["data"])

        fail_key = _to_str(params.get("fail_key", ""))
        if fail_key and key == fail_key:
            fail_attempts = params.get("fail_attempts", 0)
            count = _raise_counts.get(data, 0)
            if count < fail_attempts:
                _raise_counts[data] = count + 1
                comment = _to_str(params.get("fail_comment", ""))
                log.info("Raising on fail key (data: %s, attempt: %d)", data, count + 1)
                raise RuntimeError(f"Got fail key {key}. Comment: {comment}")

        builder = ctx.message_builder("data")
        builder.set("key", key)
        builder.set("data", data)
        output.add_message(builder.finish())


class Drop(RowFunction):
    def on_message(self, message, output, ctx):
        del message, output
        params = {_to_str(k): v for k, v in ctx.parameters.items()}
        sleep_ms = params.get("sleep_per_message_ms", 0)
        if sleep_ms:
            import time

            time.sleep(sleep_ms / 1000.0)


def main():
    pipeline = Pipeline()
    pipeline.add("reader", Read(), source=True)
    pipeline.add("processor", Drop())
    pipeline.run()


if __name__ == "__main__":
    main()
