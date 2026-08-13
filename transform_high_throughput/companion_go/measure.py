# Measures the Go-companion variant with the reference method — ../measure.py loaded as a
# module with its paths pointed at this variant's root — plus the two checks a *fed* pipeline
# owes on top of the self-generating C++ one:
#   - the input backlog (rows fed minus rows consumed by the pipeline, from the consumer's
#     committed offsets) must stay non-empty through the window, otherwise the figure measures
#     the feeder, not the pipeline;
#   - the per-key state count filters `computation_id = 'Reducer'` — the `states` table also
#     holds the engine's own source-progress rows.
#
# Run after sourcing your env file (see the repo README), with feed.py running:
#   python3 companion_go/measure.py [window_seconds]

import importlib.util
import json
import os
import sys
import time

import yt.wrapper as yt

FOLDER = os.environ["YT_DEV_ROOT"] + "/transform_high_throughput_go"
PIPELINE = FOLDER + "/pipeline"
QUEUE = FOLDER + "/output_queue"
INPUT_QUEUE = FOLDER + "/input_queue"
CONSUMER = FOLDER + "/consumer"

_spec = importlib.util.spec_from_file_location(
    "reference_measure", os.path.join(os.path.dirname(__file__), "..", "measure.py")
)
ref = importlib.util.module_from_spec(_spec)
# The reference module's main() would run on import only under __main__; exec is safe.
_spec.loader.exec_module(ref)
ref.FOLDER = FOLDER
ref.PIPELINE = PIPELINE
ref.QUEUE = QUEUE


def _select_unlimited(client, query):
    """The reference select with the server's default input_row_limit (1M rows) lifted:
    a fed input queue holds millions of rows within minutes."""
    lines = (
        client.select_rows(
            query,
            format=yt.JsonFormat(),
            raw=True,
            input_row_limit=1000000000,
            output_row_limit=1000000,
        )
        .read()
        .splitlines()
    )
    return [json.loads(line) for line in lines if line.strip()]


ref.select = _select_unlimited


def sample_input_backlog(client):
    """Rows written to the input queue minus rows the pipeline has committed reading."""
    written_rows = ref.select(
        client,
        f"[$tablet_index] as t, max([$row_index]) as m from [{INPUT_QUEUE}] group by [$tablet_index]",
    )
    written = sum(r["m"] + 1 for r in written_rows)
    # `offset` is a QL keyword, hence the brackets.
    consumed_rows = ref.select(client, f"sum([offset]) as consumed from [{CONSUMER}] group by 1")
    consumed = consumed_rows[0]["consumed"] if consumed_rows else 0
    return written - consumed


def main():
    window = float(sys.argv[1]) if len(sys.argv) > 1 else 60.0
    client = yt.YtClient(proxy=os.environ["YT_PROXY"], token=os.environ["YT_TOKEN"])

    state = ref.get_pipeline_state(client)
    print(f"pipeline state: {state}")
    if state != "working":
        print("FAIL: pipeline is not working")
        sys.exit(1)

    backlog0 = sample_input_backlog(client)
    rows0, bytes0 = ref.sample_queue(client)
    t0 = time.time()
    print(f"t0 sample: {rows0} rows, {bytes0} cumulative bytes, input backlog {backlog0} rows;"
          f" measuring for {window:.0f}s ...")
    time.sleep(window)
    rows1, bytes1 = ref.sample_queue(client)
    backlog1 = sample_input_backlog(client)
    elapsed = time.time() - t0

    state = ref.get_pipeline_state(client)
    if state != "working":
        print(f"FAIL: pipeline left `working` during the window (now {state})")
        sys.exit(1)

    rows_per_second = (rows1 - rows0) / elapsed
    mb_per_second = (bytes1 - bytes0) / elapsed / (1024 * 1024)
    print(
        f"throughput: {rows_per_second:.0f} rows/s, {mb_per_second:.3f} MB/s"
        f" (+{rows1 - rows0} rows in {elapsed:.1f}s, queue at {rows1} rows)"
    )

    print(f"input backlog: {backlog0} rows at t0 -> {backlog1} rows at t1")
    if backlog1 <= 0:
        print("FAIL: the input backlog drained — the figure above measures the feeder, not the pipeline")
        sys.exit(1)

    states = ref.select(
        client,
        f"* from [{PIPELINE}/states] where computation_id = 'Reducer' limit 1",
    )
    if not states:
        print("FAIL: no Reducer rows in the states table — per-key state was not persisted")
        sys.exit(1)
    key_count = ref.select(
        client,
        f"sum(1) as cnt from [{PIPELINE}/states] where computation_id = 'Reducer' group by 1",
    )[0]["cnt"]
    print(f"ok: states table has {key_count} Reducer keys")

    print(
        f"OK: sustained `working`, {rows_per_second:.0f} rows/s with a non-empty input backlog,"
        f" states table has {key_count} keys"
    )


if __name__ == "__main__":
    main()
