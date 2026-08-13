# Measures the pipeline's sustained throughput and verifies the scenario's asserts:
#   - the pipeline is `working` and stays `working` through the measurement window;
#   - a throughput figure: rows/s and MB/s over the window, computed from the growth
#     of the output queue (row count via `sum(1)`, bytes via the queue's
#     `$cumulative_data_weight` system column, summed over tablets);
#   - the built-in `states` table is non-empty (the transform persisted per-key state).
#
# Run after sourcing your env file (see the repo README), once the pipeline is
# deployed. The window defaults to 60 seconds:
#   python3 measure.py [window_seconds]

import json
import os
import sys
import time

import yt.wrapper as yt

FOLDER = os.environ["YT_DEV_ROOT"] + "/transform_high_throughput"
PIPELINE = FOLDER + "/pipeline"
QUEUE = FOLDER + "/output_queue"


def get_pipeline_state(client):
    return client.get_pipeline_state(PIPELINE)


def select(client, query):
    # Raw JSON keeps the script runnable without the optional YSON bindings package.
    lines = client.select_rows(query, format=yt.JsonFormat(), raw=True).read().splitlines()
    return [json.loads(line) for line in lines if line.strip()]


def sample_queue(client):
    """Returns (row_count, cumulative_bytes) of the output queue."""
    rows = select(client, f"sum(1) as cnt from [{QUEUE}] group by 1")
    row_count = rows[0]["cnt"] if rows else 0
    tablets = select(
        client,
        f"[$tablet_index] as t, max([$cumulative_data_weight]) as w from [{QUEUE}] group by [$tablet_index]",
    )
    cumulative_bytes = sum(r["w"] for r in tablets)
    return row_count, cumulative_bytes


def main():
    window = float(sys.argv[1]) if len(sys.argv) > 1 else 60.0
    client = yt.YtClient(proxy=os.environ["YT_PROXY"], token=os.environ["YT_TOKEN"])

    state = get_pipeline_state(client)
    print(f"pipeline state: {state}")
    if state != "working":
        print("FAIL: pipeline is not working")
        sys.exit(1)

    rows0, bytes0 = sample_queue(client)
    t0 = time.time()
    print(f"t0 sample: {rows0} rows, {bytes0} cumulative bytes; measuring for {window:.0f}s ...")
    time.sleep(window)
    rows1, bytes1 = sample_queue(client)
    elapsed = time.time() - t0

    state = get_pipeline_state(client)
    if state != "working":
        print(f"FAIL: pipeline left `working` during the window (now {state})")
        sys.exit(1)

    rows_per_second = (rows1 - rows0) / elapsed
    mb_per_second = (bytes1 - bytes0) / elapsed / (1024 * 1024)
    print(
        f"throughput: {rows_per_second:.0f} rows/s, {mb_per_second:.3f} MB/s"
        f" (+{rows1 - rows0} rows in {elapsed:.1f}s, queue at {rows1} rows)"
    )

    states = select(client, f"* from [{PIPELINE}/states] limit 1")
    if not states:
        print("FAIL: states table is empty — per-key state was not persisted")
        sys.exit(1)
    key_count = select(client, f"sum(1) as cnt from [{PIPELINE}/states] group by 1")[0]["cnt"]
    print(f"ok: states table non-empty ({key_count} rows)")

    print(f"OK: sustained `working`, {rows_per_second:.0f} rows/s, states table has {key_count} keys")


if __name__ == "__main__":
    main()
