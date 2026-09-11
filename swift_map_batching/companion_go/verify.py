# Verifies the Go-companion variant with the same asserts as the reference README's
# verification snippet (and the upstream test), against the swift_map_batching_go root:
#   - the pipeline is `working` (the source is not finite, so it never completes);
#   - the output queue's event_id set equals range(<total>) — no loss;
#   - no event id appears twice — no duplicates (on an uncut run);
#   - the batch_size histogram, the only place the merging is visible from outside.
#
# Run after sourcing your env file, once the pipeline has drained the wave:
#   python3 verify.py <total-events-fed>

import collections
import os
import sys
import time

import yt.wrapper as yt

DRAIN_TIMEOUT = 240


def main():
    total = int(sys.argv[1]) if len(sys.argv) > 1 else 2000

    client = yt.YtClient(proxy=os.environ["YT_PROXY"], token=os.environ["YT_TOKEN"])
    folder = os.environ["YT_DEV_ROOT"] + "/swift_map_batching_go"

    state = client.get_pipeline_state(folder + "/pipeline")
    print(f"pipeline state: {state}")

    # JSON format: the plain ytsaurus-client has no YSON bindings.
    def read_rows():
        return list(client.select_rows(
            f"event_id, batch_size from [{folder}/output_queue]",
            format=yt.JsonFormat(),
        ))

    deadline = time.time() + DRAIN_TIMEOUT
    rows = read_rows()
    while len({row["event_id"] for row in rows}) < total and time.time() < deadline:
        time.sleep(5)
        rows = read_rows()

    sizes = collections.defaultdict(list)
    for row in rows:
        sizes[row["event_id"]].append(row["batch_size"])
    ids = set(sizes)

    exact_set = ids == set(range(total))
    dups = {i: v for i, v in sizes.items() if len(v) > 1}
    print("rows:", len(rows), "distinct:", len(ids), f"equals range({total}):", exact_set)
    print("duplicated event ids:", len(dups),
          "extra rows:", sum(len(v) - 1 for v in dups.values()),
          "max copies:", max((len(v) for v in sizes.values()), default=0))
    print("batch_size histogram:",
          collections.Counter(row["batch_size"] for row in rows).most_common(5))

    if state != "working" or not exact_set or dups:
        print("FAIL")
        sys.exit(1)
    print("OK: every event delivered exactly once")


if __name__ == "__main__":
    main()
