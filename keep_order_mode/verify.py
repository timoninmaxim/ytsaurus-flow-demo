# Verifies the output queue against the upstream test's asserts: for every
# profile, the event_id sequence in queue order is exactly the write-order
# sequence 0, 1, 2, ... — no losses, no duplicates, no reordering — and the
# event_id = -1 events were dropped.
#
# Run after sourcing your env file (see the repo README), once the pipeline
# state is `completed`:
#   python3 verify.py

import json
import os
import sys

from collections import defaultdict

import yt.wrapper as yt

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from prepare_data import generate_events  # noqa: E402


def main():
    queue = os.environ["YT_DEV_ROOT"] + "/keep_order_mode/output_queue"
    client = yt.YtClient(proxy=os.environ["YT_PROXY"], token=os.environ["YT_TOKEN"])

    _, expected = generate_events()

    # Queue order == arrival order; the aliases keep the "$"-prefixed system
    # columns out of the JSON escaping rules.
    query = f"[$tablet_index] as t, [$row_index] as r, reduce_id, event_id from [{queue}]"
    rows = [json.loads(line) for line in client.select_rows(query, format="json", raw=True)]
    rows.sort(key=lambda row: (row["t"], row["r"]))

    actual = defaultdict(list)
    for row in rows:
        actual[row["reduce_id"]].append(row["event_id"])

    print(f"output rows: {len(rows)} (expected {sum(len(v) for v in expected.values())})")

    failed = False

    if set(actual) != set(expected):
        failed = True
        print(f"FAIL: profile sets differ: actual {sorted(actual)}, expected {sorted(expected)}")

    for reduce_id in sorted(expected):
        events = actual.get(reduce_id, [])
        if -1 in events:
            failed = True
            print(f"FAIL: profile {reduce_id}: event_id=-1 leaked into the output")
        if len(events) != len(set(events)):
            failed = True
            print(f"FAIL: profile {reduce_id}: duplicate events")
        missing = set(expected[reduce_id]) - set(events)
        if missing:
            failed = True
            print(f"FAIL: profile {reduce_id}: missing events {sorted(missing)[:10]}...")
        if events != expected[reduce_id]:
            failed = True
            print(f"FAIL: profile {reduce_id}: order differs\n"
                  f"    expected: {expected[reduce_id][:20]}...\n"
                  f"    actual:   {events[:20]}...")

    if failed:
        return 1
    counts = {reduce_id: len(events) for reduce_id, events in sorted(actual.items())}
    print(f"OK: every profile's sequence is exactly ordered, no losses, no duplicates: {counts}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
