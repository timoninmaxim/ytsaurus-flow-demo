# Fills the input queue with the scenario's payload: TOTAL_EVENTS rows spread
# evenly over the queue's 4 tablets, drawn from a fixed set of 1024 keys so the
# stream has repeated keys for every shuffle hop to regroup.
#
# Run after sourcing your env file (see the repo README):
#   python3 prepare_data.py [total_events]

import os
import sys

import yt.wrapper as yt

TOTAL_EVENTS = 1500
INPUT_QUEUE_TABLET_COUNT = 4
DISTINCT_KEYS = 1024
BATCH_SIZE = 1000


def main():
    total_events = int(sys.argv[1]) if len(sys.argv) > 1 else TOTAL_EVENTS
    queue = os.environ["YT_DEV_ROOT"] + "/shuffle/input_queue"
    client = yt.YtClient(proxy=os.environ["YT_PROXY"], token=os.environ["YT_TOKEN"])

    rows = [
        {
            "key": f"key_{i % DISTINCT_KEYS}",
            "data": f"data_{i}",
            # JSON doubles a literal "$" in a column name; the plain "$tablet_index"
            # spelling belongs to the YSON path, which needs the ytsaurus-yson bindings.
            "$$tablet_index": i % INPUT_QUEUE_TABLET_COUNT,
        }
        for i in range(total_events)
    ]

    # The client's default row format is YSON, which raises "YSON bindings required" unless
    # the separate ytsaurus-yson package is installed; JSON works with ytsaurus-client alone.
    for start in range(0, len(rows), BATCH_SIZE):
        client.insert_rows(queue, rows[start:start + BATCH_SIZE], format="json")

    print(f"inserted {len(rows)} rows into {queue}")


if __name__ == "__main__":
    main()
