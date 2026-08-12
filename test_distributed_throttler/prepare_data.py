# Fills the input queue with the scenario's payload: EVENT_COUNT rows carrying
# the distinct values 0..EVENT_COUNT-1, all in the queue's single tablet.
#
# Run after sourcing your env file (see the repo README):
#   python3 prepare_data.py [event_count]

import os
import sys

import yt.wrapper as yt

EVENT_COUNT = 200
BATCH_SIZE = 100


def main():
    event_count = int(sys.argv[1]) if len(sys.argv) > 1 else EVENT_COUNT
    queue = os.environ["YT_DEV_ROOT"] + "/test_distributed_throttler/input_queue"
    client = yt.YtClient(proxy=os.environ["YT_PROXY"], token=os.environ["YT_TOKEN"])

    # JSON doubles a literal "$" in a column name; the plain "$tablet_index"
    # spelling belongs to the YSON path, which needs the ytsaurus-yson bindings.
    rows = [{"value": i, "$$tablet_index": 0} for i in range(event_count)]

    # The client's default row format is YSON, which raises "YSON bindings required" unless
    # the separate ytsaurus-yson package is installed; JSON works with ytsaurus-client alone.
    for start in range(0, len(rows), BATCH_SIZE):
        client.insert_rows(queue, rows[start:start + BATCH_SIZE], format="json")

    print(f"inserted {len(rows)} rows into {queue}")


if __name__ == "__main__":
    main()
