# Seeds the Go-companion variant's input queue exactly like ../prepare_data.py (and the
# upstream test): event_id from <start> to <start + count>, group_key = event_id % 10 (ten
# grouping keys), spread over the queue's five tablets by event_id % 5.
#
# The source is not finite, so a running pipeline picks up every wave; event ids stay unique
# across waves as long as the waves do not overlap.
#
# Run after sourcing your env file (see the repo README):
#   python3 prepare_data.py [count] [start]

import os
import sys

import yt.wrapper as yt

EVENT_COUNT = 2000
GROUP_KEY_COUNT = 10
TABLET_COUNT = 5
BATCH_SIZE = 500


def main():
    count = int(sys.argv[1]) if len(sys.argv) > 1 else EVENT_COUNT
    start = int(sys.argv[2]) if len(sys.argv) > 2 else 0

    queue = f"{os.environ['YT_DEV_ROOT']}/swift_map_batching_go/input_queue"
    client = yt.YtClient(proxy=os.environ["YT_PROXY"], token=os.environ["YT_TOKEN"])

    rows = [
        {
            "event_id": event_id,
            "group_key": event_id % GROUP_KEY_COUNT,
            # The client's default row format is YSON, which raises "YSON bindings required"
            # unless the separate ytsaurus-yson package is installed; JSON works with
            # ytsaurus-client alone, and there a literal "$" in a column name is doubled.
            "$$tablet_index": event_id % TABLET_COUNT,
        }
        for event_id in range(start, start + count)
    ]
    for offset in range(0, len(rows), BATCH_SIZE):
        client.insert_rows(queue, rows[offset:offset + BATCH_SIZE], format="json")

    print(f"inserted {len(rows)} rows into {queue} (event_id {start}..{start + count - 1})")


if __name__ == "__main__":
    main()
