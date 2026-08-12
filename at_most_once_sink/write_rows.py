# Writes a slice of the scenario's payload into the input queue: rows
# payload_<start> .. payload_<end-1>, spread round-robin over the queue's
# five tablets — the upstream test's data, written in the same three slices.
#
# Run after sourcing your env file (see the repo README):
#   python3 write_rows.py <start> <end>

import os
import sys

import yt.wrapper as yt

TABLET_COUNT = 5
BATCH_SIZE = 100


def main():
    start, end = int(sys.argv[1]), int(sys.argv[2])
    queue = os.environ["YT_DEV_ROOT"] + "/at_most_once_sink/input_queue"
    client = yt.YtClient(proxy=os.environ["YT_PROXY"], token=os.environ["YT_TOKEN"])

    # JSON doubles a literal "$" in a column name; the plain "$tablet_index"
    # spelling belongs to the YSON path, which needs the ytsaurus-yson bindings.
    rows = [{"data": f"payload_{i}", "$$tablet_index": i % TABLET_COUNT} for i in range(start, end)]

    # The client's default row format is YSON, which raises "YSON bindings required" unless
    # the separate ytsaurus-yson package is installed; JSON works with ytsaurus-client alone.
    for batch_start in range(0, len(rows), BATCH_SIZE):
        client.insert_rows(queue, rows[batch_start:batch_start + BATCH_SIZE], format="json")

    print(f"inserted rows [{start}, {end}) into {queue}")


if __name__ == "__main__":
    main()
