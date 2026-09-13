# Fills one variant's input queue with the scenario's payload, and — for the delete variant — the
# rows the pipeline is then going to delete.
#
# The payload is upstream's, already fanned out: upstream's pipeline binary turns one queue row
# {data, repeat} into `repeat` messages {data, i = 0 … repeat-1}, and this scenario writes those
# messages into the queue directly so a stock passthrough reader can carry them to the sink
# unchanged. So the queue holds, for every event i of EVENT_COUNT, (i % 13) + 1 rows keyed
# "payload_<i>" with i = 0 … repeat-1.
#
# Run after sourcing your env file (see the repo README):
#   python3 prepare_data.py {swift|delete|aggregate} [event_count]

import os
import sys

import yt.wrapper as yt

VARIANTS = ("swift", "delete", "aggregate")

EVENT_COUNT = 1000
REPEAT_PERIOD = 13
BATCH_SIZE = 1000


def repeat_count(event_index):
    return (event_index % REPEAT_PERIOD) + 1


def main():
    variant = sys.argv[1] if len(sys.argv) > 1 else ""
    if variant not in VARIANTS:
        sys.exit(f"usage: prepare_data.py {{{'|'.join(VARIANTS)}}} [event_count]")
    event_count = int(sys.argv[2]) if len(sys.argv) > 2 else EVENT_COUNT

    folder = f"{os.environ['YT_DEV_ROOT']}/sorted_dynamic_table/{variant}"
    client = yt.YtClient(proxy=os.environ["YT_PROXY"], token=os.environ["YT_TOKEN"])

    if variant == "delete":
        # One row more than the pipeline deletes: "payload_<event_count>" is the survivor that
        # proves the sink deleted the keys it saw and nothing else.
        initial = [{"data": f"payload_{i}", "i": i} for i in range(event_count + 1)]
        # The client's default row format is YSON, which raises "YSON bindings required" unless
        # the separate ytsaurus-yson package is installed; JSON works with ytsaurus-client alone.
        for start in range(0, len(initial), BATCH_SIZE):
            client.insert_rows(f"{folder}/output_table", initial[start:start + BATCH_SIZE], format="json")
        print(f"inserted {len(initial)} rows into {folder}/output_table")

    rows = [
        {"data": f"payload_{event}", "i": i}
        for event in range(event_count)
        for i in range(repeat_count(event))
    ]
    for start in range(0, len(rows), BATCH_SIZE):
        client.insert_rows(f"{folder}/input_queue", rows[start:start + BATCH_SIZE], format="json")

    print(f"inserted {len(rows)} rows into {folder}/input_queue ({event_count} distinct keys)")


if __name__ == "__main__":
    main()
