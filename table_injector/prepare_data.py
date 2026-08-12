# Feeds the input queue with the upstream test's data set: ten rows
# data = "hello_" + str(i) * (i + 1), i = 0..9 — every row distinct and the
# lexicographic order of the values equal to the write order.
#
# Run after sourcing your env file (see the repo README), before ./run.sh:
#   python3 prepare_data.py

import json
import os

import yt.wrapper as yt

DATA = [{"data": "hello_" + str(i) * (i + 1)} for i in range(10)]


def main():
    queue = os.environ["YT_DEV_ROOT"] + "/table_injector/input_queue"
    client = yt.YtClient(proxy=os.environ["YT_PROXY"], token=os.environ["YT_TOKEN"])

    # The JSON format keeps this runnable with ytsaurus-client alone (the
    # default YSON path needs the separate ytsaurus-yson bindings).
    client.insert_rows(queue, ("\n".join(json.dumps(row) for row in DATA)).encode(), format="json", raw=True)
    print(f"inserted {len(DATA)} rows into {queue}")


if __name__ == "__main__":
    main()
