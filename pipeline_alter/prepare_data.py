# Feeds both input queues with the upstream test's data set: 1000 rows
# data = "payload_0" .. "payload_999". The alternate queue gets the same
# rows — it is the target of the source-path-change variant.
#
# Run after sourcing your env file (see the repo README), before ./run.sh:
#   python3 prepare_data.py

import json
import os

import yt.wrapper as yt

EVENT_COUNT = 1000
DATA = [{"data": f"payload_{i}"} for i in range(EVENT_COUNT)]
EXPECTED_DATA = sorted(row["data"] for row in DATA)


def main():
    folder = os.environ["YT_DEV_ROOT"] + "/pipeline_alter"
    client = yt.YtClient(proxy=os.environ["YT_PROXY"], token=os.environ["YT_TOKEN"])

    for queue in (folder + "/input_queue", folder + "/input_queue_alt"):
        # The JSON format keeps this runnable with ytsaurus-client alone (the
        # default YSON path needs the separate ytsaurus-yson bindings).
        client.insert_rows(queue, ("\n".join(json.dumps(row) for row in DATA)).encode(), format="json", raw=True)
        print(f"inserted {len(DATA)} rows into {queue}")


if __name__ == "__main__":
    main()
