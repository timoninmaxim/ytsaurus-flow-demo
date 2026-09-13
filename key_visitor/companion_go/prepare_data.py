# Seeds the input queue exactly like the upstream test (tests/key_visitor/cpp,
# Test.test_key_visitor): 20 keys with v1 payloads, then the same 20 keys again with v2
# payloads, as two separate inserts so the v2 rows sit strictly after the v1 rows in the queue.
# Same as ../prepare_data.py, pointed at the Go variant's Cypress root.
#
# Run after sourcing your env file and after yt_sync.py, before deploying:
#   python3 prepare_data.py

import json
import os

import yt.wrapper as yt

KEY_COUNT = 20


def main():
    client = yt.YtClient(proxy=os.environ["YT_PROXY"], token=os.environ["YT_TOKEN"])
    queue = os.environ["YT_DEV_ROOT"] + "/key_visitor_go/input_queue"

    # Raw JSON lines: the plain ytsaurus-client has no YSON bindings, and in the JSON dialect a
    # leading "$" is escaped as "$$".
    for version in ("v1", "v2"):
        rows = [
            {"key": f"k_{i:03d}", "payload": f"{version}_{i}", "$$tablet_index": 0}
            for i in range(KEY_COUNT)
        ]
        data = "".join(json.dumps(row) + "\n" for row in rows).encode()
        client.insert_rows(queue, data, format=yt.JsonFormat(), raw=True)
        print(f"inserted {len(rows)} {version} rows into {queue}")


if __name__ == "__main__":
    main()
