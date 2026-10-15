# Fills the scenario's input directory with the two static tables the pipeline reads.
#
# Each table's name is the ISO 8601 rendering of the event timestamp it stands for: that name is
# where the static-table source takes the event time of every row in the table from
# (event_timestamp_locator defaults to the "key" attribute, i.e. the node's own name, parsed as
# ISO 8601). The payload carries the table's alias so a row can be traced back to its table.
#
# Run after sourcing your env file (see the repo README):
#   python3 prepare_data.py [rows_per_table]

import datetime
import os
import sys

import yt.wrapper as yt

ROWS_PER_TABLE = 1000

# (alias, event timestamp in seconds) — upstream's two tables, unchanged.
TABLES = [
    ("first", 1_500_000_000),
    ("second", 1_600_000_000),
]

SCHEMA = [{"name": "data", "type": "string", "required": True}]


def table_name(event_timestamp):
    utc = datetime.datetime.fromtimestamp(event_timestamp, datetime.timezone.utc)
    return utc.replace(tzinfo=None).isoformat()


def main():
    rows_per_table = int(sys.argv[1]) if len(sys.argv) > 1 else ROWS_PER_TABLE
    input_dir = os.environ["YT_DEV_ROOT"] + "/static_table/input"
    client = yt.YtClient(proxy=os.environ["YT_PROXY"], token=os.environ["YT_TOKEN"])

    client.create("map_node", input_dir, ignore_existing=True)

    for alias, event_timestamp in TABLES:
        path = input_dir + "/" + table_name(event_timestamp)
        rows = [{"data": f"payload_{alias}_{i:05}"} for i in range(rows_per_table)]
        client.create("table", path, force=True, attributes={"schema": SCHEMA})
        # The client's default row format is YSON, which raises "YSON bindings required" unless
        # the separate ytsaurus-yson package is installed; JSON works with ytsaurus-client alone.
        client.write_table(path, rows, format="json")
        print(f"wrote {len(rows)} rows to {path} (event time {event_timestamp})")


if __name__ == "__main__":
    main()
