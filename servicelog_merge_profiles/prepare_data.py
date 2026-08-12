# Fills the two profile tables with the upstream test's data set:
#   profiles          — 1500 rows: key i, value i+1, second_value i+2;
#   another_profiles  — the same keys except every tenth (i % 10 == 0), with
#                       value i+3, second_value i+4.
# The holes in another_profiles are the point: the source's table_joiner must
# report them as merged.ispresent = false with null merged columns.
#
# Run after sourcing your env file (see the repo README):
#   python3 prepare_data.py

import os
import sys

import yt.wrapper as yt

ROW_COUNT = 1500
BATCH_SIZE = 500


def generate_data(row_count, is_another):
    """The upstream generator, verbatim: verify.py replays it for the expected values."""
    result = []
    for i in range(row_count):
        if is_another and i % 10 == 0:
            continue
        result.append(
            {"key": i, "value": i + 1 + (2 if is_another else 0), "second_value": i + 2 + (2 if is_another else 0)}
        )
    return result


def main():
    folder = os.environ["YT_DEV_ROOT"] + "/servicelog_merge_profiles"
    client = yt.YtClient(proxy=os.environ["YT_PROXY"], token=os.environ["YT_TOKEN"])

    for table, is_another in (("profiles", False), ("another_profiles", True)):
        rows = generate_data(ROW_COUNT, is_another)
        # The client's default row format is YSON, which raises "YSON bindings required" unless
        # the separate ytsaurus-yson package is installed; JSON works with ytsaurus-client alone.
        for start in range(0, len(rows), BATCH_SIZE):
            client.insert_rows(f"{folder}/{table}", rows[start:start + BATCH_SIZE], format="json")
        print(f"inserted {len(rows)} rows into {folder}/{table}")


if __name__ == "__main__":
    sys.exit(main())
