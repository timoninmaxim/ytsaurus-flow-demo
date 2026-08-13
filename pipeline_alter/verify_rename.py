# Verifies the rename variant against the upstream test's assert
# (tests/pipeline_alter, TestComputation.test_rename[1c_1w_stop_greedy]):
# after stop -> rename -> restart -> completed, the output queue holds exactly
# the 1000 input rows — nothing lost across the stop, nothing duplicated by
# the restart.
#
# Run after sourcing your env file (see the repo README), once the pipeline
# state is `completed`:
#   python3 verify_rename.py

import json
import os
import sys

import yt.wrapper as yt

from prepare_data import EXPECTED_DATA


def main():
    folder = os.environ["YT_DEV_ROOT"] + "/pipeline_alter"
    client = yt.YtClient(proxy=os.environ["YT_PROXY"], token=os.environ["YT_TOKEN"])

    query = f"data from [{folder}/output_queue] LIMIT {2 * len(EXPECTED_DATA)}"
    rows = sorted(json.loads(line)["data"] for line in client.select_rows(query, format="json", raw=True))
    print(f"output rows: {len(rows)} (expected {len(EXPECTED_DATA)})")

    if rows != EXPECTED_DATA:
        missing = sorted(set(EXPECTED_DATA) - set(rows))
        extra = sorted(set(rows) - set(EXPECTED_DATA))
        print(f"FAIL: output differs from the input; missing {len(missing)} {missing[:5]}, "
              f"extra {len(extra)} {extra[:5]}, duplicates {len(rows) - len(set(rows))}")
        return 1

    print("OK: output equals input — the data survived the computation rename intact")
    return 0


if __name__ == "__main__":
    sys.exit(main())
