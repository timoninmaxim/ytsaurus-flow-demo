# Verifies the output queue against the upstream test's asserts (the finite
# variant of tests/servicelog/merge_profiles):
#   - all 1500 keys arrived, each exactly once (the upstream state counts:
#     len(rows) == ROW_COUNT and count == 1 per key);
#   - value == key+1 and second_value == key+2 (the upstream in-computation
#     YT_VERIFYs on the primary columns);
#   - the join: keys missing from another_profiles (key % 10 == 0) carry
#     merged.ispresent = false and null merged columns, every other key
#     carries merged.value == key+3, merged.second_value == key+4 and
#     merged.ispresent = true.
#
# Run after sourcing your env file (see the repo README), once the pipeline
# state is `completed`:
#   python3 verify.py

import json
import os
import sys

from collections import Counter

import yt.wrapper as yt

ROW_COUNT = 1500


def main():
    queue = os.environ["YT_DEV_ROOT"] + "/servicelog_merge_profiles/output_queue"
    client = yt.YtClient(proxy=os.environ["YT_PROXY"], token=os.environ["YT_TOKEN"])

    # The aliases keep the dotted and "$"-prefixed column names out of the
    # JSON escaping rules.
    query = (
        "key, value, second_value, "
        "[merged.ispresent] as m_present, [merged.value] as m_value, [merged.second_value] as m_second "
        f"from [{queue}]"
    )
    rows = [json.loads(line) for line in client.select_rows(query, format="json", raw=True)]

    print(f"output rows: {len(rows)} (expected {ROW_COUNT})")

    failed = False

    counts = Counter(row["key"] for row in rows)
    if set(counts) != set(range(ROW_COUNT)):
        failed = True
        missing = sorted(set(range(ROW_COUNT)) - set(counts))
        extra = sorted(set(counts) - set(range(ROW_COUNT)))
        print(f"FAIL: key set differs: missing {missing[:10]}..., unexpected {extra[:10]}...")
    duplicated = {key: count for key, count in counts.items() if count != 1}
    if duplicated:
        failed = True
        print(f"FAIL: keys delivered more than once: {dict(sorted(duplicated.items())[:10])}...")

    for row in rows:
        key = row["key"]
        if row["value"] != key + 1 or row["second_value"] != key + 2:
            failed = True
            print(f"FAIL: key {key}: primary columns are ({row['value']}, {row['second_value']}), "
                  f"expected ({key + 1}, {key + 2})")
            continue
        if key % 10 == 0:
            expected = (False, None, None)
        else:
            expected = (True, key + 3, key + 4)
        actual = (row["m_present"], row["m_value"], row["m_second"])
        if actual != expected:
            failed = True
            print(f"FAIL: key {key}: merged columns are {actual}, expected {expected}")

    if failed:
        return 1
    joined = sum(1 for row in rows if row["m_present"])
    print(f"OK: every key delivered exactly once with the correct merge "
          f"({joined} keys joined, {len(rows) - joined} marked absent)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
