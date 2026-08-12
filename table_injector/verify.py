# Verifies the pipeline result against the upstream test's asserts
# (tests/table_injector, TestTableInjector.test_simple):
#   - the output queue holds exactly the ten input rows (ORDER BY data);
#   - the "input_messages" pipeline table's 'any'-typed key column works: the
#     keys come back sorted and each is the three-element grouping key
#     [hash1, hash2, data];
#   - a range query over the composite key (yson_string_to_any on the two
#     hashes of the middle key) returns exactly the upper half of the keys;
#   - the "states" pipeline table is empty (a passthrough keeps no state).
#
# Run after sourcing your env file (see the repo README), once the pipeline
# state is `completed`:
#   python3 verify.py

import json
import os
import sys

import yt.wrapper as yt

from prepare_data import DATA


def main():
    folder = os.environ["YT_DEV_ROOT"] + "/table_injector"
    pipeline = folder + "/pipeline"
    client = yt.YtClient(proxy=os.environ["YT_PROXY"], token=os.environ["YT_TOKEN"])

    def select(query):
        return [json.loads(line) for line in client.select_rows(query, format="json", raw=True)]

    failed = False

    # Upstream: list(select_rows("data from [t_output] ORDER BY data LIMIT 100")) == data.
    rows = select(f"data from [{folder}/output_queue] ORDER BY data LIMIT 100")
    print(f"output rows: {len(rows)} (expected {len(DATA)})")
    if rows != DATA:
        failed = True
        print(f"FAIL: output queue differs from the input: {rows}")

    # Upstream: the 'any'-typed key column — sorted, each key a 3-element list.
    keys = [row["key"] for row in select(f"key from [{pipeline}/input_messages] ORDER BY key LIMIT 100")]
    print(f"input_messages keys: {len(keys)}")
    if sorted(keys) != keys:
        failed = True
        print("FAIL: keys are not sorted")
    for key in keys:
        if not isinstance(key, list) or len(key) != 3:
            failed = True
            print(f"FAIL: key is not a three-element list: {key}")

    # Upstream: a composite-key range query from the middle key's two hashes
    # returns the upper half of the keys.
    if keys:
        middle_hash1 = keys[len(keys) // 2][0]
        middle_hash2 = keys[len(keys) // 2][1]
        half_keys = select(
            f"key FROM [{pipeline}/input_messages] "
            f"WHERE key >= yson_string_to_any('[{middle_hash1}u; {middle_hash2}u]') "
            "ORDER BY key LIMIT 100"
        )
        expected_half = (len(keys) + 1) // 2
        print(f"range query from the middle key: {len(half_keys)} keys (expected {expected_half})")
        if len(half_keys) != expected_half:
            failed = True
            print("FAIL: composite-key range query returned a wrong slice")

    # Upstream: the states table is empty.
    states = select(f"* FROM [{pipeline}/states] LIMIT 10000")
    if states:
        failed = True
        print(f"FAIL: states table is not empty: {len(states)} rows")

    if failed:
        return 1
    print("OK: output equals input, 'any'-typed input_messages keys behave, states are empty")
    return 0


if __name__ == "__main__":
    sys.exit(main())
