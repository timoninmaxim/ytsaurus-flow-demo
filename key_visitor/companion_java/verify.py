# Verifies the Java-companion variant with the same asserts as ../verify.py (and the
# upstream test), against the key_visitor_java root:
#   - the pipeline reaches `completed` on its own: the source is finite and the visitor's
#     `finite` default arms a final sweep once the input is drained;
#   - every seeded key was visited at least once — a row for it exists in the output queue;
#   - the *latest* visit per key (highest visit_index) carries the v2 payload: the final pass
#     ran after all input was applied, so it swept post-completion state, not a stale snapshot.
#
# Run after sourcing your env file, once the pipeline is deployed:
#   python3 verify.py

import os
import sys
import time

import yt.wrapper as yt

KEY_COUNT = 20
COMPLETION_TIMEOUT = 240


def main():
    client = yt.YtClient(proxy=os.environ["YT_PROXY"], token=os.environ["YT_TOKEN"])
    folder = os.environ["YT_DEV_ROOT"] + "/key_visitor_java"
    pipeline = folder + "/pipeline"

    expected_keys = {f"k_{i:03d}" for i in range(KEY_COUNT)}
    expected_latest = {f"k_{i:03d}": f"v2_{i}" for i in range(KEY_COUNT)}

    deadline = time.time() + COMPLETION_TIMEOUT
    while True:
        state = client.get_pipeline_state(pipeline)
        if state == "completed":
            print("ok: pipeline reached `completed`")
            break
        if time.time() > deadline:
            print(f"FAIL: pipeline state is {state!r} after {COMPLETION_TIMEOUT}s")
            sys.exit(1)
        time.sleep(5)

    # JSON format: the plain ytsaurus-client has no YSON bindings.
    rows = list(client.select_rows(
        f"`key`, `payload`, `visit_index` from [{folder}/output_queue]",
        format=yt.JsonFormat(),
    ))

    latest = {}
    for row in rows:
        index = row["visit_index"]
        if index > latest.get(row["key"], (-1, None))[0]:
            latest[row["key"]] = (index, row["payload"])

    ok = True

    missing = expected_keys - set(latest)
    if missing:
        print(f"FAIL: {len(missing)} seeded keys were never visited: {sorted(missing)[:10]}")
        ok = False
    else:
        print(f"ok: all {KEY_COUNT} seeded keys were visited")

    stale = {
        key: latest[key][1]
        for key, payload in expected_latest.items()
        if key in latest and latest[key][1] != payload
    }
    if stale:
        print(f"FAIL: latest visit carries a stale payload for {len(stale)} keys: {dict(list(stale.items())[:5])}")
        ok = False
    else:
        print("ok: the latest visit of every key carries the v2 payload")

    if latest:
        indexes = [index for index, _ in latest.values()]
        print(f"output rows: {len(rows)}; per-key max visit_index range: {min(indexes)}..{max(indexes)}")

    if not ok:
        sys.exit(1)
    print("OK: the final key-visitor pass swept the post-completion state of every key")


if __name__ == "__main__":
    main()
