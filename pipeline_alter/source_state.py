# The two verification halves of the source-path-change variant (upstream
# tests/pipeline_alter, TestComputation.test_change_source_path and
# test_source_change_erases_old_state[live_controller], YTFLOW-525).
#
# A source key is [stream id, source identity (an opaque hash of the
# identifying params), partition coordinates...]; the identity is element 1.
#
#   python3 source_state.py capture   # while the pipeline is working, before
#                                     # the stop: record the original source
#                                     # identities (waits until the reader has
#                                     # persisted state for them) into
#                                     # identities.json next to this script
#   python3 source_state.py verify    # after the restart: fresh identities
#                                     # appear, the original ones vanish from
#                                     # the layout, and the reader's persisted
#                                     # state holds no trace of them
#
# Run after sourcing your env file (see the repo README).

import json
import os
import sys
import time

import yt.wrapper as yt

IDENTITIES_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "identities.json")
PIPELINE = os.environ["YT_DEV_ROOT"] + "/pipeline_alter/pipeline"

client = yt.YtClient(proxy=os.environ["YT_PROXY"], token=os.environ["YT_TOKEN"])


def layout_identities():
    partitions = client.get_flow_view(
        PIPELINE, view_path="/state/execution_spec/layout/partitions", cache=False)
    return {str(p["source_key"][1]) for p in partitions.values() if p.get("source_key")}


def reader_state_identities():
    states = client.read_states(PIPELINE, computation_id="reader", limit=1000)
    return {str(entry["key"][1]) for entry in states["key_states"]}


def wait(predicate, message, timeout=180):
    deadline = time.time() + timeout
    while True:
        if predicate():
            return
        if time.time() > deadline:
            print(f"FAIL: timed out waiting: {message}")
            sys.exit(1)
        time.sleep(5)


def capture():
    wait(layout_identities, "at least one source partition in the layout")
    original = layout_identities()
    # The upstream test waits until the original source has persisted its
    # per-source-key state — otherwise there would be nothing to erase.
    wait(lambda: reader_state_identities() & original,
         "reader state persisted for the original source identities")
    with open(IDENTITIES_FILE, "w") as file:
        json.dump(sorted(original), file)
    print(f"captured {len(original)} original source identities: {sorted(original)}")


def verify():
    with open(IDENTITIES_FILE) as file:
        original = set(json.load(file))

    # Fresh partitions for the new source identity appear...
    wait(lambda: layout_identities() - original, "fresh source identities in the layout")
    # ...and the original ones are retired (completed) and removed.
    wait(lambda: not (layout_identities() & original), "original identities gone from the layout")
    # The new source persisted its own state, while the old identity's state
    # was erased on completion (interruption would have left it behind).
    wait(lambda: reader_state_identities() and not (reader_state_identities() & original),
         "reader state holds only the new source identities")

    print(f"new source identities: {sorted(layout_identities())}")
    print(f"reader state identities: {sorted(reader_state_identities())}")
    print("OK: the source-path change produced fresh partitions and erased the old source's state")


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in ("capture", "verify"):
        print("usage: python3 source_state.py capture|verify", file=sys.stderr)
        return 2
    capture() if sys.argv[1] == "capture" else verify()
    return 0


if __name__ == "__main__":
    sys.exit(main())
