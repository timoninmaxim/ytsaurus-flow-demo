# Writes the scenario's input data and checks the filtered output.
#
# Run after sourcing your env file (see the repo README):
#   python3 scenario.py prepare   # 5 rows: good_0, bad, good_1, bad, good_2
#   python3 scenario.py verify    # waits for Completed, asserts the output keys

import os
import sys

import yt.wrapper as yt
from yt.wrapper.flow_commands import PipelineState, get_pipeline_state, wait_pipeline_state

FOLDER = os.environ["YT_DEV_ROOT"] + "/message_filter"

# The two "bad" rows are the ones the pipeline must drop.
ROWS = [
    {"key": "good_0", "data": "0", "$tablet_index": 0},
    {"key": "bad", "data": "1", "$tablet_index": 0},
    {"key": "good_1", "data": "2", "$tablet_index": 0},
    {"key": "bad", "data": "3", "$tablet_index": 0},
    {"key": "good_2", "data": "4", "$tablet_index": 0},
]
EXPECTED_KEYS = ["good_0", "good_1", "good_2"]

COMPLETION_TIMEOUT = 600


def prepare():
    yt.insert_rows(FOLDER + "/input_queue", ROWS)
    print("inserted {} rows into {}/input_queue".format(len(ROWS), FOLDER))


def verify():
    # Raises on timeout, and on anything else that keeps the state unreadable.
    wait_pipeline_state(PipelineState.Completed, FOLDER + "/pipeline", wait_timeout=COMPLETION_TIMEOUT)
    print("pipeline state: {}".format(get_pipeline_state(FOLDER + "/pipeline")))

    keys = sorted(row["key"] for row in yt.select_rows("key from [{}/output_queue]".format(FOLDER)))
    print("output keys: {}".format(",".join(keys)))
    if keys != EXPECTED_KEYS:
        sys.exit("FAIL: unexpected keys")
    print("PASS: bad rows were filtered out")


COMMANDS = {"prepare": prepare, "verify": verify}


def main():
    if len(sys.argv) != 2 or sys.argv[1] not in COMMANDS:
        sys.exit("usage: scenario.py {{{}}}".format("|".join(COMMANDS)))
    COMMANDS[sys.argv[1]]()


if __name__ == "__main__":
    main()
