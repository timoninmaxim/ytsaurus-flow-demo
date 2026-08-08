# Runs the whole scenario: writes the input data, deploys the pipeline, checks the output.
#
# Run after sourcing your env file (see the repo README):
#   python3 scenario.py            # all three steps in order
#   python3 scenario.py verify     # a single step, e.g. to re-check a pipeline already deployed

import os
import sys

import yt.wrapper as yt
from yt.wrapper.flow_commands import PipelineState, get_pipeline_state, wait_pipeline_state

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "common"))
from deploy import deploy as deploy_pipeline  # noqa: E402

SCENARIO = "message_filter"
FOLDER = os.environ["YT_DEV_ROOT"] + "/" + SCENARIO

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


# The source is finite, so it reports itself empty as soon as it reaches the end of the queue —
# a pipeline started against an empty queue completes before the rows arrive. Hence prepare first.
def prepare():
    yt.insert_rows(FOLDER + "/input_queue", ROWS)
    print("inserted {} rows into {}/input_queue".format(len(ROWS), FOLDER))


def deploy():
    deploy_pipeline(SCENARIO)


def verify():
    # Raises on timeout, and on anything else that keeps the state unreadable.
    wait_pipeline_state(PipelineState.Completed, FOLDER + "/pipeline", wait_timeout=COMPLETION_TIMEOUT)
    print("pipeline state: {}".format(get_pipeline_state(FOLDER + "/pipeline")))

    keys = sorted(row["key"] for row in yt.select_rows("key from [{}/output_queue]".format(FOLDER)))
    print("output keys: {}".format(",".join(keys)))
    if keys != EXPECTED_KEYS:
        sys.exit("FAIL: unexpected keys")
    print("PASS: bad rows were filtered out")


STEPS = {"prepare": prepare, "deploy": deploy, "verify": verify}


def main():
    if len(sys.argv) == 1:
        for step in STEPS.values():
            step()
    elif len(sys.argv) == 2 and sys.argv[1] in STEPS:
        STEPS[sys.argv[1]]()
    else:
        sys.exit("usage: scenario.py [{}]".format("|".join(STEPS)))


if __name__ == "__main__":
    main()
