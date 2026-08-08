# The whole scenario: deploys the pipeline, feeds the input queue, checks the output, shuts down.
#
# Run after sourcing your env file (see the repo README):
#   python3 scenario.py                    # deploy → prepare → verify → stop
#   python3 scenario.py verify             # one step on its own
#   python3 scenario.py --flow-bin <path>  # runner binary to deploy

import argparse
import os
import string
import subprocess
import sys
import tempfile
import time

import yt.wrapper as yt
from yt.wrapper.flow_commands import PipelineState, get_pipeline_state, stop_pipeline, wait_pipeline_state

SCENARIO_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_FLOW_BIN = "~/ytsaurus/yt/yt/flow/bin/flow_server/flow_server"

FOLDER = os.environ["YT_DEV_ROOT"] + "/message_filter"
PIPELINE = FOLDER + "/pipeline"

# The two "bad" rows are the ones the pipeline must drop.
ROWS = [
    {"key": "good_0", "data": "0", "$tablet_index": 0},
    {"key": "bad", "data": "1", "$tablet_index": 0},
    {"key": "good_1", "data": "2", "$tablet_index": 0},
    {"key": "bad", "data": "3", "$tablet_index": 0},
    {"key": "good_2", "data": "4", "$tablet_index": 0},
]
EXPECTED_KEYS = ["good_0", "good_1", "good_2"]

VERIFY_TIMEOUT = 300
STOP_TIMEOUT = 150


def deploy(args):
    # The template's ${VAR} placeholders are string.Template syntax, so a missing variable fails
    # right here naming itself. Only this cluster needs an internal proxy URL of its own.
    env = dict(os.environ)
    env.setdefault("YT_PROXY_INTERNAL", env["YT_PROXY"])
    with open(os.path.join(SCENARIO_DIR, "pipeline.yson.template")) as template:
        config = string.Template(template.read()).substitute(env)

    with tempfile.NamedTemporaryFile("w", suffix=".yson") as config_file:
        config_file.write(config)
        config_file.flush()
        binary = os.path.expanduser(args.flow_bin)
        subprocess.check_call([binary, "--config", config_file.name], env=dict(env, YT_FLOW_WAIT="0"))

    print("pipeline state: {}".format(get_pipeline_state(PIPELINE)))


def prepare(args):
    yt.insert_rows(FOLDER + "/input_queue", ROWS)
    print("inserted {} rows into {}/input_queue".format(len(ROWS), FOLDER))


def verify(args):
    deadline = time.time() + VERIFY_TIMEOUT
    while True:
        keys = sorted(row["key"] for row in yt.select_rows("key from [{}/output_queue]".format(FOLDER)))
        if keys == EXPECTED_KEYS:
            break
        if time.time() > deadline:
            sys.exit("FAIL: after {}s the output queue holds {}".format(VERIFY_TIMEOUT, keys))
        time.sleep(5)

    print("output keys: {}".format(",".join(keys)))
    print("PASS: bad rows were filtered out")


def stop(args):
    stop_pipeline(PIPELINE)
    wait_pipeline_state(PipelineState.Stopped, PIPELINE, wait_timeout=STOP_TIMEOUT)
    print("pipeline state: {}".format(get_pipeline_state(PIPELINE)))

    # The runner records the vanilla operation it launched on the pipeline node.
    alias = yt.get(PIPELINE + "/@current_vanilla_operation/alias")
    yt.abort_operation(operation_alias=alias)
    print("operation {} aborted".format(alias))


STEPS = {"deploy": deploy, "prepare": prepare, "verify": verify, "stop": stop}


def main():
    parser = argparse.ArgumentParser(description="Run the message_filter scenario.")
    parser.add_argument("step", nargs="?", choices=list(STEPS), help="run this step alone")
    parser.add_argument("--flow-bin", default=DEFAULT_FLOW_BIN, help="flow_server binary to deploy")
    args = parser.parse_args()

    for step in [STEPS[args.step]] if args.step else STEPS.values():
        step(args)


if __name__ == "__main__":
    main()
