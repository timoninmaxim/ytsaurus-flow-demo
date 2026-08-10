# The message_filter demo: deploys the pipeline, feeds the input queue, tails the output.
#
#   python3 run_message_filter.py                    # deploy → prepare → tail
#   python3 run_message_filter.py stop               # shut the pipeline and its operation down
#   python3 run_message_filter.py --flow-bin <path>  # runner binary to deploy

import argparse
import os
import string
import subprocess
import tempfile
import time

import yt.wrapper as yt
from yt.wrapper.flow_commands import PipelineState, get_pipeline_state, stop_pipeline, wait_pipeline_state

SCENARIO_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_FLOW_BIN = "~/ytsaurus/yt/yt/flow/bin/flow_server/flow_server"

FOLDER = os.environ["YT_DEV_ROOT"] + "/message_filter"
PIPELINE = FOLDER + "/pipeline"

# Queue rows are read and written in JSON, which keeps the demo to a single pip package: the
# client's default structured format needs the separate `ytsaurus-yson` bindings. JSON encodes a
# leading "$" in a column name as "$$".
ROW_FORMAT = "json"

# The two "bad" rows are the ones the pipeline must drop.
ROWS = [
    {"key": "good_0", "data": "0", "$$tablet_index": 0},
    {"key": "bad", "data": "1", "$$tablet_index": 0},
    {"key": "good_1", "data": "2", "$$tablet_index": 0},
    {"key": "bad", "data": "3", "$$tablet_index": 0},
    {"key": "good_2", "data": "4", "$$tablet_index": 0},
]

POLL_PERIOD = 2
STOP_TIMEOUT = 150


def deploy(args):
    # The template's ${VAR} placeholders are string.Template syntax, so a missing variable fails
    # right here naming itself. Only a cluster whose public address the jobs cannot resolve needs
    # an internal proxy URL of its own.
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
    yt.insert_rows(FOLDER + "/input_queue", ROWS, format=ROW_FORMAT)
    print("inserted {} rows into {}/input_queue".format(len(ROWS), FOLDER))


def tail(args):
    print("tailing {}/output_queue, Ctrl-C to stop".format(FOLDER), flush=True)

    # Nothing consumes the output queue, so it is never trimmed and the offset is just a row count.
    offset = 0
    try:
        while True:
            rows = list(yt.pull_queue(FOLDER + "/output_queue", offset, partition_index=0, format=ROW_FORMAT))
            for row in rows:
                print("{}\t{}".format(row["key"], row["data"]), flush=True)
            offset += len(rows)
            time.sleep(POLL_PERIOD)
    except KeyboardInterrupt:
        print()


def stop(args):
    stop_pipeline(PIPELINE)
    wait_pipeline_state(PipelineState.Stopped, PIPELINE, wait_timeout=STOP_TIMEOUT)
    print("pipeline state: {}".format(get_pipeline_state(PIPELINE)))

    # The runner records the vanilla operation it launched on the pipeline node.
    alias = yt.get(PIPELINE + "/@current_vanilla_operation/alias")
    yt.abort_operation(operation_alias=alias)
    print("operation {} aborted".format(alias))


STEPS = {"deploy": deploy, "prepare": prepare, "tail": tail, "stop": stop}
DEFAULT_STEPS = ["deploy", "prepare", "tail"]


def main():
    parser = argparse.ArgumentParser(description="Run the message_filter scenario.")
    parser.add_argument("step", nargs="?", choices=list(STEPS), help="run this step alone")
    parser.add_argument("--flow-bin", default=DEFAULT_FLOW_BIN, help="flow_server binary to deploy")
    args = parser.parse_args()

    for name in [args.step] if args.step else DEFAULT_STEPS:
        STEPS[name](args)


if __name__ == "__main__":
    main()
