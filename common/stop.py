# Stops a scenario pipeline gracefully and aborts its vanilla operation.
#
# Usage: python3 common/stop.py <scenario_name> <operation_id>

import os
import sys

import yt.wrapper as yt
from yt.wrapper.flow_commands import PipelineState, get_pipeline_state, stop_pipeline, wait_pipeline_state

STOP_TIMEOUT = 150


def stop(scenario, operation_id):
    pipeline = "{}/{}/pipeline".format(os.environ["YT_DEV_ROOT"], scenario)

    stop_pipeline(pipeline)
    wait_pipeline_state(PipelineState.Stopped, pipeline, wait_timeout=STOP_TIMEOUT)
    print("pipeline state: {}".format(get_pipeline_state(pipeline)))

    yt.abort_operation(operation_id)
    print("operation {} aborted".format(operation_id))


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit("usage: stop.py <scenario_name> <operation_id>")
    stop(sys.argv[1], sys.argv[2])
