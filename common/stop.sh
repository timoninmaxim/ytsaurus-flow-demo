#!/usr/bin/env bash
# Stops a scenario pipeline gracefully and aborts its vanilla operation.
#
# Usage: common/stop.sh <scenario_name> <operation_id>

set -euo pipefail

SCENARIO=${1:?usage: stop.sh <scenario_name> <operation_id>}
OP_ID=${2:?usage: stop.sh <scenario_name> <operation_id>}
YTDIR="${YT_DEV_ROOT:?source your env file first}/$SCENARIO"

yt flow stop-pipeline "$YTDIR/pipeline" --sync --wait-timeout 150 || true
echo "pipeline state: $(yt flow get-pipeline-state "$YTDIR/pipeline")"
yt abort-op "$OP_ID" || true
echo "operation $OP_ID aborted"
