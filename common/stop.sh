#!/usr/bin/env bash
# Stops a scenario pipeline gracefully and aborts its vanilla operation.
#
# Usage: common/stop.sh <scenario_name> <operation_id>

set -euo pipefail
REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source "$REPO_ROOT/common/env.sh"

SCENARIO=${1:?usage: stop.sh <scenario_name> <operation_id>}
OP_ID=${2:?usage: stop.sh <scenario_name> <operation_id>}
YTDIR="$YT_DEV_ROOT/$SCENARIO"

ytpost stop_pipeline "{pipeline_path=\"$YTDIR/pipeline\"}" > /dev/null || true
for _ in $(seq 1 30); do
    STATE=$(ytget get_pipeline_state -G --data-urlencode "pipeline_path=$YTDIR/pipeline")
    [ "$STATE" = '"Stopped"' ] && break
    sleep 5
done
echo "pipeline state: $STATE"
ytpost abort_operation "{operation_id=\"$OP_ID\"}" > /dev/null || true
echo "operation $OP_ID aborted"
