#!/usr/bin/env bash
# Stops a YQL-over-Flow scenario and frees its cluster resources:
#   ./yql_common/stop.sh <scenario>
# Aborts the scenario's vanilla operation (looked up through the pipeline
# node's @current_vanilla_operation) and removes the scenario's Cypress root.
set -euo pipefail

SCENARIO=${1:?usage: ./yql_common/stop.sh <scenario>}
: "${YT_DEV_ROOT:?source env.sh first}"

SCENARIO_ROOT="$YT_DEV_ROOT/$SCENARIO"
PIPELINE_PATH="$SCENARIO_ROOT/pipeline"

# The ytflow gateway records its operation on the pipeline node.
op_id=$(yt get "$PIPELINE_PATH/@_yql_ytflow_vanilla_info/operation_id" 2>/dev/null | tr -d '"' || true)
if [ -n "$op_id" ]; then
    state=$(yt get-operation "$op_id" --attribute state --format json 2>/dev/null \
        | python3 -c 'import json,sys; print(json.load(sys.stdin).get("state",""))' || true)
    case "$state" in
        completed|failed|aborted|"") ;;
        *)
            echo "aborting operation $op_id ($state)"
            yt abort-op "$op_id"
            ;;
    esac
fi

# The gateway's master lock lingers for a few seconds after the abort.
for _ in 1 2 3 4 5 6; do
    if yt remove -r -f "$SCENARIO_ROOT" 2>/dev/null; then
        echo "removed $SCENARIO_ROOT"
        exit 0
    fi
    sleep 5
done
yt remove -r -f "$SCENARIO_ROOT"
echo "removed $SCENARIO_ROOT"
