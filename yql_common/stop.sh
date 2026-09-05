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

# An operation alias cannot be aborted directly; map it to the id first.
alias=$(yt get "$PIPELINE_PATH/@current_vanilla_operation/alias" 2>/dev/null | tr -d '"' || true)
if [ -n "$alias" ]; then
    op_id=$(yt list-operations --state running --format json 2>/dev/null \
        | python3 -c '
import json, sys
alias = sys.argv[1]
for op in json.load(sys.stdin).get("operations", []):
    if op.get("brief_spec", {}).get("alias") == alias:
        print(op["id"])' "$alias")
    if [ -n "$op_id" ]; then
        echo "aborting operation $op_id ($alias)"
        yt abort-op "$op_id"
    fi
fi

yt remove -r -f "$SCENARIO_ROOT"
echo "removed $SCENARIO_ROOT"
