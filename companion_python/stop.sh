#!/usr/bin/env bash
# Stops the pipeline, then aborts the vanilla operation the runner recorded on the pipeline node.
set -euo pipefail

PIPELINE="$YT_DEV_ROOT/companion_python/pipeline"

yt flow stop-pipeline "$PIPELINE"
until [ "$(yt flow get-pipeline-state "$PIPELINE")" = "stopped" ]; do sleep 2; done
echo "pipeline stopped"

# abort-op cannot resolve a live alias by itself, so map it to the operation id first.
ALIAS=$(yt get "$PIPELINE/@current_vanilla_operation/alias" --format json | python3 -c 'import json, sys; print(json.load(sys.stdin))')
OP_ID=$(yt list-operations --state running --format json | python3 -c '
import json, sys
ops = json.load(sys.stdin)["operations"]
print(next(o["id"] for o in ops if o.get("brief_spec", {}).get("alias") == sys.argv[1]))' "$ALIAS")
yt abort-op "$OP_ID"
echo "operation $OP_ID ($ALIAS) aborted"
