#!/usr/bin/env bash
# Stops the pipeline, then aborts the vanilla operation the runner recorded on the pipeline node.
# Both steps are conditional: `completed` is a final state that refuses stop-pipeline, and once the
# operation has exited there is no controller left to serve pipeline state at all.
set -euo pipefail

PIPELINE="$YT_DEV_ROOT/secret_env/pipeline"

# get-pipeline-state is relayed to the controller, so it fails whenever the operation is not running.
STATE=$(yt flow get-pipeline-state "$PIPELINE" 2>/dev/null || echo unreachable)
case "$STATE" in
    completed)
        echo "pipeline is completed (final state, nothing to stop)"
        ;;
    unreachable)
        echo "no controller answered: the vanilla operation is not running"
        ;;
    *)
        yt flow stop-pipeline "$PIPELINE"
        until [ "$(yt flow get-pipeline-state "$PIPELINE")" = "stopped" ]; do sleep 2; done
        echo "pipeline stopped"
        ;;
esac

# abort-op cannot resolve a live alias by itself, so map it to the operation id first.
ALIAS=$(yt get "$PIPELINE/@current_vanilla_operation/alias" --format json 2>/dev/null | python3 -c 'import json, sys; print(json.load(sys.stdin))' || true)
if [ -z "$ALIAS" ]; then
    echo "pipeline node records no vanilla operation"
    exit 0
fi

OP_ID=$(yt list-operations --state running --format json | python3 -c '
import json, sys
ops = json.load(sys.stdin)["operations"]
print(next((o["id"] for o in ops if o.get("brief_spec", {}).get("alias") == sys.argv[1]), ""))' "$ALIAS")
if [ -z "$OP_ID" ]; then
    echo "no running operation for this pipeline"
    exit 0
fi

yt abort-op "$OP_ID"
echo "operation $OP_ID ($ALIAS) aborted"
