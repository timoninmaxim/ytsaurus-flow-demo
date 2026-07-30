#!/usr/bin/env bash
# Deploys a scenario pipeline to the cluster.
#
# Usage: common/deploy.sh <scenario_name>
#
# Expects in the scenario dir:
#   pipeline.yson.template — runner config with ${YT_PROXY_INTERNAL}, ${YT_CLUSTER_NAME},
#                            ${YT_DEV_ROOT}, ${YT_POOL} placeholders.
#
# The cluster's RPC proxies are not reachable from outside, so the runner cannot deploy
# directly. Instead: upload the (stripped) flow binary + rendered config over the HTTP API,
# then run the runner INSIDE the cluster as a 1-job bootstrap vanilla operation
# (YT_FLOW_WAIT=0: it launches the controller+worker vanilla operation and exits).

set -euo pipefail
REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source "$REPO_ROOT/common/env.sh"

SCENARIO=${1:?usage: deploy.sh <scenario_name>}
SCENARIO_DIR="$REPO_ROOT/$SCENARIO"
YTDIR="$YT_DEV_ROOT/$SCENARIO"
BINARY=${FLOW_BINARY:-"$HOME/ytsaurus/yt/yt/flow/bin/flow_server/flow_server"}

# 1. Strip a copy of the binary (profile builds carry gigabytes of debug info).
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
cp -L "$BINARY" "$WORK/flow_server"
strip "$WORK/flow_server"

# 2. Render the runner config.
python3 - "$SCENARIO_DIR/pipeline.yson.template" > "$WORK/pipeline.yson" <<'PYEOF'
import os, sys

text = open(sys.argv[1]).read()
for var in ("YT_PROXY_INTERNAL", "YT_CLUSTER_NAME", "YT_DEV_ROOT", "YT_POOL"):
    text = text.replace("${%s}" % var, os.environ[var])
sys.stdout.write(text)
PYEOF

# 3. Upload both files.
ytpost create "{path=\"$YTDIR/files/flow_server\"; type=file; recursive=%true; ignore_existing=%true; attributes={executable=%true}}" > /dev/null
ytpost create "{path=\"$YTDIR/files/pipeline.yson\"; type=file; ignore_existing=%true}" > /dev/null
YT_CURL_TIMEOUT=600 ytput write_file "{path=\"$YTDIR/files/flow_server\"}" -H 'Transfer-Encoding: chunked' --data-binary "@$WORK/flow_server" > /dev/null
ytput write_file "{path=\"$YTDIR/files/pipeline.yson\"}" --data-binary "@$WORK/pipeline.yson" > /dev/null
echo "uploaded binary and config to $YTDIR/files"

# 4. Start the bootstrap vanilla operation.
OP_ID=$(ytcurl -X POST -H 'Content-Type: application/json' --data-binary @- "$YT_API/start_operation" <<EOF | python3 -c 'import json,sys; print(json.load(sys.stdin)["operation_id"])'
{
  "operation_type": "vanilla",
  "spec": {
    "title": "$SCENARIO bootstrap runner",
    "pool": "$YT_POOL",
    "max_failed_job_count": 1,
    "secure_vault": {"YT_TOKEN": "$YT_TOKEN"},
    "tasks": {
      "bootstrap": {
        "job_count": 1,
        "command": "export YT_TOKEN=\"\$YT_SECURE_VAULT_YT_TOKEN\"; YT_FLOW_WAIT=0 ./flow_server --config pipeline.yson >&2",
        "file_paths": ["$YTDIR/files/flow_server", "$YTDIR/files/pipeline.yson"],
        "cpu_limit": 1,
        "memory_limit": 4294967296
      }
    }
  }
}
EOF
)
echo "bootstrap operation: $OP_ID"

# 5. Wait for the bootstrap operation to finish.
while :; do
    STATE=$(ytget get_operation "--data-urlencode" "operation_id=$OP_ID" -G | python3 -c 'import json,sys; print(json.load(sys.stdin).get("state"))')
    case "$STATE" in
        completed) echo "bootstrap completed"; break;;
        failed|aborted) echo "bootstrap $STATE" >&2; exit 1;;
    esac
    sleep 10
done

echo -n "pipeline state: "; ytget get_pipeline_state -G --data-urlencode "pipeline_path=$YTDIR/pipeline"; echo
