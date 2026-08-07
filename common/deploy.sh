#!/usr/bin/env bash
# Deploys a scenario pipeline to the cluster by running the flow runner on this host.
#
# Usage: common/deploy.sh <scenario_name>
#
# Expects in the scenario dir:
#   pipeline.yson.template — runner config with ${YT_PROXY_INTERNAL}, ${YT_PROXY_RPC},
#                            ${YT_CLUSTER_NAME}, ${YT_DEV_ROOT}, ${YT_POOL} placeholders.
#
# The runner connects over RPC (proxy_addresses pinned in the config's clients_cache, because the
# cluster advertises an address that does not resolve outside k8s), uploads its own binary, submits
# the pipeline spec and launches the controller+worker vanilla operation. YT_FLOW_WAIT=0 makes it
# exit once the pipeline is Working instead of tailing it.

set -euo pipefail
REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source "$REPO_ROOT/common/env.sh"

[ -n "${YT_PROXY_RPC:-}" ] || { echo "error: YT_PROXY_RPC is not set by the env file" >&2; exit 1; }

SCENARIO=${1:?usage: deploy.sh <scenario_name>}
SCENARIO_DIR="$REPO_ROOT/$SCENARIO"
BINARY=${FLOW_BINARY:-"$HOME/ytsaurus/yt/yt/flow/bin/flow_server/flow_server"}

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# The runner uploads its own executable for the vanilla jobs, so run a stripped copy —
# a profile build carries gigabytes of debug info.
cp -L "$BINARY" "$WORK/flow_server"
strip "$WORK/flow_server"

python3 - "$SCENARIO_DIR/pipeline.yson.template" > "$WORK/pipeline.yson" <<'PYEOF'
import os, sys

text = open(sys.argv[1]).read()
for var in ("YT_PROXY_INTERNAL", "YT_PROXY_RPC", "YT_CLUSTER_NAME", "YT_DEV_ROOT", "YT_POOL"):
    text = text.replace("${%s}" % var, os.environ[var])
sys.stdout.write(text)
PYEOF

YT_FLOW_WAIT=0 "$WORK/flow_server" --config "$WORK/pipeline.yson"

echo -n "pipeline state: "; ytget get_pipeline_state -G --data-urlencode "pipeline_path=$YT_DEV_ROOT/$SCENARIO/pipeline"; echo
