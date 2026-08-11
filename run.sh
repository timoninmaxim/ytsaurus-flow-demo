#!/usr/bin/env bash
# Deploys a scenario as a vanilla YT operation and streams the controller log to the terminal:
#   ./run.sh <scenario>
# Ctrl-C only detaches — the pipeline keeps running on the cluster; ./stop.sh <scenario> shuts it
# down.
set -euo pipefail
cd "$(dirname "$0")/${1:?usage: ./run.sh <scenario>}"

# A scenario that builds a binary of its own (build.sh) leaves it here; everything else runs on the
# stock flow_server.
LOCAL_BIN=$(ls ./*.stripped 2>/dev/null | head -n 1 || true)
FLOW_BIN="${FLOW_BIN:-${LOCAL_BIN:-$HOME/ytsaurus/yt/yt/flow/bin/flow_server/flow_server}}"

python3 -c 'import os, string, sys; sys.stdout.write(string.Template(sys.stdin.read()).substitute(os.environ))' \
    < pipeline.yson.template > pipeline.yson

exec "$FLOW_BIN" --config pipeline.yson
