#!/usr/bin/env bash
# Deploys the pipeline as a vanilla YT operation and streams the controller log to the terminal.
# Ctrl-C only detaches — the pipeline keeps running on the cluster; ./stop.sh shuts it down.
set -euo pipefail
cd "$(dirname "$0")"

FLOW_BIN="${FLOW_BIN:-$HOME/ytsaurus/yt/yt/flow/bin/flow_server/flow_server}"

python3 -c 'import os, string, sys; sys.stdout.write(string.Template(sys.stdin.read()).substitute(os.environ))' \
    < pipeline.yson.template > pipeline.yson

exec "$FLOW_BIN" --config pipeline.yson
