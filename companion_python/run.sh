#!/usr/bin/env bash
# Deploys the pipeline as a vanilla YT operation and streams the controller log to the terminal.
# Ctrl-C only detaches — the pipeline keeps running on the cluster; ./stop.sh shuts it down.
# Requires companion_bundle.tgz built by ./build.sh.
set -euo pipefail
cd "$(dirname "$0")"

FLOW_BIN="${FLOW_BIN:-$HOME/ytsaurus/yt/yt/flow/bin/flow_server/flow_server}"

if [ ! -f companion_bundle.tgz ]; then
    echo "companion_bundle.tgz not found — run ./build.sh first" >&2
    exit 1
fi

export SCENARIO_DIR="$PWD"
python3 -c 'import os, string, sys; sys.stdout.write(string.Template(sys.stdin.read()).substitute(os.environ))' \
    < pipeline.yson.template > pipeline.yson

exec "$FLOW_BIN" --config pipeline.yson
