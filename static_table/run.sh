#!/usr/bin/env bash
# Deploys the pipeline as a vanilla YT operation and streams the controller log to the terminal.
# The source is finite, so the pipeline reaches Completed on its own; Ctrl-C only detaches, and
# ./stop.sh aborts the vanilla operation afterwards.
set -euo pipefail
cd "$(dirname "$0")"

FLOW_BIN="${FLOW_BIN:-$HOME/ytsaurus/yt/yt/flow/bin/flow_server/flow_server}"

python3 -c 'import os, string, sys; sys.stdout.write(string.Template(sys.stdin.read()).substitute(os.environ))' \
    < pipeline.yson.template > pipeline.yson

# Record the exact server build; the trailing "+<login>" of a local build is dropped.
echo "flow_server: $("$FLOW_BIN" --version | sed 's/+.*$//')"

exec "$FLOW_BIN" --config pipeline.yson
