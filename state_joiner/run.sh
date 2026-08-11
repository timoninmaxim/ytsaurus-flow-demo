#!/usr/bin/env bash
# Deploys the pipeline as a vanilla YT operation and streams the controller log to the terminal.
# The source is finite, so the pipeline reaches Completed on its own; Ctrl-C only detaches, and
# ./stop.sh aborts the vanilla operation afterwards.
# Requires state_joiner_companion.stripped built by ./build.sh.
set -euo pipefail
cd "$(dirname "$0")"

FLOW_BIN="${FLOW_BIN:-$HOME/ytsaurus/yt/yt/flow/bin/flow_server/flow_server}"

if [ ! -f state_joiner_companion.stripped ]; then
    echo "state_joiner_companion.stripped not found — run ./build.sh first" >&2
    exit 1
fi

export SCENARIO_DIR="$PWD"
python3 -c 'import os, string, sys; sys.stdout.write(string.Template(sys.stdin.read()).substitute(os.environ))' \
    < pipeline.yson.template > pipeline.yson

# The companion classes this spec names are newer than every published artifact, so the exact
# server build matters; record it. The trailing "+<login>" of a local build is dropped.
echo "flow_server: $("$FLOW_BIN" --version | sed 's/+.*$//')"

exec "$FLOW_BIN" --config pipeline.yson
