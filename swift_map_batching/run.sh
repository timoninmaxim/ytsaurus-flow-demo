#!/usr/bin/env bash
# Deploys the pipeline as a vanilla YT operation and streams the controller log to the terminal.
# The source is not finite, so the pipeline keeps running; Ctrl-C only detaches and ./stop.sh
# shuts it down. Requires swift_map_batching_companion.stripped built by ./build.sh.
#
# ALLOW_BATCHING sets the batcher's allow_batching_with_relaxed_guarantees, the flag the whole
# scenario is about; it defaults to %true. Deploying with %false is the contrast experiment
# described in the README — the pipeline is the same, and every batcher job then fails on the
# first key that carries more than one message in an epoch.
set -euo pipefail
cd "$(dirname "$0")"

FLOW_BIN="${FLOW_BIN:-$HOME/ytsaurus/yt/yt/flow/bin/flow_server/flow_server}"

if [ ! -f swift_map_batching_companion.stripped ]; then
    echo "swift_map_batching_companion.stripped not found — run ./build.sh first" >&2
    exit 1
fi

export SCENARIO_DIR="$PWD"
export ALLOW_BATCHING="${ALLOW_BATCHING:-%true}"
python3 -c 'import os, string, sys; sys.stdout.write(string.Template(sys.stdin.read()).substitute(os.environ))' \
    < pipeline.yson.template > pipeline.yson

# The companion classes this spec names are newer than every published artifact, so the exact
# server build matters; record it. The trailing "+<login>" of a local build is dropped.
echo "flow_server: $("$FLOW_BIN" --version | sed 's/+.*$//')"
echo "allow_batching_with_relaxed_guarantees: $ALLOW_BATCHING"

exec "$FLOW_BIN" --config pipeline.yson
