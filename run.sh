#!/usr/bin/env bash
# Deploys a scenario as a vanilla YT operation and streams the controller log to the terminal:
#   ./run.sh <scenario> [variant]
# A scenario with more than one spec template names one as [variant], i.e. [aggregate] renders
# pipeline_aggregate.yson.template. Ctrl-C only detaches — the pipeline keeps running on the
# cluster; ./stop.sh <scenario> shuts it down.
set -euo pipefail

# Resolved before the cd below, so a path relative to your shell keeps working.
FLOW_BIN=$(readlink -f "${FLOW_BIN:-$HOME/ytsaurus/yt/yt/flow/bin/flow_server/flow_server}")
[ -x "$FLOW_BIN" ] || { echo "no runner binary at $FLOW_BIN — build it or set FLOW_BIN" >&2; exit 1; }

cd "$(dirname "$0")/${1:?usage: ./run.sh <scenario> [variant]}"

# A spec that deploys a file of its own — a companion binary, a bundle — points at it through this.
export SCENARIO_DIR="$PWD"

SPEC="pipeline${2:+_$2}"
python3 -c 'import os, string, sys; sys.stdout.write(string.Template(sys.stdin.read()).substitute(os.environ))' \
    < "$SPEC.yson.template" > "$SPEC.yson"

# Record the exact server build; the trailing "+<login>" of a local build is dropped.
echo "flow_server: $("$FLOW_BIN" --version | sed 's/+.*$//')"

exec "$FLOW_BIN" --config "$SPEC.yson"
