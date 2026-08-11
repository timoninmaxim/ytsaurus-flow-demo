#!/usr/bin/env bash
# Deploys one variant of the pipeline as a vanilla YT operation and streams the controller log to
# the terminal. The source is finite, so the pipeline reaches Completed on its own; Ctrl-C only
# detaches, and ./stop.sh aborts the vanilla operation afterwards.
#
#   ./run.sh {swift|delete|aggregate}
set -euo pipefail
cd "$(dirname "$0")"

VARIANT="${1:-}"
case "$VARIANT" in
    swift|delete|aggregate) ;;
    *) echo "usage: $0 {swift|delete|aggregate}" >&2; exit 2 ;;
esac

FLOW_BIN="${FLOW_BIN:-$HOME/ytsaurus/yt/yt/flow/bin/flow_server/flow_server}"

# Rendered per variant, so two variants can be deployed from this directory at the same time.
python3 -c 'import os, string, sys; sys.stdout.write(string.Template(sys.stdin.read()).substitute(os.environ))' \
    < "pipeline_${VARIANT}.yson.template" > "pipeline_${VARIANT}.yson"

# Record the exact server build; the trailing "+<login>" of a local build is dropped.
echo "flow_server: $("$FLOW_BIN" --version | sed 's/+.*$//')"

exec "$FLOW_BIN" --config "pipeline_${VARIANT}.yson"
