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

# The spec names the secrets it wants in `vanilla/secret_env`; the runner reads their values from
# its own environment, but only after uploading the binary, so an unset one is caught here instead.
python3 -c '
import os, re, string, sys
spec = string.Template(sys.stdin.read()).substitute(os.environ)
declared = [name for block in re.findall(r"\"secret_env\"\s*=\s*\[([^]]*)\]", spec) for name in re.findall(r"\"([^\"]+)\"", block)]
missing = [name for name in declared if name not in os.environ]
if missing:
    sys.exit("declared in the spec secret_env, but not set in the environment: " + ", ".join(missing))
sys.stdout.write(spec)' < pipeline.yson.template > pipeline.yson

exec "$FLOW_BIN" --config pipeline.yson
