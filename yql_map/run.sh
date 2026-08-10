#!/usr/bin/env bash
# Deploys the pipeline as a vanilla YT operation and streams the controller log to the terminal.
# Ctrl-C only detaches — the pipeline keeps running on the cluster; ./stop.sh shuts it down.
#
# This scenario needs the yql_flow_server binary (flow_server + the YQL computation extension).
# The extension is not present in the opensource ytsaurus repo, so there is no default build
# path — point FLOW_BIN at a yql_flow_server binary explicitly (see README, "Opensource gap").
set -euo pipefail
cd "$(dirname "$0")"

: "${FLOW_BIN:?set FLOW_BIN to a yql_flow_server binary — the stock flow_server does not register the YQL process functions, and the YQL extension is not in the opensource repo (see README)}"

python3 -c 'import os, string, sys; sys.stdout.write(string.Template(sys.stdin.read()).substitute(os.environ))' \
    < pipeline.yson.template > pipeline.yson

exec "$FLOW_BIN" --config pipeline.yson
