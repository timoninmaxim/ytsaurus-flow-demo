#!/usr/bin/env bash
# Deploys the pipeline as a vanilla YT operation and streams the controller log to the terminal.
# The source is finite, so the pipeline reaches Completed on its own; Ctrl-C only detaches, and
# ./stop.sh aborts the vanilla operation afterwards.
#
# YT_MY_SECRET must be exported in this shell: the runner reads it from the environment at launch
# and puts it into the operation's secure vault (see README).
set -euo pipefail
cd "$(dirname "$0")"

# Checked here because the runner reads the secret only after uploading the binary — without this
# guard a one-line mistake costs a ~190 MB upload before it is reported.
: "${YT_MY_SECRET:?export it before running (see README)}"

FLOW_BIN="${FLOW_BIN:-./secret_env_pipeline.stripped}"

python3 -c 'import os, string, sys; sys.stdout.write(string.Template(sys.stdin.read()).substitute(os.environ))' \
    < pipeline.yson.template > pipeline.yson

exec "$FLOW_BIN" --config pipeline.yson
