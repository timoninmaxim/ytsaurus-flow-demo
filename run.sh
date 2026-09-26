#!/usr/bin/env bash
# ./run.sh <scenario>/pipeline[_variant].yson.template
set -euo pipefail
cd "$(dirname "$1")"; SPEC=$(basename "$1" .template)
export FLOW_IMAGE=${FLOW_IMAGE:-ghcr.io/ytsaurus/flow-nightly:dev-0.2.1} SCENARIO_DIR=$PWD
# Not envsubst (absent here, and it renders an unset ${VAR} empty): an unset one fails the render.
python3 -c 'import os, string, sys; sys.stdout.write(string.Template(sys.stdin.read()).substitute(os.environ))' < "$SPEC.template" > "$SPEC"
exec podman run --rm --network host --env-host -v "$PWD:$PWD" -w "$PWD" "$FLOW_IMAGE" /usr/bin/flow_server --config "$SPEC"
