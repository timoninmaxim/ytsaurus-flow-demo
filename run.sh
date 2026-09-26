#!/usr/bin/env bash
# Deploys a scenario as a vanilla YT operation and streams the controller log to the terminal:
#   ./run.sh <scenario> [variant]
# A scenario with more than one spec template names one as [variant], i.e. [aggregate] renders
# pipeline_aggregate.yson.template. Ctrl-C only detaches — the pipeline keeps running on the
# cluster; ./stop.sh <scenario> shuts it down.
#
# The runner is the released flow_server, run from the Flow image (FLOW_IMAGE) with podman on the
# host network; it uploads that same binary to the vanilla jobs. Specs also export FLOW_IMAGE to
# the jobs as their docker_image. FLOW_BIN runs a locally built binary instead — only for the
# scenarios that need a source build (see the README).
set -euo pipefail

# The one place that names the Flow release; see the README's "Released artifacts".
export FLOW_IMAGE="${FLOW_IMAGE:-ghcr.io/ytsaurus/flow-nightly:dev-0.2.1}"

REPO_DIR=$(readlink -f "$(dirname "$0")")

# Resolved before the cd below, so a path relative to your shell keeps working.
if [ -n "${FLOW_BIN:-}" ]; then
    FLOW_BIN=$(readlink -f "$FLOW_BIN")
    [ -x "$FLOW_BIN" ] || { echo "no runner binary at $FLOW_BIN" >&2; exit 1; }
fi

cd "$REPO_DIR/${1:?usage: ./run.sh <scenario> [variant]}"

# A spec that deploys a file of its own — a companion binary, a bundle — points at it through this.
export SCENARIO_DIR="$PWD"

SPEC="pipeline${2:+_$2}"
python3 -c 'import os, string, sys; sys.stdout.write(string.Template(sys.stdin.read()).substitute(os.environ))' \
    < "$SPEC.yson.template" > "$SPEC.yson"

if [ -n "${FLOW_BIN:-}" ]; then
    # Record the exact server build; the trailing "+<login>" of a local build is dropped.
    echo "flow_server: $("$FLOW_BIN" --version | sed 's/+.*$//') (local $FLOW_BIN)"
    exec "$FLOW_BIN" --config "$SPEC.yson"
fi

# The repo is mounted at its own path, so the absolute paths of the rendered spec (SCENARIO_DIR,
# local_files) resolve inside the container; files a spec ships must live under the repo.
# --env-host hands the runner the whole shell environment: the YT_* coordinates and any variable
# a spec forwards to the jobs through secret_env.
PODMAN=(podman run --rm --network host --env-host -v "$REPO_DIR:$REPO_DIR" -w "$PWD" "$FLOW_IMAGE")
echo "flow_server: $("${PODMAN[@]}" /usr/bin/flow_server --version) ($FLOW_IMAGE)"
exec "${PODMAN[@]}" /usr/bin/flow_server --config "$SPEC.yson"
