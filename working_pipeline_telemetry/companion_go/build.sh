#!/usr/bin/env bash
# Builds the Go pipeline binary (the runner and the companion are the same executable).
# Needs go >= 1.24 and a checkout of github.com/ytsaurus/ytsaurus next to this repo (the SDK
# is taken through the replace directive in go.mod — it is not in a tagged yt/go release yet).
# With no system go, point GO at another toolchain, e.g. GO="$HOME/arcadia/ya tool go".
set -euo pipefail
cd "$(dirname "$0")"

CGO_ENABLED=0 ${GO:-go} build -o working_pipeline_telemetry_go .
echo "working_pipeline_telemetry_go: $(du -h working_pipeline_telemetry_go | cut -f1)"
