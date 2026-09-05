#!/usr/bin/env bash
# Runs a YQL-over-Flow scenario end to end:
#   ./yql_common/run.sh <scenario>
# Renders the gateway config and the query from their templates, bootstraps the
# scenario's input/output queues (setup.sh), executes the query through ytrun —
# which compiles it into a Flow pipeline and launches the vanilla operation on
# the cluster — then waits for the pipeline to complete and checks the output
# (verify.sh). See yql_common/README.md for the required binaries and the
# host-connectivity patches they must carry.
set -euo pipefail

SCENARIO=${1:?usage: ./yql_common/run.sh <scenario>}
ROOT=$(cd "$(dirname "$0")/.." && pwd)

# The two binaries built from the ytsaurus checkout (see yql_common/README.md).
YTRUN_BIN=$(readlink -f "${YTRUN_BIN:-$HOME/ytsaurus/yt/yql/tools/ytrun/ytrun}")
YTFLOW_WORKER_BIN=$(readlink -f "${YTFLOW_WORKER_BIN:-$HOME/ytsaurus/yt/yql/tools/ytflow_worker/ytflow_worker.stripped}")
[ -x "$YTRUN_BIN" ] || { echo "no ytrun binary at $YTRUN_BIN — build it or set YTRUN_BIN" >&2; exit 1; }
[ -x "$YTFLOW_WORKER_BIN" ] || { echo "no ytflow_worker binary at $YTFLOW_WORKER_BIN — build it or set YTFLOW_WORKER_BIN" >&2; exit 1; }
export YTFLOW_WORKER_BIN

: "${YT_PROXY:?source env.sh first}" "${YT_PROXY_INTERNAL:?}" "${YT_PROXY_RPC:?}" "${YT_TOKEN:?}" "${YT_DEV_ROOT:?}" "${YT_POOL:?}"

cd "$ROOT/$SCENARIO"
export SCENARIO_DIR="$PWD"
export SCENARIO_ROOT="$YT_DEV_ROOT/$SCENARIO"
export PIPELINE_PATH="$SCENARIO_ROOT/pipeline"

render() {
    python3 -c 'import os, string, sys; sys.stdout.write(string.Template(sys.stdin.read()).substitute(os.environ))' \
        < "$1" > "$2"
}

# YQL query text is full of its own $bindings ($row, $stream, ...), so the
# query render only substitutes what it knows (the UPPERCASE env placeholders)
# and leaves everything else alone.
render_yql() {
    python3 -c 'import os, string, sys; sys.stdout.write(string.Template(sys.stdin.read()).safe_substitute(os.environ))' \
        < "$1" > "$2"
}

render "$ROOT/yql_common/gateways.conf.template" gateways.conf
render_yql query.yql.template query.yql

echo "== bootstrapping input/output queues under $SCENARIO_ROOT"
./setup.sh

# The cluster's RPC proxies advertise in-cluster addresses; pin the externally
# reachable endpoint for every host-side RPC client (ytrun's gateway and the
# local ytflow_worker launcher).
export YT_RPC_PROXY_ADDRESSES="$YT_PROXY_RPC"

# In-cluster DNS serves A records only, while the flow node's default resolver
# is IPv6-only — without this the controller dies at startup unable to resolve
# its own fqdn. Applied only to the config shipped into the vanilla jobs; the
# launcher on this (IPv6-only) host keeps the default resolver.
export YQL_YTFLOW_JOB_NODE_CONFIG_PATCH='{address_resolver={enable_ipv4=%true;enable_ipv6=%false}}'

echo "== running the query through ytrun"
YTRUN_ARGS=(-s -p query.yql --gateways-cfg gateways.conf --print-result --langver "${YQL_LANGVER:-2025.05}")
# UDF modules (String::, Datetime::, ...) are shared libraries; point
# YQL_UDF_DIR at a directory of built *.so files when a scenario needs them.
[ -n "${YQL_UDF_DIR:-}" ] && YTRUN_ARGS+=(--udfs-dir "$YQL_UDF_DIR")
"$YTRUN_BIN" "${YTRUN_ARGS[@]}" ${YTRUN_EXTRA_ARGS:-} 2>&1 | tee ytrun.log

echo "== waiting for the pipeline to complete"
for _ in $(seq 120); do
    state=$(yt flow get-pipeline-state "$PIPELINE_PATH" 2>/dev/null || true)
    echo "pipeline state: ${state:-<unavailable>}"
    [ "$state" = "completed" ] && break
    sleep 5
done
[ "$state" = "completed" ] || { echo "pipeline did not complete; see $SCENARIO_ROOT and the operation logs" >&2; exit 1; }

echo "== verifying the output"
./verify.sh

echo "== OK: $SCENARIO"
echo "cleanup: ./yql_common/stop.sh $SCENARIO"
