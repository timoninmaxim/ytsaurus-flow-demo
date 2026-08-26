#!/usr/bin/env bash
# Deploys the Java-companion variant: renders the pipeline template and launches the pipeline
# entry point in runner mode, which enriches the spec (stream schemas, companion jars, the
# TJavaCompanionManager classpath) and execs flow_server. Returns when the pipeline completes.
#
# Source your env file first (see the repo README). FLOW_BIN must point at a stripped
# flow_server built from the same-era checkout.
#
# The two YT_FLOW_* overrides below replace the SDK's default JDK porto layers, which do not
# exist on this cluster: the worker task instead runs in a plain docker image that carries a
# JRE (set in pipeline_java.yson.template), and the companion is started from that image's
# java binary.
set -euo pipefail
cd "$(dirname "$0")/.."

FLOW_BIN=${FLOW_BIN:-"$HOME/ytsaurus/yt/yt/flow/bin/flow_server/flow_server.stripped"}
LIBS="companion_java/build/companion-libs"

python3 -c 'import os, string, sys; sys.stdout.write(string.Template(sys.stdin.read()).substitute(os.environ))' \
    < pipeline_java.yson.template > pipeline_java.yson

export YT_FLOW_JDK_LAYERS='[]'
export YT_FLOW_JDK_BIN_PATH=${YT_FLOW_JDK_BIN_PATH:-/opt/java/openjdk/bin/java}

exec "${JAVA:-java}" -Djava.library.path="$LIBS" -cp "$LIBS/*" \
    tech.ytsaurus.flow.demo.keyvisitor.KeyVisitorMain \
    --config pipeline_java.yson --flow-bin "$FLOW_BIN"
