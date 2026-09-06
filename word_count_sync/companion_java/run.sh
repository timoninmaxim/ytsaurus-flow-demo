#!/usr/bin/env bash
# Deploys the Java-companion variant: renders the pipeline template and launches the pipeline
# entry point in runner mode, which enriches the spec, ships the companion jars, completes the
# TJavaCompanionManager classpath and execs flow_server. The source is finite, so this returns
# on its own when the pipeline completes — budget about a minute and a half.
#
# Source your env file first (see the repo README). FLOW_BIN must point at a stripped
# flow_server built from the same-era checkout.
#
# JDK delivery is resolved from the pipeline config: the worker task's docker_image switches
# the launch to docker mode (no porto layers, which do not exist on this cluster), and the
# companion resource's jdk_bin_path points at the image's java binary — both are set in
# pipeline_java.yson.template, so no YT_FLOW_JDK_* overrides are needed.
set -euo pipefail
cd "$(dirname "$0")/.."

FLOW_BIN=${FLOW_BIN:-"$HOME/ytsaurus/yt/yt/flow/bin/flow_server/flow_server.stripped"}
LIBS="companion_java/build/companion-libs"

python3 -c 'import os, string, sys; sys.stdout.write(string.Template(sys.stdin.read()).substitute(os.environ))' \
    < pipeline_java.yson.template > pipeline_java.yson

exec "${JAVA:-java}" -Djava.library.path="$LIBS" -cp "$LIBS/*" \
    tech.ytsaurus.flow.demo.wordcountsync.WordCountSyncMain \
    --config pipeline_java.yson --flow-bin "$FLOW_BIN"
