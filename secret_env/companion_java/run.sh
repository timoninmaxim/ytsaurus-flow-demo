#!/usr/bin/env bash
# Deploys the Java-companion variant: renders the pipeline template and launches the pipeline
# entry point in runner mode, which enriches the spec (companion jars, the TJavaCompanionManager
# classpath) and spawns flow_server as a child process — with the runner's full environment, which
# is what carries YT_MY_SECRET into the vault injection. Returns when the pipeline completes.
#
# Source your env file and export YT_MY_SECRET first (see the scenario README). FLOW_BIN must
# point at a stripped flow_server built from the same-era checkout.
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
    tech.ytsaurus.flow.demo.secretenv.SecretEnvMain \
    --config pipeline_java.yson --flow-bin "$FLOW_BIN"
