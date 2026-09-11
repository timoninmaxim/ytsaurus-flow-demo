#!/usr/bin/env bash
# Builds the Java companion module (the runner and the companion are the same entry point) and
# collects the runnable classpath into build/companion-libs — the directory the launch script
# points `java.library.path` at so the Flow runner discovers the jars to ship into the worker.
#
# Needs a JDK 17+ and a checkout of github.com/ytsaurus/ytsaurus next to this repo (the Flow
# Java SDK is not on Maven Central yet — see settings.gradle.kts). With no system gradle, the
# checkout's wrapper is used.
set -euo pipefail
cd "$(dirname "$0")"

GRADLEW=${GRADLEW:-"$HOME/ytsaurus/gradlew"}
"$GRADLEW" --no-daemon test collectRuntime
echo "companion-libs: $(ls build/companion-libs | wc -l) jars, $(du -sh build/companion-libs | cut -f1)"
