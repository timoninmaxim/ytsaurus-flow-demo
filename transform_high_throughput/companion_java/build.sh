#!/usr/bin/env bash
# Builds the Java pipeline module (the runner and the companion are the same entry point) and
# collects the runnable classpath into build/companion-libs — the directory the launch script
# points `java.library.path` at so the Flow runner discovers the jars to ship into the worker.
#
# Needs a JDK 17+ (JAVA_HOME); the Gradle wrapper fetches Gradle itself. The Flow Java SDK is
# resolved from Maven — pass -PflowVersion=X.Y.Z to build against another Flow release.
set -euo pipefail
cd "$(dirname "$0")"

./gradlew --no-daemon test collectRuntime "$@"
echo "companion-libs: $(ls build/companion-libs | wc -l) jars, $(du -sh build/companion-libs | cut -f1)"
