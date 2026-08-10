#!/usr/bin/env bash
# Builds this scenario's own pipeline binary (pipeline/main.cpp) and strips it into the scenario
# dir as secret_env_pipeline.stripped — that stripped file is what run.sh deploys.
#
# ya only builds targets that live inside the checkout, and there is no way to build against
# installed Flow libraries out of tree, so the sources are staged into the checkout, built there,
# and the staging dir is dropped again. Point YTSAURUS at a checkout set up for `ya make` as
# described in the ytsaurus repo's BUILD.md — a CMake-only checkout cannot build a directory that
# was just added, because the per-target CMakeLists.txt files are generated, not authored.
set -euo pipefail
cd "$(dirname "$0")"

YTSAURUS="${YTSAURUS:-$HOME/ytsaurus}"
STAGE_DIR="$YTSAURUS/yt/yt/flow/demo/secret_env"

# The staging copy — the two source files plus ya's symlink to the unstripped binary — is scratch,
# so it goes away on the way out; the build cache under ~/.ya keeps the real artifacts.
cleanup() {
    rm -rf "$STAGE_DIR"
    rmdir "$(dirname "$STAGE_DIR")" 2>/dev/null || true
}
trap cleanup EXIT

mkdir -p "$STAGE_DIR"
cp pipeline/main.cpp pipeline/ya.make "$STAGE_DIR/"

(cd "$YTSAURUS" && ./ya make --build=release yt/yt/flow/demo/secret_env)

strip -o secret_env_pipeline.stripped "$STAGE_DIR/secret_env_pipeline"
chmod +x secret_env_pipeline.stripped

ls -l secret_env_pipeline.stripped
