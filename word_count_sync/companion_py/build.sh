#!/usr/bin/env bash
# Builds companion_bundle.tgz -- everything the Python reader/counter need at runtime inside the
# worker's vanilla job (whose image has only a bare python3.8 while the companion SDK needs 3.9+):
#   - a self-contained CPython runtime (python-build-standalone),
#   - the ytsaurus-flow-companion package (SDK + generated proto stubs) with its dependencies,
#   - main.py (the reader and the counter).
# Same recipe as key_visitor/companion_py/build.sh: the ytsaurus-flow-companion wheel is taken
# from $WHEEL or from companion_python's build dir if one is already there, and only built from
# $YTSAURUS_SRC/yt/yt/flow/tools/python_companion_package otherwise (the package's home since
# 2026-08; older checkouts do not have it).
set -euo pipefail
cd "$(dirname "$0")"

# The runtime shipped into the job; wheels are resolved for this version.
PYTHON_RUNTIME_URL="https://github.com/astral-sh/python-build-standalone/releases/download/20250612/cpython-3.12.11%2B20250612-x86_64-unknown-linux-gnu-install_only.tar.gz"
BUNDLE_PYTHON_VERSION=312

rm -rf build companion_bundle.tgz
mkdir -p build/bundle

# The bundled CPython (~60 MB); cached across rebuilds at the repo root.
CACHE_DIR=../../.cache
mkdir -p "$CACHE_DIR"
RUNTIME_TAR="$CACHE_DIR/$(basename "$PYTHON_RUNTIME_URL")"
[ -f "$RUNTIME_TAR" ] || curl -sSL -o "$RUNTIME_TAR" "$PYTHON_RUNTIME_URL"
tar xzf "$RUNTIME_TAR" -C build/bundle  # Extracts into build/bundle/python/.

WHEEL="${WHEEL:-$(ls ../../companion_python/build/wheels/ytsaurus_flow_companion-*.whl 2>/dev/null | head -1 || true)}"
if [ -z "$WHEEL" ]; then
    YTSAURUS_SRC="${YTSAURUS_SRC:-$HOME/ytsaurus}"
    pip3 wheel --quiet --no-deps -w build/wheels "$YTSAURUS_SRC/yt/yt/flow/tools/python_companion_package"
    WHEEL=$(ls build/wheels/ytsaurus_flow_companion-*.whl)
fi
pip3 install --quiet --target build/bundle \
    --platform manylinux2014_x86_64 --implementation cp \
    --python-version "$BUNDLE_PYTHON_VERSION" --only-binary=:all: \
    "$WHEEL"

cp main.py build/bundle/

tar czf companion_bundle.tgz -C build/bundle .
echo "companion_bundle.tgz: $(du -h companion_bundle.tgz | cut -f1)"
