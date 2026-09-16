#!/usr/bin/env bash
# Builds companion_bundle.tgz -- everything the Python reader/counter need at runtime inside the
# worker's vanilla job (whose image has only a bare python3.8 while the companion SDK needs 3.9+):
#   - a self-contained CPython runtime (python-build-standalone),
#   - the ytsaurus-flow-companion package (SDK + generated proto stubs) with its dependencies,
#   - main.py (the reader and the counter).
# Same recipe as companion_python/build.sh.
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

# The ytsaurus-flow-companion package comes from PyPI; it is pure python, so it installs for the
# bundled runtime regardless of the host python.
pip3 install --quiet --target build/bundle \
    --platform manylinux2014_x86_64 --implementation cp \
    --python-version "$BUNDLE_PYTHON_VERSION" --only-binary=:all: \
    "ytsaurus-flow-companion==${COMPANION_VERSION:-0.1.0}"

cp main.py build/bundle/

tar czf companion_bundle.tgz -C build/bundle .
echo "companion_bundle.tgz: $(du -h companion_bundle.tgz | cut -f1)"
