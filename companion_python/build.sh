#!/usr/bin/env bash
# Builds companion_bundle.tgz -- everything the Python companion needs at runtime inside the
# worker's vanilla job, whose image has only a bare python3.8 while the companion SDK needs 3.9+:
#   - a self-contained CPython runtime (python-build-standalone),
#   - the ytsaurus-flow-companion package (SDK + generated proto stubs) with its dependencies,
#   - main.py (the user code).
# The Arcadia build ships all of this as one self-contained PY3_PROGRAM binary; opensource has
# no such packaging, hence this script.
set -euo pipefail
cd "$(dirname "$0")"

YTSAURUS_SRC="${YTSAURUS_SRC:-$HOME/ytsaurus}"
# The runtime shipped into the job; wheels are resolved for this version.
PYTHON_RUNTIME_URL="https://github.com/astral-sh/python-build-standalone/releases/download/20250612/cpython-3.12.11%2B20250612-x86_64-unknown-linux-gnu-install_only.tar.gz"
BUNDLE_PYTHON_VERSION=312

rm -rf build companion_bundle.tgz
mkdir -p build/bundle

# The bundled CPython (~60 MB); cached across rebuilds at the repo root.
CACHE_DIR=../.cache
mkdir -p "$CACHE_DIR"
RUNTIME_TAR="$CACHE_DIR/$(basename "$PYTHON_RUNTIME_URL")"
[ -f "$RUNTIME_TAR" ] || curl -sSL -o "$RUNTIME_TAR" "$PYTHON_RUNTIME_URL"
tar xzf "$RUNTIME_TAR" -C build/bundle  # Extracts into build/bundle/python/.

# The companion SDK, its generated proto stubs, and the pinned toolchain all come from the
# ytsaurus-flow-companion package (yt/yt/flow/tools/python_companion_package in the checkout). Build its wheel
# first: `pip install <src dir>` cannot be combined with the cross-platform flags below, and the
# wheel is pure python, so it installs for the bundled runtime regardless of the host python.
pip3 wheel --quiet --no-deps -w build/wheels "$YTSAURUS_SRC/yt/yt/flow/tools/python_companion_package"
pip3 install --quiet --target build/bundle \
    --platform manylinux2014_x86_64 --implementation cp \
    --python-version "$BUNDLE_PYTHON_VERSION" --only-binary=:all: \
    build/wheels/ytsaurus_flow_companion-*.whl

cp main.py build/bundle/

tar czf companion_bundle.tgz -C build/bundle .
echo "companion_bundle.tgz: $(du -h companion_bundle.tgz | cut -f1)"
