#!/usr/bin/env bash
# Builds companion_bundle.tgz -- everything the Python companion needs at runtime inside the
# worker's vanilla job, whose image has only a bare python3.8 while the companion SDK needs 3.9+:
#   - a self-contained CPython runtime (python-build-standalone),
#   - proto stubs generated from the checkout (companion gRPC contract + Flow common protos),
#   - dependencies resolved for that bundled runtime (manylinux wheels),
#   - the companion SDK sources from the checkout,
#   - main.py (the user code).
# The Arcadia build ships all of this as one self-contained PY3_PROGRAM binary; opensource has
# no such packaging, hence this script.
set -euo pipefail
cd "$(dirname "$0")"

YTSAURUS_SRC="${YTSAURUS_SRC:-$HOME/ytsaurus}"
# The runtime shipped into the job; wheels are resolved for this version.
PYTHON_RUNTIME_URL="https://github.com/astral-sh/python-build-standalone/releases/download/20250612/cpython-3.12.11%2B20250612-x86_64-unknown-linux-gnu-install_only.tar.gz"
BUNDLE_PYTHON_VERSION=312
# grpcio-tools generates code for the protobuf runtime of the same release line -- pin both.
GRPCIO_VERSION=1.70.0
PROTOBUF_VERSION=5.29.3

rm -rf build companion_bundle.tgz
mkdir -p build/bundle build/tools

# Host-side codegen tool; --target keeps the host's python untouched (no venv needed).
pip3 install --quiet --target build/tools "grpcio-tools==$GRPCIO_VERSION"
protoc() { PYTHONPATH=build/tools python3 -m grpc_tools.protoc "$@"; }

# The bundled CPython (~60 MB); cached across rebuilds at the repo root.
CACHE_DIR=../.cache
mkdir -p "$CACHE_DIR"
RUNTIME_TAR="$CACHE_DIR/$(basename "$PYTHON_RUNTIME_URL")"
[ -f "$RUNTIME_TAR" ] || curl -sSL -o "$RUNTIME_TAR" "$PYTHON_RUNTIME_URL"
tar xzf "$RUNTIME_TAR" -C build/bundle  # Extracts into build/bundle/python/.

# Runtime dependencies, resolved for the bundled python/platform (the host python may differ).
# Installed before codegen: pip --target refuses to write into pre-existing package dirs.
pip3 install --quiet --target build/bundle \
    --platform manylinux2014_x86_64 --implementation cp \
    --python-version "$BUNDLE_PYTHON_VERSION" --only-binary=:all: \
    "grpcio==$GRPCIO_VERSION" "protobuf==$PROTOBUF_VERSION" ytsaurus-client

# Proto import paths follow Arcadia's PROTO_NAMESPACE(yt): "yt/flow/..." and "yt_proto/...",
# while the files live at yt/yt/flow/... and yt/yt_proto/... -- stage a matching root.
mkdir -p build/proto_root/yt build/stubs
ln -s "$YTSAURUS_SRC/yt/yt/flow" build/proto_root/yt/flow
ln -s "$YTSAURUS_SRC/yt/yt_proto" build/proto_root/yt_proto

protoc -I build/proto_root \
    --python_out=build/stubs --grpc_python_out=build/stubs \
    yt/flow/library/cpp/companion/proto/companion_service.proto
protoc -I build/proto_root \
    --python_out=build/stubs \
    yt/flow/library/cpp/common/proto/message.proto \
    yt/flow/library/cpp/common/proto/timer.proto \
    yt/flow/library/cpp/common/proto/visit.proto \
    yt_proto/yt/core/misc/proto/guid.proto \
    yt_proto/yt/core/misc/proto/error.proto \
    yt_proto/yt/core/ytree/proto/attributes.proto \
    yt_proto/yt/core/yson/proto/protobuf_interop.proto

# The companion SDK and its runner helper, at their real import paths.
SDK_DST=build/bundle/yt/yt/flow/library/python
mkdir -p "$SDK_DST/companion" "$SDK_DST/runner"
cp "$YTSAURUS_SRC"/yt/yt/flow/library/python/companion/*.py "$SDK_DST/companion/"
cp "$YTSAURUS_SRC"/yt/yt/flow/library/python/runner/__init__.py "$SDK_DST/runner/"

# The stubs also go under the real prefix yt/yt/flow/...: the SDK's _proto_compat aliases
# yt.flow.* onto yt.yt.flow.*, so stubs placed at the literal yt/flow/ would be shadowed.
cp -r build/stubs/yt/flow/library/cpp build/bundle/yt/yt/flow/library/
cp -r build/stubs/yt_proto build/bundle/

cp main.py build/bundle/

tar czf companion_bundle.tgz -C build/bundle .
echo "companion_bundle.tgz: $(du -h companion_bundle.tgz | cut -f1)"
