#!/usr/bin/env bash
# Builds companion_sdk.tgz: the Python companion SDK and its runtime dependencies, laid out as an
# importable directory the job puts on PYTHONPATH.
#
# It is built *inside the job's docker image* so the native wheels (grpcio, protobuf) match the
# interpreter that will import them. The image must be the one pipeline.yson.template names.
set -euo pipefail
cd "$(dirname "$0")"

YTSAURUS="${YTSAURUS:-$HOME/ytsaurus}"
DOCKER_IMAGE="${FLOW_DOCKER_IMAGE:-docker.io/library/python:3.12-slim}"
DOCKER="${DOCKER:-podman}"

# Everything the package build reads: its own sources, the python packages it ships, the Flow protos
# it compiles, and the yt_proto protos those import. Staged into a small tree of its own rather than
# mounting the checkout, which is gigabytes.
SOURCES=(
    yt/yt/flow/tools/python_companion_package
    yt/yt/flow/library/python/companion
    yt/yt/flow/library/python/runner
    yt/yt/flow/library/cpp/companion/proto
    yt/yt/flow/library/cpp/common/proto
    yt/yt_proto/yt/core/misc/proto
    yt/yt_proto/yt/core/ytree/proto
    yt/yt_proto/yt/core/yson/proto
)

for source in "${SOURCES[@]}"; do
    [ -d "$YTSAURUS/$source" ] || {
        echo "missing $source under $YTSAURUS — point YTSAURUS at a checkout that has it" >&2
        exit 1
    }
done

# The staging copy is scratch; the tarball next to it is the artifact.
STAGE_DIR=$(mktemp -d "$PWD/.build.XXXXXX")
trap 'rm -rf "$STAGE_DIR"' EXIT

for source in "${SOURCES[@]}"; do
    mkdir -p "$STAGE_DIR/$(dirname "$source")"
    cp -r "$YTSAURUS/$source" "$STAGE_DIR/$source"
done

rm -rf sdk companion_sdk.tgz

# `pip install --target` silently skips a top-level package dir that already exists, which would
# drop yt/yt/flow on top of the yt/ that ytsaurus-client just installed. A venv merges them.
#
# The four yt_proto stubs are generated here because the companion package's own yandex-yt-proto
# dependency is not published on PyPI, while its generated *_pb2.py import yt_proto at runtime.
"$DOCKER" run --rm -v "$STAGE_DIR:/src:ro" -v "$PWD:/out" "$DOCKER_IMAGE" sh -c '
set -e
cp -r /src /work
python -m venv /venv
/venv/bin/pip install -q --no-cache-dir \
    "grpcio>=1.70.0" "protobuf>=5.29.0,<7.0.0" ytsaurus-client "grpcio-tools==1.70.0"
/venv/bin/python -m grpc_tools.protoc -I/work/yt --python_out=/venv/lib/python3.12/site-packages \
    yt_proto/yt/core/misc/proto/guid.proto \
    yt_proto/yt/core/misc/proto/error.proto \
    yt_proto/yt/core/ytree/proto/attributes.proto \
    yt_proto/yt/core/yson/proto/protobuf_interop.proto
/venv/bin/pip install -q --no-cache-dir --no-deps /work/yt/yt/flow/tools/python_companion_package
/venv/bin/pip uninstall -y -q grpcio-tools
cp -r /venv/lib/python3.12/site-packages /out/sdk
find /out/sdk -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true
'

tar czf companion_sdk.tgz sdk

ls -l companion_sdk.tgz
