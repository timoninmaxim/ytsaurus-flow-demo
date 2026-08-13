#!/usr/bin/env bash
# Builds companion_sdk.tgz for the job-file route: the companion SDK and its dependencies, laid out
# as an importable directory the job puts on PYTHONPATH.
#
# It is lifted straight out of the companion image, which already has all of it installed — so the
# native wheels (grpcio, protobuf) match the interpreter that will import them, and this script
# needs no ytsaurus checkout of its own.
set -euo pipefail
cd "$(dirname "$0")"

COMPANION_IMAGE="${FLOW_COMPANION_IMAGE:-ytflow-python-companion:latest}"
DOCKER="${DOCKER:-podman}"

rm -rf sdk companion_sdk.tgz

"$DOCKER" run --rm -v "$PWD:/out" "$COMPANION_IMAGE" sh -c '
set -e
cp -r "$(python -c "import site; print(site.getsitepackages()[0])")" /out/sdk
# The packaging machinery is not needed inside the job.
rm -rf /out/sdk/pip /out/sdk/pip-* /out/sdk/pkg_resources \
       /out/sdk/setuptools /out/sdk/setuptools-* /out/sdk/wheel /out/sdk/wheel-*
find /out/sdk -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null || true
'

tar czf companion_sdk.tgz sdk

ls -l companion_sdk.tgz
