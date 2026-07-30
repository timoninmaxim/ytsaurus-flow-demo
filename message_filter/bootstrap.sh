#!/usr/bin/env bash
# Creates the scenario's Cypress objects with the vendored yt_sync_mini.
set -euo pipefail
REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source "$REPO_ROOT/common/env.sh"
PYTHONPATH="$REPO_ROOT/lib" python3 "$REPO_ROOT/message_filter/yt_sync.py"
echo "bootstrap done"
