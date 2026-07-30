#!/usr/bin/env bash
# Creates the scenario's Cypress objects with the vendored yt_sync_mini.
set -euo pipefail
REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source "$REPO_ROOT/common/env.sh"
# The ensure flow is idempotent; retry to ride out master follower lag
# (a read right after a create can hit a follower that has not seen it yet).
for attempt in 1 2 3; do
    if PYTHONPATH="$REPO_ROOT/lib" python3 "$REPO_ROOT/message_filter/yt_sync.py"; then
        echo "bootstrap done"
        exit 0
    fi
    echo "bootstrap attempt $attempt failed, retrying..." >&2
    sleep 5
done
echo "bootstrap failed after 3 attempts" >&2
exit 1
