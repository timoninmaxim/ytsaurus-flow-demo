#!/usr/bin/env bash
# Writes the 5 input rows; the two "bad" rows must be dropped by the filter.
set -euo pipefail
REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source "$REPO_ROOT/common/env.sh"
YTDIR="$YT_DEV_ROOT/message_filter"

# YT's JSON format encodes a leading "$" in a key as "$$".
ytput insert_rows "{path=\"$YTDIR/input_queue\"; input_format=json}" --data-binary \
$'{"key":"good_0","data":"0","$$tablet_index":0}\n{"key":"bad","data":"1","$$tablet_index":0}\n{"key":"good_1","data":"2","$$tablet_index":0}\n{"key":"bad","data":"3","$$tablet_index":0}\n{"key":"good_2","data":"4","$$tablet_index":0}'
echo "inserted 5 rows into $YTDIR/input_queue"
