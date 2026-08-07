#!/usr/bin/env bash
# Writes the 5 input rows; the two "bad" rows must be dropped by the filter.
#
# Assumes the cluster env is already loaded (source common/env.sh once), which points
# the yt CLI at the cluster.
set -euo pipefail
YTDIR="$YT_DEV_ROOT/message_filter"

# YT's JSON format encodes a leading "$" in a key as "$$".
printf '%s\n' \
    '{"key":"good_0","data":"0","$$tablet_index":0}' \
    '{"key":"bad","data":"1","$$tablet_index":0}' \
    '{"key":"good_1","data":"2","$$tablet_index":0}' \
    '{"key":"bad","data":"3","$$tablet_index":0}' \
    '{"key":"good_2","data":"4","$$tablet_index":0}' \
    | yt insert-rows --format json "$YTDIR/input_queue"
echo "inserted 5 rows into $YTDIR/input_queue"
