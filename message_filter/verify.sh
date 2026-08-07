#!/usr/bin/env bash
# Waits for the finite pipeline to complete and checks the filtered output.
#
# Assumes the cluster env is already loaded (source common/env.sh once), which points
# the yt CLI at the cluster.
set -euo pipefail
YTDIR="$YT_DEV_ROOT/message_filter"

for _ in $(seq 1 60); do
    STATE=$(yt flow get-pipeline-state "$YTDIR/pipeline")
    [ "$STATE" = "completed" ] && break
    sleep 10
done
echo "pipeline state: $STATE"
[ "$STATE" = "completed" ] || { echo "FAIL: pipeline did not complete" >&2; exit 1; }

KEYS=$(yt select-rows --format json "key from [$YTDIR/output_queue]" \
    | python3 -c 'import json,sys; print(",".join(sorted(json.loads(l)["key"] for l in sys.stdin if l.strip())))')
echo "output keys: $KEYS"
[ "$KEYS" = "good_0,good_1,good_2" ] && echo "PASS: bad rows were filtered out" || { echo "FAIL: unexpected keys" >&2; exit 1; }
