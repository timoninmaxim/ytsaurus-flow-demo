#!/usr/bin/env bash
# Waits for the finite pipeline to complete and checks the filtered output.
set -euo pipefail
REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source "$REPO_ROOT/common/env.sh"
YTDIR="$YT_DEV_ROOT/message_filter"

for _ in $(seq 1 60); do
    STATE=$(ytget get_pipeline_state -G --data-urlencode "pipeline_path=$YTDIR/pipeline")
    [ "$STATE" = '"Completed"' ] && break
    sleep 10
done
echo "pipeline state: $STATE"
[ "$STATE" = '"Completed"' ] || { echo "FAIL: pipeline did not complete" >&2; exit 1; }

KEYS=$(ytget select_rows -G --data-urlencode "query=key from [$YTDIR/output_queue]" --data-urlencode "output_format=json" \
    | python3 -c 'import json,sys; print(",".join(sorted(json.loads(l)["key"] for l in sys.stdin if l.strip())))')
echo "output keys: $KEYS"
[ "$KEYS" = "good_0,good_1,good_2" ] && echo "PASS: bad rows were filtered out" || { echo "FAIL: unexpected keys" >&2; exit 1; }
