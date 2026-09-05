#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/../yql_common/lib.sh"

key_columns=$(yt get "$SCENARIO_ROOT/output_table/@key_columns" --format json | python3 -c 'import json,sys; print(",".join(json.load(sys.stdin)))')
[ "$key_columns" = "key" ] || { echo "expected the output table to be sorted by [key], got [$key_columns]" >&2; exit 1; }
echo "output table is sorted by [key]"

assert_rows "$SCENARIO_ROOT/output_table" expected.json
