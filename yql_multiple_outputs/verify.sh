#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/../yql_common/lib.sh"

assert_rows "$SCENARIO_ROOT/good_queue" expected_good.json
assert_rows "$SCENARIO_ROOT/bad_queue" expected_bad.json
