#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/../yql_common/lib.sh"

assert_rows "$SCENARIO_ROOT/output_queue" expected.json
