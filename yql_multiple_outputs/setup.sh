#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/../yql_common/lib.sh"

yt create map_node "$SCENARIO_ROOT" -r -i >/dev/null

create_queue "$SCENARIO_ROOT/input_queue" \
    '{name=int64_field;type=int64}'
create_queue "$SCENARIO_ROOT/good_queue" \
    '{name=int64_field;type=int64}'
create_queue "$SCENARIO_ROOT/bad_queue" \
    '{name=string_field;type=string}'

insert_json "$SCENARIO_ROOT/input_queue" <<'EOF'
{"int64_field": 1}
{"int64_field": 10}
{"int64_field": 100}
EOF
