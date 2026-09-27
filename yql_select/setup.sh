#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/../yql_common/lib.sh"

yt create map_node "$SCENARIO_ROOT" -r -i >/dev/null

create_queue "$SCENARIO_ROOT/input_queue" \
    '{name=string_field;type=string};{name=int64_field;type=int64}'
create_queue "$SCENARIO_ROOT/output_queue" \
    '{name=string_field;type=string};{name=int64_field;type=int64};{name=bool_field;type=boolean}'

insert_json "$SCENARIO_ROOT/input_queue" <<'EOF'
{"string_field": "foo", "int64_field": 1}
{"string_field": "bar", "int64_field": 10}
{"string_field": "foobar", "int64_field": 100}
EOF
