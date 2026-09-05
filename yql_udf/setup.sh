#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/../yql_common/lib.sh"

yt create map_node "$SCENARIO_ROOT" -r -i >/dev/null

create_queue "$SCENARIO_ROOT/input_queue" \
    '{name=string_field;type=string}'
create_queue "$SCENARIO_ROOT/output_queue" \
    '{name=string_field;type=string};{name=length_field;type=uint32};{name=bool_field;type=boolean}'

insert_json "$SCENARIO_ROOT/input_queue" <<'EOF'
{"string_field": "foo"}
{"string_field": "bar"}
{"string_field": "foobar"}
EOF
