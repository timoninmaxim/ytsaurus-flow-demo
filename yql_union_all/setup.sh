#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/../yql_common/lib.sh"

yt create map_node "$SCENARIO_ROOT" -r -i >/dev/null

for q in input_queue_a input_queue_b input_queue_c; do
    create_queue "$SCENARIO_ROOT/$q" \
        '{name=string_field;type=string};{name=int64_field;type=int64}'
    insert_json "$SCENARIO_ROOT/$q" <<'EOF'
{"string_field": "foo", "int64_field": 1}
{"string_field": "bar", "int64_field": 10}
EOF
done

create_queue "$SCENARIO_ROOT/output_queue" \
    '{name=string_field;type=string};{name=int64_field;type=int64}'
