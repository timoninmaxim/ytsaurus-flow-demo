#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/../yql_common/lib.sh"

yt create map_node "$SCENARIO_ROOT" -r -i >/dev/null

create_queue "$SCENARIO_ROOT/input_queue" \
    '{name=key;type=string};{name=value;type=int64}'
create_queue "$SCENARIO_ROOT/output_queue" \
    '{name=key;type=string};{name=value;type=int64};{name=key_before;type=string};{name=key_after;type=string};{name=kv_value_a;type=int64};{name=kv_value_b;type=int64}'

create_sorted_table "$SCENARIO_ROOT/kv_table_a" \
    '{name=key;type=string;sort_order=ascending};{name=kv_value_a;type=int64}'
create_sorted_table "$SCENARIO_ROOT/kv_table_b" \
    '{name=key;type=string;sort_order=ascending};{name=kv_value_b;type=int64}'

insert_json "$SCENARIO_ROOT/kv_table_a" <<'EOF'
{"key": "foo", "kv_value_a": 12}
{"key": "foobar", "kv_value_a": 14}
EOF

insert_json "$SCENARIO_ROOT/kv_table_b" <<'EOF'
{"key": "foo", "kv_value_b": 16}
{"key": "foobar", "kv_value_b": 18}
EOF

insert_json "$SCENARIO_ROOT/input_queue" <<'EOF'
{"key": "foo", "value": 1}
{"key": "bar", "value": 2}
{"key": null, "value": 3}
{"key": "foobar", "value": 4}
{"key": "foo", "value": 5}
{"key": "bar", "value": 6}
EOF
