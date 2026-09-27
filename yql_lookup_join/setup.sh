#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/../yql_common/lib.sh"

yt create map_node "$SCENARIO_ROOT" -r -i >/dev/null

create_queue "$SCENARIO_ROOT/input_queue" \
    '{name=key;type=string};{name=value;type=int64}'
create_queue "$SCENARIO_ROOT/output_queue" \
    '{name=key;type=string};{name=value;type=int64};{name=key_before;type=string};{name=key_after;type=string};{name=kv_value;type=int64}'

create_sorted_table "$SCENARIO_ROOT/kv_table" \
    '{name=key;type=string;sort_order=ascending};{name=kv_value;type=int64}'

insert_json "$SCENARIO_ROOT/kv_table" <<'EOF'
{"key": "foo", "kv_value": 10}
{"key": "foobar", "kv_value": 20}
EOF

insert_json "$SCENARIO_ROOT/input_queue" <<'EOF'
{"key": "foo", "value": 1}
{"key": "bar", "value": 2}
{"key": null, "value": 3}
{"key": "foobar", "value": 4}
{"key": "foo", "value": 5}
{"key": "bar", "value": 6}
EOF
