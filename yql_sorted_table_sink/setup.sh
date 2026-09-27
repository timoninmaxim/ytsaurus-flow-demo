#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/../yql_common/lib.sh"

yt create map_node "$SCENARIO_ROOT" -r -i >/dev/null

create_queue "$SCENARIO_ROOT/input_queue" \
    '{name=key;type=string};{name=value;type=int64}'

# No output bootstrap: the query itself creates the sorted dynamic table.
yt remove -f "$SCENARIO_ROOT/output_table"

insert_json "$SCENARIO_ROOT/input_queue" <<'EOF'
{"key": "foo", "value": 1}
{"key": "bar", "value": 10}
{"key": "baz", "value": 100}
EOF
