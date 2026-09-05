#!/usr/bin/env bash
set -euo pipefail
. "$(dirname "$0")/../yql_common/lib.sh"

yt create map_node "$SCENARIO_ROOT" -r -i >/dev/null

create_queue "$SCENARIO_ROOT/input_queue" \
    '{name=key;type=string};{name=ts;type=uint32};{name=value;type=int64}'
create_queue "$SCENARIO_ROOT/output_queue" \
    '{name=window_start;type=timestamp};{name=window_end;type=timestamp};{name=key;type=string};{name=sum_values;type=int64};{name=sum_if_values;type=int64};{name=count_values;type=uint64};{name=count_if_values;type=uint64}'

insert_json "$SCENARIO_ROOT/input_queue" <<'EOF'
{"key": "foo", "ts": 110, "value": 1}
{"key": "foo", "ts": 117, "value": 10}
{"key": "bar", "ts": 115, "value": 1}
{"key": "bar", "ts": 117, "value": 10}
{"key": "bar", "ts": 125, "value": 100}
{"key": "baz", "ts": 121, "value": 1}
EOF
