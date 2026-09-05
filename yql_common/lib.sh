#!/usr/bin/env bash
# Queue helpers shared by the yql_* scenario setup/verify scripts. Source it:
#   . "$(dirname "$0")/../yql_common/lib.sh"

# create_queue <path> <schema-columns-yson>
# Creates an ordered dynamic table with the Flow queue system columns prepended
# and mounts it. <schema-columns-yson> is the data part of the schema, e.g.
#   '{name=string_field;type=string};{name=int64_field;type=int64}'
create_queue() {
    local path=$1 columns=$2
    yt remove -f "$path"
    yt create table "$path" --attributes \
        "{dynamic=%true;schema=[{name=\"\$timestamp\";type=uint64};{name=\"\$cumulative_data_weight\";type=int64};$columns]}"
    yt mount-table "$path" --sync
}

# create_sorted_table <path> <schema-columns-yson>
# Creates a sorted dynamic table (lookup-join right side) and mounts it. Key
# columns carry sort_order inside <schema-columns-yson>, e.g.
#   '{name=key;type=string;sort_order=ascending};{name=kv_value;type=int64}'
create_sorted_table() {
    local path=$1 columns=$2
    yt remove -f "$path"
    yt create table "$path" --attributes \
        "{dynamic=%true;schema=[$columns]}"
    yt mount-table "$path" --sync
}

# insert_json <path>  — reads JSON rows from stdin (one object per line).
insert_json() {
    yt insert-rows "$1" --format json
}

# read_data_rows <path>  — every row as JSON with the queue system columns
# ($$row_index etc. in JSON encoding) stripped, sorted for stable comparison.
read_data_rows() {
    yt select-rows "* from [$1]" --format json \
        | python3 -c '
import json, sys
rows = []
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    row = json.loads(line)
    for column in list(row):
        if column.startswith("$"):
            del row[column]
    rows.append(json.dumps(row, sort_keys=True))
print("\n".join(sorted(rows)))'
}

# assert_rows <path> <expected-file>  — compares the queue content against a
# file of expected JSON rows (one object per line, order-insensitive).
assert_rows() {
    local path=$1 expected=$2
    local actual
    actual=$(read_data_rows "$path")
    local want
    want=$(python3 -c '
import json, sys
rows = [json.dumps(json.loads(line), sort_keys=True) for line in open(sys.argv[1]) if line.strip()]
print("\n".join(sorted(rows)))' "$expected")
    if [ "$actual" != "$want" ]; then
        echo "MISMATCH in $path" >&2
        diff <(echo "$want") <(echo "$actual") >&2 || true
        return 1
    fi
    echo "rows in $path match $expected"
}
