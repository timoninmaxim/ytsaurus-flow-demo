# yql_sorted_table_sink

A **sorted dynamic table as the sink**, created by the query itself:
`replace into ... order by key` makes the provider derive the table's key
columns from the `order by` clause, create the table, and write the stream
into it as upserts. The other scenarios end in queues; this one lands in a
key-value table you can `select-rows` from — the streaming-materialized-view
pattern.

## Run

```bash
source env.sh
./yql_common/run.sh yql_sorted_table_sink
./yql_common/stop.sh yql_sorted_table_sink
```

Three rows in; the same three rows in the created table, which is verified to
be keyed by `key`.
