# yql_lookup_join

A **lookup join**: a stream (queue source) inner-joined `using (key)` with a
sorted dynamic table. Flow executes it as per-row lookups into the dynamic
table rather than materializing the right side, so the table can be big and
live — this is the streaming-enrichment pattern (events joined with a
dictionary).

The query also exercises the surrounding plumbing: a filtered derived stream
(`where value > 2`), a null join key (dropped by the inner join), and a final
projection with another filter over the joined columns.

## Run

```bash
source env.sh
./yql_common/run.sh yql_lookup_join
./yql_common/stop.sh yql_lookup_join
```

Six stream rows and a two-row dictionary go in; the two rows that survive the
filter, the join, and the post-join predicate come out enriched with
`kv_value`.
