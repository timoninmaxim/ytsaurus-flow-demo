# yql_select_star

`select * from input_queue` — the identity pipeline. Verifies that the whole
YQL-over-Flow path (compile, bootstrap, vanilla launch, queue source, queue
sink, finite-stream completion) moves rows untouched, including the column-set
inference behind `*` over a schematized queue.

## Run

```bash
source env.sh
./yql_common/run.sh yql_select_star
./yql_common/stop.sh yql_select_star
```

Three rows in, the same three rows out.
