# yql_hopping_aggregate

Windowed streaming aggregation — the YQL `GROUP BY HOP` construct over a
queue: five-second tumbling windows per `key`, with `sum`, `sum_if`, `count`
and `count_if` aggregates and the window bounds exported through
`HOP_START()`/`HOP_END()`. On the Flow side this becomes a stateful keyed
computation driven by watermarks; with finite streams the source exhaustion
advances the watermark and flushes every open window.

The query calls `Datetime::FromSeconds`, so it also proves UDF delivery into
the vanilla jobs (see the run script's UDF handling).

## Run

```bash
source env.sh
./yql_common/run.sh yql_hopping_aggregate
./yql_common/stop.sh yql_hopping_aggregate
```

Six events across three keys in; five window rows out (`sum_if` over an empty
match set is null).
