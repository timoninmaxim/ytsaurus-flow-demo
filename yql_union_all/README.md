# yql_union_all

`union all` over three input queues into one output queue. Each branch tags
its rows so the output shows every source contributed. On the Flow side this
is a pipeline with three queue sources feeding one sink — the multi-source
wiring is what the scenario checks.

## Run

```bash
source env.sh
./yql_common/run.sh yql_union_all
./yql_common/stop.sh yql_union_all
```

Two rows per input queue, six rows out, suffixed `_first`/`_second`/`_third`
by source.
