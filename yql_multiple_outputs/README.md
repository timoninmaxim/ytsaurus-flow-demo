# yql_multiple_outputs

Row routing with `process ... using` and a **variant-returning lambda**: rows
that pass validation go to `good_queue` unchanged; everything else is recast
(here: the number stringified) and lands in `bad_queue`. One input stream, two
sinks — the classic main-flow/dead-letter split, and the multi-output shape of
a Flow computation driven from YQL.

## Run

```bash
source env.sh
./yql_common/run.sh yql_multiple_outputs
./yql_common/stop.sh yql_multiple_outputs
```

Rows 1/10/100 in; `10` comes out the good queue, `"1"` and `"100"` the bad
one.
