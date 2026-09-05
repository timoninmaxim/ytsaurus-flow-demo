# yql_stream_two_lookups

One stream enriched through **two chained lookup joins**: the queue source is
inner-joined with dictionary table A, and the result with dictionary table B
(`on second_arg.key = third_arg.key`). Flow chains two lookup stages in one
pipeline — the multi-stage enrichment pattern.

Stream-to-stream joins are not supported by the engine (the upstream suite
marks them expected-to-fail); joining a stream against dynamic tables, as
here, is the supported form.

## Run

```bash
source env.sh
./yql_common/run.sh yql_stream_two_lookups
./yql_common/stop.sh yql_stream_two_lookups
```

Six stream rows and two two-row dictionaries in; two rows out, carrying values
from both dictionaries.
