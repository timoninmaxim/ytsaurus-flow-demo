# yql_select

The smallest YQL-over-Flow query: a projection with expressions and a `where`
filter over one input queue, written to one output queue.

```sql
insert into output_queue
select
    string_field || "_ytflow" as string_field,
    int64_field * 100 as int64_field,
    int64_field > 10 as bool_field
from input_queue
where string_field = "foo" or int64_field >= 100;
```

YQL compiles the query into a Flow pipeline (queue source → map computation →
queue sink), bootstraps the pipeline's Cypress objects itself, and launches the
whole thing as one vanilla operation. With finite streams enabled the pipeline
drains the input and completes on its own.

## Run

```bash
source env.sh
./yql_common/run.sh yql_select     # setup + query + wait + verify
./yql_common/stop.sh yql_select    # abort the operation, drop the Cypress root
```

Three input rows go in; the two rows matching the filter come out, transformed.
See `yql_common/README.md` for the binaries this needs and how the whole path
works.
