# shuffle

A queue-to-queue pipeline built entirely from stock classes (the pipeline binary is the stock
`flow_server`) whose only job is to move every message across the cluster four times:

`reader` (`TSwiftPassthroughOrderedSourceComputation` over `TQueueSource`, `finite = %true`) →
`shuffle_a` → `shuffle_b` → `shuffle_c` → `shuffle_d` (four `TPassthroughComputation`s) →
`TSyncQueueSink` on the output queue.

The four stages are identical except for their `group_by_schema`: each hashes `key` with a
different multiplier (`farm_hash(key) * 1009`, `* 13`, `* 17`, plain `farm_hash(key)`) and asks for
a different partition count (2, 3, 4, 5). The grouping key is what Flow partitions on, so every hop
regroups the whole stream differently and messages cross partitions — and, with more than one
worker, machines — at every stage. The input queue has 4 tablets, so the stream is already
partitioned before the first hop.

**What it proves.** The source is finite: it reads the queue to its end and the pipeline reaches
`completed` on its own. The assertion is then a single number — the output queue holds exactly as
many rows as were written to the input queue. Under normal operation, four repartitionings lose
nothing and duplicate nothing.

## Run

Needs the prerequisites and the sourced env file from the root README (`yt` CLI,
`ytsaurus-flow-yt-sync-mini` and `jinjanate` in one Python environment, podman).

From this dir: bootstrap the Cypress objects, fill the input queue, render the spec, launch:

```bash
python3 yt_sync.py       # pipeline node, input_queue (4 tablets) + consumer, output_queue
python3 prepare_data.py  # 1500 rows over 1024 keys, spread evenly across the 4 tablets
jinjanate pipeline.yson.j2 > pipeline.yson   # every {{ VAR }} from the env; an unset one fails the render
podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" ghcr.io/ytsaurus/flow-nightly:dev-0.2.1 --config pipeline.yson
```

The source is finite, so the launch returns on its own once the pipeline is `completed`. In the
first seconds the controller log shows a few `FlowViewKeeper is not initialized` errors that recover
within five seconds: the controller answers requests before its flow view exists.

## Check

The pipeline is done:

```bash
yt flow get-pipeline-state "$YT_DEV_ROOT/shuffle/pipeline"
```

```
completed
```

The output queue holds exactly as many rows as were written (a server-side aggregate, so the check
costs the same at any input size):

```bash
yt select-rows "sum(1) as cnt from [$YT_DEV_ROOT/shuffle/output_queue] group by 1" --format json
```

```json
{"cnt":1500}
```

The count is right for the right reason — `data` is unique per input row, so 1500 distinct values
each appearing once means no loss compensated by a duplicate:

```bash
yt select-rows "data, sum(1) as cnt from [$YT_DEV_ROOT/shuffle/output_queue] group by data" --format json | python3 -c '
import json, sys
rows = [json.loads(l) for l in sys.stdin]
print("distinct data values:", len(rows))
print("max copies of one value:", max(r["cnt"] for r in rows))'
```

```
distinct data values: 1500
max copies of one value: 1
```

The stages really did repartition — the partition layout survives the run in the pipeline's own
`flow_state` table (rows come in any order):

```bash
yt select-rows "c, sum(1) as partitions from [$YT_DEV_ROOT/shuffle/pipeline/flow_state] where not is_null(value) group by try_get_string(value, \"/computation_id\") as c" --format json
```

```json
{"c":"reader","partitions":4}
{"c":"shuffle_a","partitions":2}
{"c":"shuffle_b","partitions":3}
{"c":"shuffle_c","partitions":4}
{"c":"shuffle_d","partitions":5}
```

`reader` has one partition per input tablet; 2/3/4/5 are the `desired_partition_count`s from the
dynamic spec. `yt flow describe-pipeline` is no help after the run: its `total_partition_count`
excludes `Completed` partitions and reads 0 everywhere once the pipeline is done.

## Stop

The pipeline is already `completed`, so only its vanilla operation is left — abort it by the id the
runner printed at launch, in the `Started vanilla operation (..., OperationId: <id>)` line:

```bash
yt abort-op <OperationId>
```

To drop the scenario's Cypress objects as well: `yt remove -r "$YT_DEV_ROOT/shuffle"`.

## Rerunning

`completed` is final: it refuses both `stop-pipeline` and a spec update, and the input queue's
consumer cannot be rewound. To run again, stop as above, drop the Cypress objects and repeat Run from
the start. Recreated queues can make the first `select-rows` fail with `Tablet … is not known` /
`No such object <id>` while the proxies' mount cache catches up; repeat it a few seconds later.
