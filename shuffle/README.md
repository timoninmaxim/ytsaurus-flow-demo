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

Terminal 1 — from this dir: bootstrap the Cypress objects once, fill the input queue, render the
spec, launch:

```bash
python3 yt_sync.py       # once: pipeline node, input_queue (4 tablets) + consumer, output_queue
python3 prepare_data.py  # 1500 rows over 1024 keys, spread evenly across the 4 tablets
jinjanate pipeline.yson.j2 > pipeline.yson   # every {{ VAR }} from the env; an unset one fails the render
podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" ghcr.io/ytsaurus/flow-nightly:dev-0.2.1 --config pipeline.yson
```

Unlike the endless scenarios, this command returns on its own here: the source is finite, so the
runner waits for `completed` and exits.

The first seconds of the controller log look alarming and are not: one
`E ... Failed to confirm leader_controller_address` and three
`W ... Component became broken (/collect_feedback, /build_cache, /update_metrics)`, all with the
inner error `FlowViewKeeper is not initialized`, followed within five seconds by
`I ... Component recovered`. That is the controller answering requests before its flow view exists.

Terminal 2 — check the count, a server-side aggregate, not a full pull, so the check costs the
same at any input size:

```bash
yt flow get-pipeline-state "$YT_DEV_ROOT/shuffle/pipeline"

yt select-rows "sum(1) as cnt from [$YT_DEV_ROOT/shuffle/output_queue] group by 1" --format json
```

Optionally check that the count is right for the right reason — `data` is unique per input row, so
1500 distinct values each appearing once means no loss compensated by a duplicate:

```bash
yt select-rows "data, sum(1) as cnt from [$YT_DEV_ROOT/shuffle/output_queue] group by data" --format json | python3 -c '
import json, sys
rows = [json.loads(l) for l in sys.stdin]
print("distinct data values:", len(rows))
print("max copies of one value:", max(r["cnt"] for r in rows))'
```

And that the stages really did repartition — the partition layout survives the run in the
pipeline's own `flow_state` table, so this works after the pipeline has completed:

```bash
yt select-rows "c, sum(1) as partitions from [$YT_DEV_ROOT/shuffle/pipeline/flow_state] where not is_null(value) group by try_get_string(value, \"/computation_id\") as c" --format json
```

## Observed output

```
$ yt flow get-pipeline-state "$YT_DEV_ROOT/shuffle/pipeline"
completed

$ yt select-rows "sum(1) as cnt from [$YT_DEV_ROOT/shuffle/output_queue] group by 1" --format json
{"cnt":1500}

distinct data values: 1500
max copies of one value: 1

$ yt select-rows "c, sum(1) as partitions from [$YT_DEV_ROOT/shuffle/pipeline/flow_state] where not is_null(value) group by try_get_string(value, \"/computation_id\") as c" --format json
{"c":"reader","partitions":4}
{"c":"shuffle_a","partitions":2}
{"c":"shuffle_b","partitions":3}
{"c":"shuffle_c","partitions":4}
{"c":"shuffle_d","partitions":5}
```

`reader` has one partition per input tablet; 2/3/4/5 are the `desired_partition_count`s from the
dynamic spec. `yt flow describe-pipeline` reports the same layout in `total_partition_count` while
the pipeline runs, but that counter deliberately excludes partitions in `Completed`/`Interrupted`
state, so it drops to 0 stage by stage as the finite pipeline drains and reads 0 everywhere once it
is done. `flow_state` is the durable answer.

## Stop

From the repo root:

```bash
./stop.sh shuffle
```

The pipeline is already `completed` (a final state), so this only aborts the vanilla operation.

## Rerunning

`completed` is a final state that refuses both `stop-pipeline` and a spec update, and there is no
way to rewind the input queue's consumer or clear the output queue, so a repeat run means
recreating the scenario from scratch:

```bash
./stop.sh shuffle
yt remove -r "$YT_DEV_ROOT/shuffle"
python3 yt_sync.py && python3 prepare_data.py
jinjanate pipeline.yson.j2 > pipeline.yson
podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" ghcr.io/ytsaurus/flow-nightly:dev-0.2.1 --config pipeline.yson
```

Recreating the queues invalidates the proxies' table mount cache, so the first `insert-rows` or
`select-rows` afterwards can fail with `Tablet … is not known` / `No such object <id>`. The Python
client retries writes by itself; for a read, just repeat the command a few seconds later.
