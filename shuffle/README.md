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
nothing and duplicate nothing. Nothing here injects a fault, so this is the happy path only; the
failure path is the upstream test this scenario does not port (see the last section).

## Run

From the repo root:

```bash
python3 shuffle/yt_sync.py       # once: pipeline node, input_queue (4 tablets) + consumer, output_queue
python3 shuffle/prepare_data.py  # 1500 rows over 1024 keys, spread evenly across the 4 tablets
./run.sh shuffle                 # deploys and streams the controller log until the pipeline completes
```

Unlike the endless scenarios, `run.sh` returns on its own here: the source is finite, so the
runner waits for `completed` and exits. Budget about two and a half minutes (the measured split
is below).

The first seconds of the controller log look alarming and are not: one
`E ... Failed to confirm leader_controller_address` and three
`W ... Component became broken (/collect_feedback, /build_cache, /update_metrics)`, all with the
inner error `FlowViewKeeper is not initialized`, followed within five seconds by
`I ... Component recovered`. That is the controller answering requests before its flow view exists.

Check the count — a server-side aggregate, not a full pull, so the check costs the same at any
input size:

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

Then `./stop.sh shuffle` aborts the vanilla operation (the pipeline is already `completed`, a
final state, so there is nothing to stop).

## Observed output

`run.sh` ends with, and exits 0 on (cluster URL and Cypress root elided):

```
I	FlowClient	Pipeline completed (Pipeline: <…>/shuffle/pipeline)
```

```
$ yt flow get-pipeline-state "$YT_DEV_ROOT/shuffle/pipeline"
completed

$ yt select-rows "sum(1) as cnt from [$YT_DEV_ROOT/shuffle/output_queue] group by 1" --format json
{"cnt":1500}

distinct data values: 1500
max copies of one value: 1
```

The partition layout, read back after completion:

```
$ yt select-rows "c, sum(1) as partitions from [$YT_DEV_ROOT/shuffle/pipeline/flow_state] where not is_null(value) group by try_get_string(value, \"/computation_id\") as c" --format json
{"c":"reader","partitions":4}
{"c":"shuffle_a","partitions":2}
{"c":"shuffle_c","partitions":4}
{"c":"shuffle_b","partitions":3}
{"c":"shuffle_d","partitions":5}
```

`reader` has one partition per input tablet; 2/3/4/5 are the `desired_partition_count`s from the
dynamic spec. `yt flow describe-pipeline` reports the same layout in `total_partition_count` while
the pipeline runs, but that counter deliberately excludes partitions in `Completed`/`Interrupted`
state, so it drops to 0 stage by stage as the finite pipeline drains and reads 0 everywhere once it
is done. `flow_state` is the durable answer.

Timings, measured from the vanilla operation and the controller log of the run above (four
workers): operation start `23:07:20` → pipeline `working` `23:08:23` → `completed` `23:09:38` —
63 s of YT starting the jobs, then 75 s of pipeline. Nothing was uploaded in this run: the stock
binary was already in the cluster's file cache from an earlier scenario. A first deploy of a
binary the cluster has not seen adds its ~196 MB upload on top.

## Rerunning

`completed` is a final state that refuses both `stop-pipeline` and a spec update, and there is no
way to rewind the input queue's consumer or clear the output queue, so a repeat run means
recreating the scenario from scratch:

```bash
./stop.sh shuffle
yt remove -r "$YT_DEV_ROOT/shuffle"
python3 shuffle/yt_sync.py && python3 shuffle/prepare_data.py
./run.sh shuffle
```

Recreating the queues invalidates the proxies' table mount cache, so the first `insert-rows` or
`select-rows` afterwards can fail with `Tablet … is not known` / `No such object <id>`. The Python
client retries writes by itself; for a read, just repeat the command a few seconds later.

## Differences from the integration test this is ported from

Upstream: `yt/yt/flow/tests/shuffle/` (`test_shuffle.py`, `pipeline.yson`) in the ytsaurus repo.

- **Only `test_basic` is ported.** The upstream file also has
  `test_controller_loss_cancels_and_recreates_jobs`, which stops the controller mid-run and checks
  the workers' jobs are cancelled and recreated without losing rows. That is the failure-path
  counterpart to this scenario and a separate subject.
- **The upstream `use_compact_partition_output` parameter is dropped, because the engine ignores
  it.** `test_basic` runs twice, setting that field on all four stages to switch the partition
  output format — but no such spec field exists (`TComputationSpec::Register` in
  `yt/yt/flow/library/cpp/common/spec.cpp` has only `use_compact_input_messages`), and
  `TUniversalComputationBase::CreateOutputStore` always builds the compact store. The two upstream
  cases are the same configuration run twice. Worth knowing generally: unknown keys under
  `spec/computations/<name>/` are dropped silently — they do not trip
  `abort_on_specs_parseability_error` — so a typo in a spec field costs nothing at submit time and
  simply has no effect.
- **1500 rows, the upstream default** (upstream drops to 200 under sanitizers and takes
  `--test-param SHUFFLE_TOTAL_EVENTS` for perf runs; `prepare_data.py` takes the same number as its
  one optional argument).
- **Four workers, as upstream.** The shuffle is only interesting when partitions actually land on
  different machines; with `worker.count = 1` every hop would repartition inside one process.
- **Rows are inserted in the JSON format**, where a literal `$` in a column name is doubled
  (`$$tablet_index`). Upstream uses the client's default YSON path, which needs the separate
  `ytsaurus-yson` bindings; this repo asks only for `ytsaurus-client`.
