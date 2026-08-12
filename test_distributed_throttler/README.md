# test_distributed_throttler

A queue-to-queue pipeline built entirely from stock classes (the pipeline binary is the stock
`flow_server`) whose subject is Flow's **distributed throttler**: a named quota served centrally by
the pipeline controller and drawn concurrently by every job that references it, so one global rate
limit holds across all partitions and workers.

`reader` (`TSwiftPassthroughOrderedSourceComputation` over `TQueueSource`, `finite = %true`) →
`throttled` (`TPassthroughComputation`, 2 partitions) → `TSyncQueueSink` on the output queue.

The throttler is declared once in the dynamic spec and referenced by id from the computation:

```yson
"dynamic_spec" = {
    "computations" = {
        "throttled" = {
            "input_rows_throttler_id" = "api";   // draw 1 unit per input row from "api"
        };
    };
    "throttlers" = {
        "api" = {"limit" = 50.0; "period" = 1000;};   // 50 units per second, globally
    };
};
```

Before each processing step, every `throttled` job draws its input batch's row count from the
`api` throttler. The throttler clients live in the worker jobs, the token bucket lives in the
controller, and quota travels over an RPC service the controller serves — that is what makes the
limit *distributed* rather than per-process. Both fields are dynamic spec: the limit can be changed
(`yt flow update-dynamic-spec`) without a pipeline restart. Referencing an id that is not declared
under `dynamic_spec/throttlers` is rejected at spec validation.

**What it proves.** The source is finite: the pipeline drains the input queue and reaches
`completed` on its own, with throttling slowing it down but never wedging it. The assertion is the
same as upstream's: the output queue holds exactly the 200 rows that were written to the input
queue — throttling delays messages, it must not drop or duplicate them. The `slow` spec variant
(identical but `limit = 2.0`) makes the rate cap visible in wall-clock time.

## Run

From the repo root:

```bash
python3 test_distributed_throttler/yt_sync.py       # once: pipeline node, input_queue + consumer, output_queue
python3 test_distributed_throttler/prepare_data.py  # 200 rows with distinct values 0..199
./run.sh test_distributed_throttler                 # deploys and streams the controller log until completed
```

The source is finite, so `run.sh` returns on its own; budget about a minute and a half. The first
seconds of the controller log show the startup noise every scenario here shows (a burst of
`Failed to update pipeline … leader_controller_address is not set` from the client polling before
the controller published itself, and `Component became broken … FlowViewKeeper is not initialized`
followed by `Component recovered`) — none of it is specific to this scenario.

Check the state and the count — a server-side aggregate, not a full pull:

```bash
yt flow get-pipeline-state "$YT_DEV_ROOT/test_distributed_throttler/pipeline"

yt select-rows "sum(1) as cnt from [$YT_DEV_ROOT/test_distributed_throttler/output_queue] group by 1" --format json
```

Optionally check the count is right for the right reason — `value` is unique per input row, so 200
distinct values each appearing once means no loss compensated by a duplicate:

```bash
yt select-rows "value, sum(1) as cnt from [$YT_DEV_ROOT/test_distributed_throttler/output_queue] group by value" --format json | python3 -c '
import json, sys
rows = [json.loads(l) for l in sys.stdin]
print("distinct values:", len(rows))
print("max copies of one value:", max(r["cnt"] for r in rows))
print("value range ok:", sorted(r["value"] for r in rows) == list(range(200)))'
```

Then `./stop.sh test_distributed_throttler` aborts the vanilla operation (the pipeline is already
`completed`, a final state, so there is nothing to stop).

## Observed output

```
$ yt flow get-pipeline-state "$YT_DEV_ROOT/test_distributed_throttler/pipeline"
completed

$ yt select-rows "sum(1) as cnt from [$YT_DEV_ROOT/test_distributed_throttler/output_queue] group by 1" --format json
{"cnt":200}

distinct values: 200
max copies of one value: 1
value range ok: True
```

The partition layout, read back from the pipeline's `flow_state` table after completion — one
reader partition per input tablet, two `throttled` partitions sharing the one `api` budget:

```
$ yt select-rows "c, sum(1) as partitions from [$YT_DEV_ROOT/test_distributed_throttler/pipeline/flow_state] where not is_null(value) group by try_get_string(value, \"/computation_id\") as c" --format json
{"c":"reader","partitions":1}
{"c":"throttled","partitions":2}
```

In the run above, one of the two controller jobs happened to land on the same exec node as the
worker and failed with `Failed to bind a server socket to tcp://[::]:10081: Address already in
use` — the flow node's monitoring port is a fixed default and the demo cluster's jobs share the
host network. YT rescheduled it, its standby took leadership, and the pipeline was unaffected;
worth recognizing if you see a `failed` controller job on an otherwise green run.

## Seeing the throttler bite

At `limit = 50` the quota wait for 200 rows (~3 s) drowns in the epoch cadence of a short finite
pipeline, so completion time alone does not show throttling. The `slow` variant is the same spec
with `limit = 2.0`: 200 rows now need ~100 s of quota, which dominates everything else. Recreate
the scenario (see Rerunning) and deploy the variant:

```bash
./run.sh test_distributed_throttler slow
```

Measured on the same cluster, from the controller log (`working` → `completed`):

| Spec | Throttler | Pipeline time |
|------|-----------|---------------|
| `pipeline` | 50 rows/s | 60 s (00:35:15 → 00:36:15) |
| `pipeline_slow` | 2 rows/s | 151 s (00:39:58 → 00:42:29) |

The ~91 s difference is the throttler: 200 rows at 2/s is 100 s of quota, minus the bucket's
initial fill. Both runs end `completed` with the same 200-row, no-loss, no-duplicate output.

## Rerunning

`completed` is a final state that refuses both `stop-pipeline` and a spec update, and there is no
way to rewind the input queue's consumer or clear the output queue, so a repeat run means
recreating the scenario from scratch:

```bash
./stop.sh test_distributed_throttler
yt remove -r "$YT_DEV_ROOT/test_distributed_throttler"
python3 test_distributed_throttler/yt_sync.py && python3 test_distributed_throttler/prepare_data.py
./run.sh test_distributed_throttler        # or: ./run.sh test_distributed_throttler slow
```

Recreating the queues invalidates the proxies' table mount cache, so the first `insert-rows` or
`select-rows` afterwards can fail with `Tablet … is not known` / `No such tablet`. The Python
client retries writes by itself; for a read, just repeat the command a few seconds later.

## Differences from the integration test this is ported from

Upstream: `yt/yt/flow/tests/test_distributed_throttler/` (`test_distributed_throttler.py`,
`pipeline/main.cpp`, `pipeline/pipeline.yson`) in the ytsaurus repo.

- **Only `test_computation_uses_throttler` is ported** (completed + all 200 rows in the output
  queue). The upstream file also has `test_throttler_survives_leader_switch`, which restarts the
  controller mid-run and checks the new leader re-registers the throttlers; that failure path
  needs orchestrated leader switches and is not ported. (Ironically, the run recorded above got an
  unplanned controller failover for free — see the port-collision note — and survived it.)
- **The custom computation is replaced by the engine's built-in throttling, which is why the
  stock binary suffices.** Upstream ships its own binary whose `TThrottledPassthrough` calls
  `GetThrottler(TThrottlerId("api"))->Throttle(1)` per message inside `DoProcessMessage`. The
  engine offers the same thing declaratively: `input_rows_throttler_id` (and its companion
  `input_bytes_throttler_id`) on any computation's dynamic spec draws from the same named
  distributed throttler, per input batch instead of per message. Same throttler declaration, same
  controller-served quota path, no user C++. The programmatic API is still there for computations
  that need to throttle something the engine cannot see (e.g. calls to an external service):
  `TComputationBase::GetThrottler(TThrottlerId)` in
  `yt/yt/flow/library/cpp/computation/computation_base.h` — using it means building your own
  pipeline binary, as upstream's does.
- **Numbers are upstream's:** 200 events, `limit = 50.0` per `period = 1000` ms, reader batches of
  at most 10 rows every 100 ms, `desired_partition_count = 2` for `throttled`, one worker. The
  `slow` variant (limit 2.0) is this port's addition to make the rate cap observable.
- **Rows are inserted in the JSON format**, where a literal `$` in a column name is doubled
  (`$$tablet_index`). Upstream uses the client's default YSON path, which needs the separate
  `ytsaurus-yson` bindings; this repo asks only for `ytsaurus-client`.
