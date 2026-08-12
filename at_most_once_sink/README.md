# at_most_once_sink

A queue-to-queue pipeline built entirely from stock classes (the pipeline binary is the stock
`flow_server`) whose subject is the **at-most-once delivery strategy** of `TAsyncQueueSink`. One
computation, two sinks off the same stream:

- `queue` → `output_queue`, with `at_most_once_strategy = {enabled = %true}` — fire-and-forget:
  the sink acks every message to the engine immediately and writes it out in the background, so a
  broken output queue never blocks the pipeline; whatever cannot be written in time is **dropped**.
- `control_queue` → `control_output_queue`, a plain `TAsyncQueueSink` — the default ordered
  strategy holds the epoch until the write succeeds, so it loses nothing and serves as the ground
  truth for what went through the pipeline.

`reader` is `TSwiftPassthroughOrderedSourceComputation` over `TQueueSource` on a five-tablet input
queue (`finite = %false` — the pipeline runs until stopped), so the pipeline has five partitions,
one per input tablet.

Two dynamic-spec knobs bound the loss window, both under
`computations/reader/sinks/queue/parameters/at_most_once_strategy`:

- `suspend_destruction_duration` (default 10 s; the spec starts it at `1d`) — how long the sink's
  pending background writes survive after the job is destroyed (pipeline stop, rebalance). Within
  this window a write that finally succeeds still lands.
- `total_queue_bytes_limit` (default 100 MB) — the in-memory budget for pending writes; once it is
  exhausted, subsequent messages of the epoch are dropped on the floor (the sink logs
  `Async sink queue size limit exceeded`).

**What it proves** — the three loss modes of at-most-once, exercised by unmounting the output
queue at the right moments (an unmounted dynamic table rejects writes, so the sink's background
writes hang in retry):

1. With a generous `suspend_destruction_duration` (`1d`), rows written while the output queue is
   unmounted are *not* lost: the pipeline sails on (control queue fills up), and after the
   pipeline is stopped and the queue mounted back the pending writes complete asynchronously —
   all 500 rows of the first half arrive.
2. With `suspend_destruction_duration = 100` (ms), the same unmount window *does* lose data: the
   250 rows of the third quarter never reach the output queue — the pipeline was stopped, the
   grace expired, the pending writes were abandoned.
3. With `total_queue_bytes_limit = 1`, the sink's budget fits nothing: of the last quarter
   (mounted queue, healthy writes!) only the single in-flight message per partition survives —
   five rows, `payload_750..payload_754`, one per partition.

Final tally, same as the upstream test asserts: the control queue holds **all 1000** payloads
exactly once; the output queue holds **505** = 500 (first half) + 0 (third quarter) + 5 (one per
partition of the last quarter).

## Run

The scenario is interactive: three phases driven from the shell, mirroring the upstream test's
steps. From the repo root, with your env file sourced:

```bash
python3 at_most_once_sink/yt_sync.py      # once: pipeline node, 3 queues (5 tablets), consumer, producer

P="$YT_DEV_ROOT/at_most_once_sink/pipeline"
OUT="$YT_DEV_ROOT/at_most_once_sink/output_queue"
CTL="$YT_DEV_ROOT/at_most_once_sink/control_output_queue"

# Phase 1: unmount the output queue, feed the first half, deploy.
yt unmount-table "$OUT" --sync
python3 at_most_once_sink/write_rows.py 0 500
./run.sh at_most_once_sink                # deploys; Ctrl-C detaches, the pipeline keeps running
```

Wait until the control queue has all 500 rows (the output queue is unmounted and cannot even be
selected from; the runner log meanwhile streams `Received job retryable error … Failed to write to
the queue` — that is the at-most-once sink retrying against the unmounted queue, i.e. the scenario
working):

```bash
yt select-rows "sum(1) as cnt from [$CTL] group by 1" --format json     # -> {"cnt":500}
yt flow get-flow-view "$P" --view-path /state/execution_spec/layout/partitions --format json --cache false \
    | python3 -c 'import json,sys; print("partitions:", len(json.load(sys.stdin)))'   # -> 5
```

Stop the pipeline, mount the queue back, and watch the suspended writes drain on their own —
no pipeline is running, yet the count climbs to 500:

```bash
yt flow stop-pipeline "$P"            # wait for `yt flow get-pipeline-state "$P"` -> stopped
yt mount-table "$OUT" --sync
yt select-rows "sum(1) as cnt from [$OUT] group by 1" --format json     # -> {"cnt":500} within seconds
```

Phase 2: shrink the destruction grace to 100 ms, repeat the unmount window with the third quarter —
this time the rows die with the jobs:

```bash
yt flow set-pipeline-dynamic-spec "$P" \
    --spec-path /computations/reader/sinks/queue/parameters/at_most_once_strategy/suspend_destruction_duration \
    --value 100
yt unmount-table "$OUT" --sync
python3 at_most_once_sink/write_rows.py 500 750
yt flow start-pipeline "$P"
# wait: yt select-rows "sum(1) as cnt from [$CTL] group by 1" -> {"cnt":750}
yt flow stop-pipeline "$P"            # wait for stopped, then give the 100 ms grace a moment
sleep 2
```

Phase 3: starve the sink's memory budget, mount the queue back, feed the last quarter:

```bash
yt flow set-pipeline-dynamic-spec "$P" \
    --spec-path /computations/reader/sinks/queue/parameters/at_most_once_strategy/total_queue_bytes_limit \
    --value 1
yt mount-table "$OUT" --sync
python3 at_most_once_sink/write_rows.py 750 1000

yt select-rows "sum(1) as cnt from [$OUT] group by 1" --format json     # still {"cnt":500}: the third quarter is gone

yt flow start-pipeline "$P"
# wait: yt select-rows "sum(1) as cnt from [$CTL] group by 1" -> {"cnt":1000}
yt flow stop-pipeline "$P"
```

Verify the final tally:

```bash
yt select-rows "data from [$CTL]" --format json | python3 -c '
import json, sys
data = sorted(json.loads(l)["data"] for l in sys.stdin)
expected = sorted(f"payload_{i}" for i in range(1000))
print("control rows:", len(data))
print("control content == all 1000 payloads:", data == expected)'

yt select-rows "sum(1) as cnt from [$OUT] group by 1" --format json     # -> 500 + partition count

yt select-rows "data from [$OUT]" --format json | python3 -c '
import json, sys
data = [json.loads(l)["data"] for l in sys.stdin]
extras = sorted(d for d in data if int(d.split("_")[1]) >= 500)
first_half = sorted(d for d in data if int(d.split("_")[1]) < 500)
print("first-half rows:", len(first_half), "complete:", first_half == sorted(f"payload_{i}" for i in range(500)))
print("extras from later quarters:", extras)'
```

Then `./stop.sh at_most_once_sink` aborts the vanilla operation (the pipeline is already stopped).

## Observed output

Recorded against the demo cluster, `flow_server: 26.2.0-local-os~5c69dd1804e43fe5`:

```
$ yt select-rows "sum(1) as cnt from [$CTL] group by 1" --format json   # phase 1, output unmounted
{"cnt":500}
partitions: 5

$ yt select-rows "sum(1) as cnt from [$OUT] group by 1" --format json   # after stop + mount
{"cnt":500}

$ yt flow get-pipeline-dynamic-spec "$P" --spec-path /computations/reader/sinks/queue/parameters/at_most_once_strategy --format json
{"spec":{"suspend_destruction_duration":100,"total_queue_bytes_limit":1},"version":1918317245491052571}

$ yt select-rows "sum(1) as cnt from [$OUT] group by 1" --format json   # phase 3, before restart
{"cnt":500}

control rows: 1000
control content == all 1000 payloads: True

$ yt select-rows "sum(1) as cnt from [$OUT] group by 1" --format json   # final
{"cnt":505}

first-half rows: 500 complete: True
extras from later quarters: ['payload_750', 'payload_751', 'payload_752', 'payload_753', 'payload_754']
```

505 = 500 + 5 partitions, exactly the upstream assert (`EVENT_COUNT // 2 + partitions_count`); the
five survivors of the 1-byte phase are the first message each partition put in flight after the
restart.

## Rerunning

There is no way to rewind the consumer or clear the queues, so a repeat run means recreating the
scenario from scratch:

```bash
./stop.sh at_most_once_sink
yt remove -r "$YT_DEV_ROOT/at_most_once_sink"
python3 at_most_once_sink/yt_sync.py
```

and starting again from Phase 1. Recreating the queues invalidates the proxies' table mount cache,
so the first `insert-rows` or `select-rows` afterwards can fail with `No such tablet`; the Python
client retries writes by itself, for a read just repeat the command a few seconds later.

## Differences from the integration test this is ported from

Upstream: `yt/yt/flow/tests/at_most_once_sink/` (`test_pipeline.py`, `pipeline/main.cpp`,
`pipeline/pipeline.yson`) in the ytsaurus repo.

- **The custom `TReader` is replaced by the stock `TSwiftPassthroughOrderedSourceComputation`,
  which is why the stock binary suffices.** Upstream's binary exists only to re-emit the `data`
  column of each input row onto the output stream; the stock passthrough source does the same for
  a matching stream schema.
- **Pipeline control goes through the `yt flow` CLI** (`stop-pipeline` / `start-pipeline` /
  `set-pipeline-dynamic-spec --spec-path`) against the vanilla-deployed controller, where upstream
  drives the client API in-process. The vanilla operation keeps running across the pipeline
  stop/start cycles; only `./stop.sh` at the very end aborts it. Both dynamic knobs are
  pre-declared in the spec's `dynamic_spec` so the targeted `--spec-path` writes have a parent to
  land in.
- **Numbers are upstream's:** 1000 events over 5 tablets, written in the same three slices
  (halves/quarters), `suspend_destruction_duration` `1d` → `100` ms, `total_queue_bytes_limit`
  → `1`. Upstream's `partitions_count` (via `get_flow_view` on
  `/state/execution_spec/layout/partitions`) is read here with `yt flow get-flow-view`.
- **Rows are inserted in the JSON format**, where a literal `$` in a column name is doubled
  (`$$tablet_index`). Upstream uses the client's default YSON path, which needs the separate
  `ytsaurus-yson` bindings; this repo asks only for `ytsaurus-client`.
