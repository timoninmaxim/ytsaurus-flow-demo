# keep_order_mode

A four-stage shuffle pipeline built entirely from stock classes (the pipeline binary is the stock
`flow_server`) whose subject is the **strict message-ordering guarantee**: with
`distribution_ordering = "strict"` on every computation, per-key message order survives the whole
chain, hop by hop.

```
Reader ──> Shuffle_1 ──> Shuffle_2 ──> QueueReducer ──> output_queue
(7 part.)  (8 part.)     (8 part.)     (8 part.)
```

- `Reader` is `TSwiftPassthroughOrderedSourceComputation` over `TQueueSource` on a seven-tablet
  input queue (`finite = %true` — the pipeline completes when the input is drained). Its
  `watermark_strategy/event_timestamp_assigner` takes event timestamps from the `event_time`
  column.
- `Shuffle_1` / `Shuffle_2` / `QueueReducer` are `TPassthroughComputation`s that all group by
  `reduce_id` but with **different hash expressions** (`farm_hash(reduce_id) * 1009`, `* 13`,
  plain), so every hop reshuffles the keys across a different 8-partition layout while all events
  of one `reduce_id` still meet in a single partition per hop.
- Every computation with inputs sets `input_ordering = {time_type = "event_time"}`, and the input
  rows carry **random** event times — deliberately uncorrelated with write order — so nothing but
  the strict-ordering machinery can reproduce the input sequence.
- `QueueReducer` drops the marker events (`event_id = -1`) via the dynamic-spec
  `skip_if_expression`, and its sink stream schema projects `event_time` away; the stock schema
  conversion drops the column on output.

The data set is the upstream test's: 2500 events over ten profiles (`reduce_id` 0..9,
tablet = `reduce_id % 7`). Within each profile `event_id` counts up 0, 1, 2, ... in write order;
402 interleaved events carry `event_id = -1` and must not reach the output.

**What it proves** — for every profile, the `event_id` sequence read back from the output queue in
queue order is exactly `0, 1, 2, ..., n-1`: nothing lost, nothing duplicated, nothing reordered,
after three group-by hops with disagreeing hash layouts, 7→8→8→8 partition fan-out over two
workers, and event-time noise that would scramble the sequence if anything ordered by time instead
of arrival.

## Run

From the repo root, with your env file sourced and the stock stripped `flow_server` built (see the
repo README):

```bash
python3 keep_order_mode/yt_sync.py         # once: pipeline node, 2 queues, consumer
python3 keep_order_mode/prepare_data.py    # 2500 rows across the 7 input tablets
./run.sh keep_order_mode                   # deploys and waits; finite pipeline -> the runner
                                           # exits by itself on completion (~2 min)
```

The runner streams the controller log and returns once it prints `Pipeline completed`. Then:

```bash
yt select-rows "sum(1) as cnt from [$YT_DEV_ROOT/keep_order_mode/output_queue] group by 1" --format json
python3 keep_order_mode/verify.py          # the upstream asserts: exact order, no losses, no dups
./stop.sh keep_order_mode                  # aborts the vanilla operation (pipeline is completed)
```

## Observed output

Recorded against the demo cluster, `flow_server: 26.2.0-local-os~5c69dd1804e43fe5`:

```
$ python3 keep_order_mode/prepare_data.py
inserted 2500 rows into //tmp/timoninmaxim/ytsaurus_dev/keep_order_mode/input_queue (402 of them event_id=-1, expected output rows: 2098)

$ yt flow get-pipeline-state "$YT_DEV_ROOT/keep_order_mode/pipeline"
completed

$ yt select-rows "sum(1) as cnt from [$YT_DEV_ROOT/keep_order_mode/output_queue] group by 1" --format json
{"cnt":2098}

$ python3 keep_order_mode/verify.py
output rows: 2098 (expected 2098)
OK: every profile's sequence is exactly ordered, no losses, no duplicates: {0: 316, 1: 237, 2: 267, 3: 134, 4: 198, 5: 217, 6: 204, 7: 170, 8: 182, 9: 173}

$ ./stop.sh keep_order_mode
pipeline is completed (final state, nothing to stop)
operation aec07e2c-ebc1b1db-103e8-3111d045 (...) aborted
```

Deploy to `Pipeline completed` took about two minutes; the only retryable error in the controller
log was the usual one-off `FlowViewKeeper is not initialized` at startup.

## Rerunning

There is no way to rewind the consumer or clear the queues, so a repeat run means recreating the
scenario from scratch:

```bash
yt remove -r "$YT_DEV_ROOT/keep_order_mode"
python3 keep_order_mode/yt_sync.py
```

Recreating the queues invalidates the proxies' table mount cache, so the first `insert-rows` or
`select-rows` afterwards can fail with `No such tablet`; the Python client retries writes by
itself, for a read just repeat the command a few seconds later.

## Differences from the integration test this is ported from

Upstream: `yt/yt/flow/tests/keep_order_mode/` (`test_pipeline.py`, `pipeline/main.cpp`,
`pipeline/pipeline.yson`) in the ytsaurus repo. This ports the happy path of `test_basic`; the
`test_repartitioning_and_killing` variant (repartition-then-kill-workers loops) drives worker
processes directly and has no vanilla-deployment equivalent.

- **All three custom computations are replaced by stock classes**, which is why the stock binary
  suffices:
  - Upstream's `TReader` only zstd-decodes a YSON blob from the input rows into typed columns — a
    transport encoding of the test harness, not the ordering subject. Here the input queue carries
    the typed columns (`reduce_id`, `event_id`, `event_time`) directly and the stock passthrough
    source reads them.
  - Upstream's `TRacyPassthroughComputation` is a passthrough plus a **random 0–4 s delay in
    `DoPrepare`** — a stress amplifier widening the race window between interrupting and executing
    partitions, aimed at the injected process restarts (`problems=True`) and the
    repartitioning/killing test. The happy path injects no restarts, so the delay is dropped; the
    assert is unchanged and still exercised by the multi-hop reshuffle itself (jobs of all four
    computations still start, hand over, and rebalance across the two workers while data flows).
  - Upstream's `TQueueReducer` is a passthrough that drops `event_id = -1` and strips
    `event_time`; both are expressed in the spec (`skip_if_expression`, output-schema projection).
- **`relaxed_ordering` does not exist in the current spec surface** — today's knob is
  `distribution_ordering` (`"strict"` / `"relaxed"`, default `"strict"`). The spec sets it to
  `"strict"` explicitly on every computation, which is what upstream's
  `computation["relaxed_ordering"] = False` intends. (The upstream test still writes the old
  field name, which the spec parser silently ignores — a stale no-op.)
- **Two workers, one controller** as upstream, minus the bullied-process federation
  (`ProblemsConfig` soft restarts) — that harness supervises local processes and cannot run
  inside vanilla jobs.
- **Numbers are upstream's non-sanitizer set:** 2500 events, ten profiles, seven input tablets,
  `desired_partition_count = 8` per shuffle hop, reader `batch_duration = 100` /
  `max_rows_per_batch = 10`, `message_distributor/send_queue_batch_duration = "1s"`.
- **Verification replays the upstream generator** (`prepare_data.generate_events` is
  deterministic; only `event_time` is random, and the expected output does not depend on it) and
  checks the same four asserts: profile set equality, no duplicates, no missing events, exact
  per-profile sequence equality — reading the output queue in `($tablet_index, $row_index)`
  order.
- **Rows are inserted in the JSON format**, where a literal `$` in a column name is doubled
  (`$$tablet_index`). Upstream uses the client's default YSON path, which needs the separate
  `ytsaurus-yson` bindings; this repo asks only for `ytsaurus-client`.
