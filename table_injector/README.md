# table_injector

A finite, watermark-driven queue-to-queue pipeline built entirely from stock classes (the pipeline
binary is the stock `flow_server`):
`First` (`TSwiftPassthroughOrderedSourceComputation` over a finite `TQueueSource`) → `Second`
(`TPassthroughComputation` grouped by a three-column composite key) → `TAsyncQueueSink` writing
through a queue producer.

What it proves — the upstream test's asserts, verbatim:

- the pipeline drains the input and reaches the `completed` state, driven by the
  `watermark_strategy/watermark_generator` (`out_of_orderness_bound = 5000`,
  `unavailable_partition_groups/min_available_groups = 0`) on the source;
- the output queue holds **exactly** the ten input rows — nothing lost, nothing duplicated —
  after a 1-partition source fanned out to a 5-partition transform in 5-row batches;
- the transform's materialized `input_messages` pipeline table works through its `'any'`-typed
  `key` column: every key is the three-element grouping key `[hash1, hash2, data]`, the keys come
  back sorted, and a composite-key **range query** (`key >= yson_string_to_any('[h1u; h2u]')`
  built from the middle key's two hashes) returns exactly the upper half of them;
- the `states` pipeline table stays empty — a passthrough keeps no per-key state.

The transform writes to `input_messages` (not its compact variant) because the spec pins
`use_compact_input_messages = %false`, exactly like the upstream test: with a `uint64` hash in the
grouping key the compact table would be chosen by default, and the point here is inspecting the
full one.

## Run

From the repo root, with your env file sourced and the stock stripped `flow_server` built (see the
repo README):

```bash
python3 table_injector/yt_sync.py         # once: pipeline node, 2 queues, consumer, producer
python3 table_injector/prepare_data.py    # ten rows into the input queue
./run.sh table_injector                   # deploys and waits; finite pipeline -> the runner
                                          # exits by itself on completion (~1.5 min)
```

The runner streams the controller log and returns once it prints `Pipeline completed`. Then
**verify right away** — see the note below:

```bash
python3 table_injector/verify.py          # the upstream asserts: output == input, keys, states
./stop.sh table_injector                  # aborts the vanilla operation (pipeline is completed)
```

**Verify promptly after completion.** The `input_messages` rows are transient by design: the
controller continuously advances a watermark on the table
(`@custom_runtime_data`, `row_merger_type = "watermark"` in its mount config), and once the
tablet's dynamic stores get flushed the processed rows are merged away for good. Seconds after
completion all ten keys are there (that is when the upstream test reads them too); a few minutes
later the table reads back empty — the output-queue and `states` checks still pass, only the two
`input_messages` checks lose their subject.

## Observed output

Recorded against the demo cluster, `flow_server: 26.2.0-local-os~5c69dd1804e43fe5`:

```
$ python3 table_injector/prepare_data.py
inserted 10 rows into //tmp/timoninmaxim/ytsaurus_dev/table_injector/input_queue

$ ./run.sh table_injector       # deploy 01:55:57 -> "Pipeline completed" 01:57:24 (~1.5 min)

$ yt flow get-pipeline-state "$YT_DEV_ROOT/table_injector/pipeline"
completed

$ python3 table_injector/verify.py
output rows: 10 (expected 10)
input_messages keys: 10
range query from the middle key: 5 keys (expected 5)
OK: output equals input, 'any'-typed input_messages keys behave, states are empty

$ yt select-rows "data from [$YT_DEV_ROOT/table_injector/output_queue] ORDER BY data LIMIT 100" --format json
{"data":"hello_0"}
{"data":"hello_11"}
...
{"data":"hello_9999999999"}

$ ./stop.sh table_injector
pipeline is completed (final state, nothing to stop)
operation c78736c-37702dcc-103e8-326f7fe1 (...) aborted
```

The only retryable error in the controller log was the usual one-off
`FlowViewKeeper is not initialized` at startup.

## Rerunning

There is no way to rewind the consumer or clear the queues, so a repeat run means recreating the
scenario from scratch. `./stop.sh` must come first — while the vanilla operation is alive its
controller holds a lock under the pipeline node and `yt remove` fails with
`Cannot take "exclusive" lock`:

```bash
./stop.sh table_injector
yt remove -r "$YT_DEV_ROOT/table_injector"
python3 table_injector/yt_sync.py
```

Two flavours of caching bite right after the recreation:

- the proxies' table mount cache: the first `insert-rows`/`select-rows` can fail with
  `No such tablet` — the Python client retries writes by itself, for a read just repeat the
  command a few seconds later;
- the proxies' permission/resolve cache: `register_queue_consumer` inside `yt_sync.py` can fail
  with `No such object <id>` (the id of the **deleted** queue; the cached error is even replayed
  verbatim on immediate retries). It expires by itself — rerun `yt_sync.py` after ~a minute; the
  ensure flow is idempotent.

## Differences from the integration test this is ported from

Upstream: `yt/yt/flow/tests/table_injector/` (`test_table_injector.py`, `main.cpp`,
`pipeline.yson`) in the ytsaurus repo. This ports `TestTableInjector.test_simple[fresh_queue]` —
the primary finite path; the same file's lease/worker-kill/spec-API/watermark-advancing tests
drive local worker processes or the management API and are separate subjects, and
`TestTableInjectorWithRemoteQueue` needs a second cluster.

- **Both custom computations are replaced by stock classes**, which is why the stock binary
  suffices:
  - Upstream's `TTableReader` is a passthrough that also derives a `key` column
    (`ToString(len(data))`) whose only consumer is the downstream `group_by_schema`. Here the
    grouping hashes are computed straight from `data`
    (`farm_hash(data)`, `farm_hash(data, 123)`, plus `data` itself as the key column), so the
    derived column disappears and the source becomes the stock passthrough. The asserted key
    structure is unchanged: a three-element `[uint64, uint64, string]` composite key, distinct
    per row.
  - Upstream's `TTableWriter` is a passthrough whose output-schema conversion drops the extra
    column — the stock `TPassthroughComputation` does that conversion; its other override is a
    150 ms sleep in `DoSync`, a test-only pacing artifact with no semantic effect.
- **The input rows are inserted before the deploy**, not after the pipeline starts: without the
  test harness there is no reason to race the finite source, and the asserts do not depend on
  the insertion moment.
- **Numbers are upstream's:** ten rows `hello_ + str(i) * (i + 1)`, source
  `max_rows_per_batch = 5` (the test's `select_limit`), `desired_partition_count` 1 → 5,
  `batch_duration = 100`, `message_distributor/send_queue_max_rows_per_batch = 50`, source-queue
  `update_info_period = 1000` / `unavailable_threshold = 2000`, and the same
  `watermark_strategy`. One worker, one controller, as the upstream federation default.
- **The `old_queue` parametrization is dropped**: it only pre-seeds the queue with an
  already-consumed row (`advance_consumer`) to prove the consumer offset is honoured — a
  variation of the same path, not the primary subset.
- **Rows are inserted and read in the JSON format**, keeping the scenario runnable with
  `ytsaurus-client` alone (the default YSON path needs the separate `ytsaurus-yson` bindings).
