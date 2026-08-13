# pipeline_alter

Altering a **live pipeline's static spec**: stop it, edit the spec through the management API,
start it again — no redeploy, the runner and the vanilla operation stay up throughout. The
pipeline itself is a queue-to-queue passthrough built entirely from stock classes (the pipeline
binary is the stock `flow_server`): `reader` (`TSwiftPassthroughOrderedSourceComputation` over a
`TQueueSource`) → `TAsyncQueueSink` writing through a queue producer.

Two variants, each porting one upstream assert:

- **rename** (`pipeline.yson.template`, finite source): stop the working pipeline mid-stream,
  rename the computation `reader` → `reader_renamed` in both the static and the dynamic spec,
  start again — the pipeline completes and the output holds **exactly** the 1000 input rows:
  nothing lost across the stop, nothing duplicated by the restart under the new computation id.
- **source_change** (`pipeline_source_change.yson.template`, infinite source): stop, point the
  reader's queue source at a different queue (`input_queue_alt` / `consumer_alt`), start again —
  the changed source identity produces a **fresh set of partitions**, the old partitions are
  retired (completed) and vanish from the layout, and the old identity's persisted state is
  **erased** rather than left behind as garbage (upstream YTFLOW-525: retirement must complete
  the partitions, not interrupt them).

A source key is `[stream id, source identity (an opaque hash of the identifying params),
partition coordinates...]`; "identity" below is element 1 of that key.

## Run — rename variant

From the repo root, with your env file sourced and the stock stripped `flow_server` built (see
the repo README):

```bash
python3 pipeline_alter/yt_sync.py         # once: pipeline node, 3 queues, 2 consumers, producer
python3 pipeline_alter/prepare_data.py    # 1000 rows into each input queue
./run.sh pipeline_alter                   # deploy; keep it running (Ctrl-C only detaches)
```

From a second terminal, the alter choreography:

```bash
P="$YT_DEV_ROOT/pipeline_alter/pipeline"
yt flow get-pipeline-state "$P"           # poll until `working`, then stop right away
yt flow stop-pipeline "$P"                # graceful: drains in-flight data to an epoch boundary
yt flow get-pipeline-state "$P"           # poll until `stopped`
python3 pipeline_alter/alter_rename.py    # reader -> reader_renamed in static + dynamic spec
yt flow start-pipeline "$P"
yt flow get-pipeline-state "$P"           # poll until `completed` (finite source)
python3 pipeline_alter/verify_rename.py   # the upstream assert: output == input, exactly
./stop.sh pipeline_alter                  # aborts the vanilla operation (pipeline is completed)
```

The dynamic spec throttles the reader (`batch_duration = 1000`, `max_rows_per_batch = 5`) so the
drain takes long enough to stop the pipeline mid-stream. Expect the graceful stop to flush
everything already read by the source: in the recorded run the output had 510 rows when
`stop-pipeline` was issued and 1000 once the state reached `stopped` — the stop drains
read-ahead, it does not discard it. The meat of the assert is therefore the restart: the renamed
computation picks the pipeline up, completes it, and the output is exactly the input — no
duplicates from re-reading, no losses.

## Observed output — rename

Recorded against the demo cluster, `flow_server: 26.2.0-local-os~5c69dd1804e43fe5`:

```
$ ./run.sh pipeline_alter                 # deploy 03:10:20 -> working 03:10:53

$ yt select-rows "sum(1) as c from [$YT_DEV_ROOT/pipeline_alter/output_queue] group by 1"
{"c":510}                                 # pre-stop: mid-drain
$ yt flow stop-pipeline "$P"              # draining 03:11:08 -> stopped 03:11:12
{"c":1000}                                # post-stop: the graceful stop drained the read-ahead

$ python3 pipeline_alter/alter_rename.py
static spec: computation 'reader' renamed to 'reader_renamed'
dynamic spec: computation 'reader' renamed to 'reader_renamed'

$ yt flow start-pipeline "$P"             # working 03:11:46 -> completed 03:11:52

$ python3 pipeline_alter/verify_rename.py
output rows: 1000 (expected 1000)
OK: output equals input — the data survived the computation rename intact
```

The runner (still attached from before the stop) logged
`Job completed (... ComputationId: reader_renamed)` — the renamed computation really ran — then
`Pipeline completed` and exited 0 by itself. `./stop.sh pipeline_alter` aborted the vanilla
operation `23c72033-2ee87da-103e8-7fc8cad2`.

## Run — source_change variant

Recreate the scenario first (see Rerunning below), then:

```bash
python3 pipeline_alter/yt_sync.py
python3 pipeline_alter/prepare_data.py
./run.sh pipeline_alter source_change     # same pipeline, but finite = %false

# second terminal:
P="$YT_DEV_ROOT/pipeline_alter/pipeline"
yt flow get-pipeline-state "$P"                 # poll until `working`
python3 pipeline_alter/source_state.py capture  # record the original source identities
                                                # (waits until their state is persisted)
yt flow stop-pipeline "$P"                      # poll until `stopped`
python3 pipeline_alter/alter_source.py          # queue_path/consumer_path -> the alt queue
yt flow start-pipeline "$P"                     # poll until `working`
python3 pipeline_alter/source_state.py verify   # fresh identities, old ones gone + state erased
./stop.sh pipeline_alter
```

## Observed output — source_change

```
$ python3 pipeline_alter/source_state.py capture
captured 1 original source identities: ['b743106067e4d9ad7c3e3500b4e1e64d']

$ yt flow stop-pipeline "$P"              # draining 03:14:30 -> stopped 03:14:55
$ python3 pipeline_alter/alter_source.py
static spec: reader source switched to input_queue_alt / consumer_alt
$ yt flow start-pipeline "$P"             # working 03:15:01

$ python3 pipeline_alter/source_state.py verify
new source identities: ['ac58b670aa2b13959656cf71318268d9']
reader state identities: ['ac58b670aa2b13959656cf71318268d9']
OK: the source-path change produced fresh partitions and erased the old source's state
```

`./stop.sh pipeline_alter` then stopped the pipeline and aborted operation
`1ca6f490-95e9c1dd-103e8-7131fd11`.

### The switched-to queue is not re-read from offset 0

An observation beyond the upstream asserts, recorded here because it will surprise anyone
following the upstream test's comment ("the new physical source is read from offset 0"): after
the switch, the alt queue's **pre-existing 1000 rows never reached the output**. The new
partition's source state was seeded with the retired source's committed offset
(`persisted_offset_exclusive = 1000` in `read-states` immediately after the restart), the fresh
`consumer_alt` was force-advanced to offset 1000, and the source began reading at row 1000 of
the new queue. The pipeline is healthy from there on: three probe rows inserted into
`input_queue_alt` after the switch appeared in the output within seconds, through a fresh
producer session (`sequence_number = 3`) — so this is offset inheritance at the source, not
producer-side deduplication. If you switch a source to a queue whose existing content you want
processed, check `read-states` / the consumer offset after the restart before trusting that the
backlog will be consumed.

## Rerunning

There is no way to rewind the consumers or clear the queues, so a repeat run (including moving
from the rename variant to the source_change one) means recreating the scenario from scratch.
`./stop.sh` must come first — while the vanilla operation is alive its controller holds a lock
under the pipeline node and `yt remove` fails with `Cannot take "exclusive" lock`:

```bash
./stop.sh pipeline_alter
yt remove -r "$YT_DEV_ROOT/pipeline_alter"
python3 pipeline_alter/yt_sync.py
```

Two flavours of caching bite right after the recreation (both expire by themselves within about
a minute; both were hit while recording this scenario):

- the proxies' table mount cache: the first `insert-rows` can fail with `No such tablet` — the
  Python client retries it by itself;
- the proxies' permission/resolve cache: `register_queue_consumer` inside `yt_sync.py` can fail
  with `No such object <id>` (the id of the **deleted** queue). Rerun `yt_sync.py` after ~a
  minute; the ensure flow is idempotent.

## Differences from the integration test this is ported from

Upstream: `yt/yt/flow/tests/pipeline_alter/` (`test_alter.py`, `pipeline/main.cpp`,
`pipeline/pipeline.yson`) in the ytsaurus repo. The rename variant ports
`TestComputation.test_rename[1c_1w_stop_greedy]` (stop + computation rename — the one
parametrization that asserts data intactness); the source_change variant ports
`test_change_source_path` (fresh identities) merged with
`test_source_change_erases_old_state[live_controller]` (state erasure), which differ only in
the source's finiteness and which halves of the same restart they inspect.

- **The custom `TReader` is replaced by the stock class**, which is why the stock binary
  suffices: upstream's reader is a `TSwiftOrderedSourceComputation` that copies the `data`
  column from input to output — exactly `NYT::NFlow::TSwiftPassthroughOrderedSourceComputation`.
- **The spec surgery goes through the management API from the dev host** (`get-pipeline-spec` /
  `set-pipeline-spec` and the dynamic-spec pair), exactly as the upstream test drives its local
  federation; the vanilla operation and the attached runner keep running across the whole
  stop → alter → start cycle.
- **One tablet per input queue** instead of upstream's five. The asserts do not depend on the
  partition count, and a single tablet keeps `prepare_data.py` free of the `$tablet_index`
  column (writable with plain JSON rows).
- **The reader is throttled** via the dynamic spec (upstream is not): on a real cluster the
  stop must land mid-drain to be worth anything, and an unthrottled 1000-row drain finishes
  before the deploy is even reported `working`.
- **Numbers are upstream's:** 1000 rows `payload_0..999` written to both input queues before
  the deploy; one worker, one controller; `use_cpu_aware_balancer = %false` (the greedy
  parametrization).
- The other rename targets (`source_stream`, `sink`, `pipeline_stream`) and the
  pause-based update path are variations of the same choreography and are dropped; upstream
  itself only asserts completion (not data intactness) for them, since they discard state by
  design. The `restarted_controller` erasure variant restarts the local federation processes
  between the spec change and the retirement — with a vanilla deployment that subject belongs
  to release choreography, not to this scenario.
- **Rows are inserted and read in the JSON format**, keeping the scenario runnable with
  `ytsaurus-client` alone (the default YSON path needs the separate `ytsaurus-yson` bindings).
