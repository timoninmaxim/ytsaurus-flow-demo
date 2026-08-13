# transform_high_throughput

A port of the upstream *transform throughput benchmark*: the built-in random-message generator
feeding a **transform** — the mode where everything is materialized to YT per epoch — whose per-key
state lives in the pipeline's built-in `states` table, with the output written to a YT queue by the
async queue sink. One message through the pipeline loads the whole transform write path:
`compact_input_messages`, `output_messages`, `states`, and the output queue. The deliverable is a
*throughput figure* for that path, measured on a live pipeline in `working`.

The pipeline:

- `Reader` — `NYT::NFlow::TSwiftPassthroughOrderedSourceComputation` over the built-in
  `NYT::NFlow::TRandomSource` (infinite: the source generates messages for as long as the pipeline
  runs).
- `Reducer` — the stock `NYT::NFlow::TProcessFunctionComputation` hosting
  `NYT::NFlow::NDemo::TReducer` (`pipeline/main.cpp`), a transform-mode process function: per
  message it loads the state for the message's key from the built-in `states` table
  (`InitClient` / `TMutableStateKeyClient`), bumps `count`, stores the payload as `last_data`, and
  re-emits the message. Grouping is `farm_hash(key)`, so state, ordering, and partitioning are all
  per key.
- `output` — `NYT::NFlow::TAsyncQueueSink` off the Reducer, writing to `output_queue` through the
  `output_producer` queue-producer node.

## Deviation from the upstream benchmark, deliberate

The upstream benchmark (`transform_high_throughput` in the Flow benchmark suite) is a *finite*
stress run on a dedicated local cluster: `finite = %true`, `partition_message_count` sized in the
hundreds of thousands, 10 + 10 partitions, 10K-row batches, and a deliberately shrunken key-state
cache; it measures messages/s as `EVENT_COUNT / elapsed` of the completed run. This demo cluster is
small (16 CPU × 5 nodes) and shared, so this port keeps the pipeline shape and the state/sink load
path but runs it *infinite and modest*: two source partitions, two reducer partitions, one worker,
and the engine's **default batching** (1 s / 1000 rows per batch; the random source is pull-driven,
so the actual rate is however fast the transform commits epochs — measured below at ~9.5K rows/s
total). Throughput is measured on the running pipeline as the growth of the output queue over a
window, not from a completion time.

Two random-source facts the spec accounts for (both bite silently if missed):

- Every source knob must be nested inside `source_streams/random_source/parameters` in the
  dynamic spec — knobs placed a level up are ignored without any diagnostic.
- `message_key_range` is the **Poisson mean** of the key distribution, not a range: the value
  `1000000` concentrates keys in roughly `[996000, 1004000]` — about 6–8 thousand distinct keys,
  which is what the `states` table ends up holding.

## This scenario ships its own binary

`TReducer` is user C++ code, and the stock `flow_server` does not link the random connector
anyway. `pipeline/ya.make` is deliberately minimal: the runner, the two connectors (random source,
queue sink), the process-function host and the computation library. `build.sh` stages the sources
into your ytsaurus checkout, builds with `ya make` and strips the result back here (see
`secret_env/README.md` for why staging is needed).

## Run

From the repo root, with your env file sourced:

```bash
transform_high_throughput/build.sh              # builds + strips the binary (YTSAURUS=<checkout>)
python3 transform_high_throughput/yt_sync.py    # once: pipeline node, output queue (2 tablets), producer
FLOW_BIN=transform_high_throughput/transform_high_throughput_pipeline.stripped \
    ./run.sh transform_high_throughput          # deploy + stream the controller log; Ctrl-C detaches
```

Then, from a second terminal, measure (the script waits out a 60-second window by default;
`python3 measure.py 120` for a longer one):

```bash
python3 transform_high_throughput/measure.py
```

What it does, in order:

1. `get-pipeline-state` must be `working` before and after the window — the pipeline sustains the
   load, not merely survives the sample.
2. Samples the output queue twice, `window` seconds apart: row count via `sum(1)`, bytes via the
   queue's `$cumulative_data_weight` system column (summed over tablets) — and prints **rows/s**
   and **MB/s** of the delta.
3. `states` must be non-empty; prints the row (distinct key) count.

The random source is an unthrottled load generator, so stop the pipeline once measured:

```bash
./stop.sh transform_high_throughput
```

## Knobs

All in `pipeline.yson.template`, all in the dynamic spec (changeable on a running pipeline with
`yt flow set-pipeline-dynamic-spec`):

| Knob | Where | Default here | Effect |
|---|---|---|---|
| `partition_count` | `Reader/source_streams/random_source/parameters` | 2 | source partitions, each capped by the batch settings |
| `message_size_mean` | same | 100 | Poisson mean of the `data` payload size, bytes |
| `message_key_range` | same | 1000000 | Poisson **mean** of the key — sets the distinct-key count (≈ ±4√λ around λ) |
| `desired_partition_count` | `Reducer/parameters` | 2 | reducer partitions |
| `batch_duration`, `max_rows_per_batch` | per computation | engine defaults (1 s, 1000) | per-read batch bounds (the source is pull-driven, so these shape batches, not a hard rate cap); the upstream benchmark runs 100 ms / 10K |

## Observed output

Recorded from the live run on the demo cluster. The pipeline reached `working` in ~14 seconds
(runner log: `Wait finished (CurrentState: Working, TargetState: Working)`; one vanilla job
failed and restarted during startup — the known co-location port race — after which the pipeline
was healthy for the whole run).

The measurement, 60-second window:

```
$ python3 transform_high_throughput/measure.py 60
pipeline state: working
t0 sample: 70000 rows, 8649668 cumulative bytes; measuring for 60s ...
throughput: 9505 rows/s, 1.119 MB/s (+570922 rows in 60.1s, queue at 640922 rows)
ok: states table non-empty (6560 rows)
OK: sustained `working`, 9505 rows/s, states table has 6560 keys
```

So the transform path — per-key state read-modify-write in `states`, output materialized into
`output_messages`, and the async queue sink — sustained **~9500 rows/s (~1.1 MB/s)** end to end
on one worker with two partitions per computation, with the pipeline in `working` before and
after the window.

The per-key state, as the upstream benchmark asserts it (the `states` table also holds the
engine's own source-progress rows under `computation_id = "Reader"`, so the check filters):

```
$ yt select-rows "sum(1) as cnt from [$YT_DEV_ROOT/transform_high_throughput/pipeline/states] \
      where computation_id = 'Reducer' group by 1" --format json
{"cnt":6858}

$ yt select-rows "computation_id, key, state from [...pipeline/states] where computation_id = 'Reducer' limit 2" --format json
{"computation_id":"Reducer","key":[798995128167919,"998816"],"state":{"count":190,"last_data":"µoÝ©ù[9ÙM…"}}
{"computation_id":"Reducer","key":[3411560120071481,"999036"],"state":{"count":250,"last_data":"K_³=?3×ûÏ…"}}
```

Note the keys — `998816`, `999036`: with `message_key_range = 1000000` every key lands within a
few thousand of one million (Poisson mean, not a range), and 6858 distinct keys accumulated by
the end of the run.

Stopping (the queue had ~800K rows by then and the source never stops on its own):

```
$ ./stop.sh transform_high_throughput
no controller answered: the vanilla operation is not running
operation ed5ff2c6-f8a17019-103e8-bfa3aeed (*flow-runner ...) aborted
```

On this run the controller briefly stopped answering right as `stop.sh` sampled it, so the script
took its no-controller branch and went straight to aborting the vanilla operation — the pipeline's
*persisted* state therefore remains `working`, and a later `./run.sh` of the same scenario resumes
it. When the controller does answer, `stop.sh` performs the graceful `stop-pipeline` first.
