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

## Python companion variant

`companion_py/` re-runs the benchmark with the per-key-state reducer written in **Python**, hosted
by the stock `flow_server` — no custom binary. The pipeline shape is the same: a two-partition
reader feeding a two-partition transform (`farm_hash(key)` grouping, per-key state in the built-in
`states` table) whose output the async queue sink writes to a two-tablet queue, on one worker.
`main.py` registers the single `Reducer` computation with the Flow Python companion SDK
(`BatchFunction`, per-key internal state `"state"` declared in `parameters/internal_states`); the
reader stays native (`TSwiftPassthroughOrderedSourceComputation`), so the Python code sits exactly
where the C++ user code sat — on the transform path. The companion delivery is the launcher +
bundle pair the other `companion_py` scenarios use (`entrypoint = ./py_companion`, two
`local_files`, worker `port_count = 3`).

Two deliberate adaptations against the C++ variant:

- **The input is a queue fed from the dev host, not `TRandomSource`** — the stock binary does not
  link the random connector. `companion_py/feed.py` stands in for the generator and mirrors its
  distributions: keys from the normal approximation of Poisson(1000000) (≈6–8 thousand distinct
  keys, exactly like `message_key_range = 1000000`), 100-byte payloads, rows alternating between
  the two input tablets (the two source partitions). For the figure to be about the *pipeline*,
  the feed must outrun it: `--rate 15000` (~1.6× the C++ figure) held with no "behind" warnings on
  the measured run, and `companion_py/measure.py` fails the measurement outright unless the input
  backlog stays non-empty through the window.
- **The measurement is the reference `measure.py`** loaded as a module with its paths switched to
  this variant's root, plus the two checks a fed pipeline owes on top: the backlog condition above
  (rows written minus the consumer's committed offsets), and the per-key state count filtered to
  `computation_id = 'Reducer'` (the `states` table also holds the engine's source-progress rows).
  One practical amendment: the selects pass an explicit `input_row_limit`, because a queue fed at
  15K rows/s crosses the server's default 1M-row select scan limit within minutes.

### Run

From the repo root, with your env file sourced:

```bash
transform_high_throughput/companion_py/build.sh          # companion_bundle.tgz: CPython + SDK + main.py
python3 transform_high_throughput/companion_py/yt_sync.py     # once: pipeline node, queues, consumer, producer
```

On a cluster with fewer than six online data nodes, clear the erasure codec the pipeline preset
puts on the system tables (see `state_joiner/README.md` for the full story of this sharp edge)
before the first deploy:

```bash
for t in $(yt find "$YT_DEV_ROOT/transform_high_throughput_py" --type table); do
    yt set "$t/@erasure_codec" none
    yt set "$t/@hunk_erasure_codec" none
    yt remount-table "$t"
done
```

Then deploy and, from a second terminal, feed and measure:

```bash
FLOW_BIN=~/ytsaurus/yt/yt/flow/bin/flow_server/flow_server.stripped \
    ./run.sh transform_high_throughput py     # deploy + stream the controller log; Ctrl-C detaches

python3 transform_high_throughput/companion_py/feed.py --duration 900   # keep it running…
python3 transform_high_throughput/companion_py/measure.py               # …while this measures

./stop.sh transform_high_throughput_py        # stop the pipeline + abort the vanilla operation
```

### Observed output

Recorded from the live run on the demo cluster, server build `26.2.0-local-os~1bdcb82f3ab63fcb`,
one worker, feed sustained at ~15,000 rows/s (feeder log: `fed 2505000 rows, 15069 rows/s
achieved`, zero "behind" warnings over the whole run). The pipeline reached `working` in ~16
seconds and held it before, through, and after the window:

```
$ python3 transform_high_throughput/companion_py/measure.py 60
pipeline state: working
t0 sample: 698475 rows, 86385673 cumulative bytes, input backlog 1900803 rows; measuring for 60s ...
throughput: 5726 rows/s, 0.672 MB/s (+345000 rows in 60.2s, queue at 1043475 rows)
input backlog: 1900803 rows at t0 -> 2420475 rows at t1
ok: states table has 7262 Reducer keys
OK: sustained `working`, 5726 rows/s with a non-empty input backlog, states table has 7262 keys
```

So the same transform path with the user code in Python sustained **~5,700 rows/s (~0.67 MB/s)**
against the C++ variant's **~9,500 rows/s (~1.12 MB/s)** — about **60%** of the C++ figure, with
identical per-row data weight (117.6 bytes/row both ways). The backlog *grew* by half a million
rows during the window, so the figure is the pipeline's ceiling, not the feeder's.

Where the time goes, from the flow view's per-epoch timings after ~20 minutes of load — the
Reducer partitions spend ~74% of epoch wall time in `Process` (the companion call path), and the
Reader partitions spend ~78% blocked on `Distribute.OutputBufferOverflow`, i.e. waiting for the
Reducer to drain — the Python transform is the bottleneck, everything upstream idles behind it:

```
Reader  total=81.1s {'Distribute.OutputBufferOverflow': 63.2, 'Commit': 8.5, 'Input.Fetch': 3.5, ...}
Reducer total=81.7s {'Process': 60.4, 'Commit': 7.8, 'FinalizeTransaction': 3.7, ...}
```

The companion ran as a **single CPython process**: the SDK's auto-sizing
(`companion_process_count = 0`, the default) resolves the process count from the job's cgroup CPU
quota, and this cluster's job environment reports no CPU limit, which auto-sizing deliberately
treats as "do not fan out" (worker job stderr):

```
INFO:...companion.sizing:Resolved CPU quota from cgroup v1 (Quota: inf, Source: /sys/fs/cgroup/cpu)
```

So the 5,700 rows/s figure is one Python interpreter on the whole transform path — the honest
stock-defaults number, not the SDK's multi-process ceiling.

The per-key state, filtered as the C++ check is (the same key neighbourhood around one million;
the companion's internal state is engine-opaque bytes, so it lands in the `states` row as a YSON
payload string rather than the C++ variant's structured map):

```
$ yt select-rows "computation_id, key, state from [...pipeline/states] where computation_id = 'Reducer' limit 2" --format json
{"computation_id":"Reducer","key":[798995128167919,"998816"],"state":{"payload":"{\"count\"=200;\"last_data\"=\"msnvv…\";}"}}
{"computation_id":"Reducer","key":[3411560120071481,"999036"],"state":{"payload":"{\"count\"=276;\"last_data\"=\"pvvxd…\";}"}}
```
