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

## Go companion variant

`companion_go/` re-runs the benchmark with the per-key-state reducer written in **Go**
(`go.ytsaurus.tech/yt/go/flow`), hosted by the stock `flow_server` — no custom binary. The pipeline
shape is unchanged: a two-partition native reader feeding a two-partition transform
(`farm_hash(key)` grouping, per-key internal state `"state"` persisted into the built-in `states`
table) whose output the async queue sink writes to a two-tablet queue, on one worker. `main.go`
registers the single `Reducer` computation (`flow.BatchFunction` + `flow.OpenYSONState`, grouping
the mixed-key batch in first-appearance order, exactly like the Python variant); the reader stays
native, so the Go code sits exactly where the C++ user code sat — on the transform path. **The
pipeline binary is its own runner**, as in the other `companion_go` variants: the same `main`
calling `pipeline.Run()` is the companion served inside the worker job and the launcher run on the
dev host, so the spec has no `streams` block (the `event`/`out` schemas are injected), no
`entrypoint` and no `local_files`.

The two adaptations proven by the Python variant carry over unweakened: the input is a queue fed
from the dev host (`companion_go/feed.py`, same distributions as `TRandomSource` under
`message_key_range = 1000000`), and `companion_go/measure.py` is the reference method plus the
fed-input honesty checks (the input backlog must stay non-empty through the window, the state
count filters `computation_id = 'Reducer'`, selects pass an explicit `input_row_limit`).

### Run

From the repo root, with your env file sourced (the sibling `~/ytsaurus` checkout provides the SDK
through the `replace` in `go.mod`; `./run.sh` does not fit the Go route — the Go runner is the
pipeline binary itself and needs `--flow-bin` — so the template is rendered with a one-liner and
the binary launched directly):

```bash
transform_high_throughput/companion_go/build.sh    # go build; GO="ya tool go" if no system go
(cd transform_high_throughput/companion_go && ${GO:-go} test ./...)  # offline reducer-logic proof

python3 transform_high_throughput/companion_go/yt_sync.py  # once: pipeline node, queues, consumer, producer
```

On a cluster with fewer than six online data nodes, clear the erasure codec the pipeline preset
puts on the system tables (see `state_joiner/README.md` for the full story of this sharp edge)
before the first deploy:

```bash
for t in $(yt find "$YT_DEV_ROOT/transform_high_throughput_go" --type table); do
    yt set "$t/@erasure_codec" none
    yt set "$t/@hunk_erasure_codec" none
    yt remount-table "$t"
done
```

Then deploy and, from a second terminal, feed and measure:

```bash
cd transform_high_throughput
SCENARIO_DIR="$PWD" python3 -c 'import os, string, sys; sys.stdout.write(string.Template(sys.stdin.read()).substitute(os.environ))' \
    < pipeline_go.yson.template > pipeline_go.yson
./companion_go/transform_high_throughput_go --config pipeline_go.yson \
    --flow-bin ~/ytsaurus/yt/yt/flow/bin/flow_server/flow_server.stripped
                                    # deploy + stream the controller log; Ctrl-C detaches

python3 transform_high_throughput/companion_go/feed.py --duration 600 --rate 8000   # keep it running…
python3 transform_high_throughput/companion_go/measure.py                           # …while this measures

./stop.sh transform_high_throughput_go   # stop the pipeline + abort the vanilla operation
```

### Observed output

Recorded from the live run on the demo cluster, `flow_server` built from ytsaurus commit
`1bdcb82f3ab` (heads/main), one worker, feed at 8,000 rows/s (feeder log: `fed 200000 rows, 8290
rows/s achieved`, zero "behind" warnings). The pipeline reached `working` in under a minute and
held it before, through, and after the window:

```
$ python3 transform_high_throughput/companion_go/measure.py 60
pipeline state: working
t0 sample: 2319532 rows, 286710278 cumulative bytes, input backlog 79999 rows; measuring for 60s ...
throughput: 8004 rows/s, 0.942 MB/s (+482543 rows in 60.3s, queue at 2802075 rows)
input backlog: 79999 rows at t0 -> 82130 rows at t1
ok: states table has 7741 Reducer keys
OK: sustained `working`, 8004 rows/s with a non-empty input backlog, states table has 7741 keys
```

The three-way comparison, same pipeline shape, same partition counts, same one worker (identical
~123 bytes/row all three ways):

| Variant | User code runs in | rows/s | MB/s | vs C++ |
|---|---|---|---|---|
| C++ (`pipeline/main.cpp`) | worker process, in-binary | 9,505 | 1.119 | 100% |
| **Go (`companion_go/main.go`)** | one Go process, gRPC companion | **8,004** | **0.942** | **84%** |
| Python (`companion_py/main.py`) | one CPython process, gRPC companion | 5,726 | 0.672 | 60% |

Two honesty notes on the Go figure:

- **The backlog held but did not grow much** (79,999 → 82,130 rows across the window): with a
  standing ~80K-row backlog always available to read, the pipeline processed at almost exactly the
  feed rate rather than draining the backlog — 8,004 rows/s is what the whole system sustained,
  with the input never the limiting factor inside the window.
- **Feeding faster collapses the cluster, not the pipeline.** Every attempt above 8K rows/s —
  12K single-feeder, 2×15K dual-feeder — was killed within seconds to minutes by
  `Node is out of tablet memory, all writes disabled` (code 1703) from the demo cluster's one
  active tablet node (`tnd-0` carries every Flow tablet and has a 5 GiB total memory limit;
  `tnd-1` idles at zero). The transform path writes each message several times (input store,
  output store, output queue, state), so the fed benchmark roughly doubles the tablet write load
  the self-generating C++ variant produced — the C++ 9,505 figure had no input queue to feed. On
  this cluster the Go companion's own ceiling is therefore *at least* 8K rows/s; a bigger tablet
  bundle would be needed to find where Go actually tops out. Recovery, when the node trips:
  wait out the write freeze, `yt trim-rows` the consumed input, and force a flush with
  `yt freeze-table` + `yt unfreeze-table` on the fat queues.

Parallelism: the whole transform ran as a **single Go process** — the Go runner ships the pipeline
binary itself as the one companion executable, and the SDK parses `companion_process_count` but
deliberately ignores it (no multi-process fan-out, unlike the Python SDK). Concurrency is
goroutines: the gRPC server serves both Reducer partitions' `ProcessBatch` calls concurrently, and
`GOMAXPROCS` is unrestricted in this job environment (no cgroup CPU quota, the same observation the
Python run recorded). So the 8,004 rows/s figure is one compiled process with in-process
concurrency against one interpreter for Python — which is the whole difference: 40% over Python
from the language runtime alone, 16% short of C++ mostly to the gRPC hop and YSON re-encoding on
the companion boundary.

The per-key state, filtered as the C++ check is (the same key neighbourhood around one million;
the Go SDK stores internal state as binary-YSON bytes, so like the Python variant it lands in the
`states` row as an opaque payload rather than the C++ variant's structured map — the readable
parts of the payload are the `count` and `last_data` map keys):

```
$ yt select-rows "computation_id, key, state from [...pipeline/states] where computation_id = 'Reducer' limit 2" --format json
{"computation_id":"Reducer","key":[798995128167919,"998816"],"state":{"payload":"{count=...;last_data=...psbco...;}"}}
{"computation_id":"Reducer","key":[3411560120071481,"999036"],"state":{"payload":"{count=...;last_data=...mmfqi...;}"}}

$ yt select-rows "sum(1) as cnt from [...pipeline/states] where computation_id = 'Reducer' group by 1" --format json
{"cnt":7800}
```
