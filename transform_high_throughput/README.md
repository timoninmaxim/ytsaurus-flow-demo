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

## Java companion variant

`companion_java/` re-runs the benchmark with the per-key-state reducer written in **Java**
(`tech.ytsaurus:flow-*`), hosted by the stock `flow_server` — no custom binary. The pipeline
shape is unchanged: a two-partition native reader feeding a two-partition transform
(`farm_hash(key)` grouping, per-key internal state `"state"` persisted into the built-in `states`
table) whose output the async queue sink writes to a two-tablet queue, on one worker.
`Reducer.java` implements the SDK's `BatchFunction` (the request's whole batch arrives with keys
mixed, so it groups by key in first-appearance order, exactly like the Go and Python variants) and
opens the state through `StateDescriptors.yson` — the `@Entity` POJO lands in the `states` row as
a binary-YSON payload with the C++ variant's field names (`count`, `last_data`). The reader stays
native, so the Java code sits exactly where the C++ user code sat: on the transform path. The
entry point is one class for both roles, as in the other `companion_java` variants: the runner
(enriches the spec, ships the jars collected into `build/companion-libs`, completes the
`TJavaCompanionManager` classpath, execs `flow_server`) and the companion server inside the
worker job.

The adaptations proven by the Python and Go variants carry over unweakened: the input is a queue
fed from the dev host (`companion_java/feed.py`, same distributions as `TRandomSource` under
`message_key_range = 1000000`), and `companion_java/measure.py` is the reference method plus the
fed-input honesty checks. One hardening this run forced: `feed.py` now retries inserts on error
1703 (`Node is out of tablet memory, all writes disabled`) instead of dying — on this cluster's
single active tablet node a sustained feed *will* meet a write freeze sooner or later.

Two more things this variant pins, both about where the code comes from:

- **The SDK and the server come from a Flow release, not from a source checkout.** The Gradle
  build resolves `tech.ytsaurus:flow-*` from Maven: released versions from Maven Central, test
  releases (`X.Y.Z-SNAPSHOT`) from the Sonatype snapshot repository; the version is
  `-PflowVersion` (default `0.1.0-SNAPSHOT`). The `flow_server` the runner ships is taken out of
  the release image of the same version and passed in `FLOW_BIN`; the vanilla jobs run in
  `ghcr.io/ytsaurus/flow-java:<version>` — the `flow` image plus a JRE at `/opt/java/openjdk`, so
  the same image carries both the server and the `java` the companion is launched with, and the
  resource's `jdk_bin_path` points inside it. `FLOW_IMAGE` names it.
- **The released state API is nullable, not `Optional`-valued.** `StateAccessor.get()` returns
  the state or `null`, so the reducer opens its state with
  `accessor.getOrDefault(new ReducerState())`. `ReducerTest`, which drives `Computation.doProcess`
  directly, also had to follow the internals: the state value type is `State` (constructed from a
  `ByteString`, read with `getBytes()`), `StatesHolder` is no longer generic, and modified states
  are collected with `collectModifiedStates()`.

### Run

From the repo root, with your env file sourced (a JDK 17+ is required; the Gradle wrapper fetches
Gradle itself):

```bash
export FLOW_IMAGE=ghcr.io/ytsaurus/flow-java-nightly:dev-0.1.0   # a release is ghcr.io/ytsaurus/flow-java:<version>
docker create --name flow "$FLOW_IMAGE" && docker cp flow:/usr/bin/flow_server ~/flow_server && docker rm flow
export FLOW_BIN=~/flow_server

transform_high_throughput/companion_java/build.sh   # gradle test + collectRuntime (63 jars, SDK from Maven)

python3 transform_high_throughput/companion_java/yt_sync.py  # once: pipeline node, queues, consumer, producer
```

On this demo cluster, check the pipeline system tables after `yt_sync.py` and, if any carries an
`@erasure_codec` / `@hunk_erasure_codec` other than `none`, run the erasure-codec workaround (see
`word_count_sync/README.md`): clear both and remount; empty tablets stuck `transient` after ~60 s
need `yt unmount-table --force` + `yt mount-table`. A current `yt_sync_mini` bootstraps them
without erasure, so on the run below there was nothing to do.

```bash
transform_high_throughput/companion_java/run.sh     # deploy + stream the controller log; Ctrl-C detaches

python3 transform_high_throughput/companion_java/feed.py --duration 600 --rate 8000   # keep it running…
python3 transform_high_throughput/companion_java/measure.py                           # …while this measures

./stop.sh transform_high_throughput_java   # stop the pipeline + abort the vanilla operation
```

### Observed output

Recorded from the live run on the demo cluster on Flow release `0.1.0-SNAPSHOT` end to end — the
SDK from the Sonatype snapshot repository, `flow_server` and the job image out of
`ghcr.io/ytsaurus/flow-java-nightly:dev-0.1.0` — one worker. The pipeline reached `working` 24
seconds after the vanilla operation started and stayed there for the whole session, with no
tablet write freeze (1703) this time: the scenario's tablets were pinned to an idle tablet cell
first, so the feed never met the node the earlier runs kept filling up.

Two 60-second windows, measured while **two** feeder processes (`--rate 9000` each, ~18.0K
rows/s achieved) kept a 1.1-million-row input backlog in front of the pipeline:

```
$ python3 transform_high_throughput/companion_java/measure.py 60
pipeline state: working
t0 sample: 331998 rows, 41001948 cumulative bytes, input backlog 1223352 rows; measuring for 60s ...
throughput: 19606 rows/s, 2.313 MB/s (+1183000 rows in 60.3s, queue at 1514998 rows)
input backlog: 1223352 rows at t0 -> 1165746 rows at t1
ok: states table has 7428 Reducer keys
OK: sustained `working`, 19606 rows/s with a non-empty input backlog, states table has 7428 keys

$ python3 transform_high_throughput/companion_java/measure.py 60
throughput: 18967 rows/s, 2.236 MB/s (+1146222 rows in 60.4s, queue at 2952774 rows)
input backlog: 1145998 rows at t0 -> 1121573 rows at t1
ok: states table has 7798 Reducer keys
```

The backlog shrank slightly inside both windows, so ~19K rows/s is still a *feed*-set lower
bound and not the pipeline's ceiling — the pipeline drained 7.56 million rows faster than two
feeder processes could produce them.

The exactly-once ledger, after stopping the feed and letting the backlog drain to zero — input
rows ever written, consumer offsets committed, and output rows agree to the row:

```
written=7560000 consumed=7560000 output=7560000 backlog=0
```

The four-way comparison, same pipeline shape, same partition counts, same one worker (~123
bytes/row throughout):

| Variant | User code runs in | rows/s | MB/s | vs C++ |
|---|---|---|---|---|
| C++ (`pipeline/main.cpp`) | worker process, in-binary | 9,505 | 1.119 | 100% |
| **Java (`companion_java/`)** | one JVM, gRPC companion | **18,967–19,606** | **2.24–2.31** | **~200%** |
| Go (`companion_go/main.go`) | one Go process, gRPC companion | 8,004 | 0.942 | 84% |
| Python (`companion_py/main.py`) | one CPython process, gRPC companion | 5,726 | 0.672 | 60% |

Read the table with its capping conditions in mind — each figure is a lower bound set by a
different limiter, not a controlled shoot-out, and only the Java row was re-measured on the
release (the other three are the earlier checkout-build runs, kept for shape, not for a
version-to-version comparison). The C++ 9,505 is the self-generating random source's pull rate;
the Go 8,004 and Python 5,726 were measured against an 8K-rows/s feed on a day the tablet node
froze writes above that; the Java ~19K is what two feeder processes could deliver, with the
pipeline still ahead of them. What the Java figure does establish: the companion gRPC hop plus
JVM YSON re-encoding is **not** the transform path's bottleneck at twice the rate any earlier
variant was capped at.

The per-key state, filtered as the C++ check is (`8261` keys by the end of the run, the same
key neighbourhood around one million; the `@Entity` codec keeps the C++ field names inside the
binary-YSON payload):

```
$ yt select-rows "computation_id, key, state from [...pipeline/states] where computation_id = 'Reducer' limit 1" --format json
{"computation_id":"Reducer","key":[798995128167919,"998816"],"state":{"payload":"{count=1192;last_data=epbtjebxym...}"}}
```

Tablet-memory practicalities of running the fed benchmark on this cluster, in the order they
bit: the single active tablet node sits within ~300 MB of its 5 GiB memory limit at rest (a
~3.7 GB block cache pinned by earlier work holds the floor), so the first feed attempt froze all
writes (1703) after ~85 s; `yt freeze-table` + `unfreeze-table` on the fat queues and system
tables reclaims the dynamic-store part (freeze is async — poll `@tablet_state`, and empty
tablets can wedge in `freezing`/`transient`, fixed by `yt unmount-table --force` + mount); under
sustained pressure the node eventually evicted ~700 MB of cache on its own, after which the full
benchmark ran with zero freezes. A backlog can also be pre-filled with the pipeline paused
(`yt flow pause-pipeline`, feed in flushed bursts, `start-pipeline`) — that run drained
1,000,000 pre-filled rows to an exact output count before the live-feed windows above.
