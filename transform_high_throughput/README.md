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

**Not covered by the 0.2.1 artifact run.** This C++ variant needs a source build (its own binary,
see below) and is not verified against the released artifacts on this repo's CI — the "Run" section
below documents the source-build procedure as-is, unverified against 0.2.1. The Java-companion
variant further down runs entirely on the released artifacts and is verified there.

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

From this dir, with your env file sourced:

```bash
./build.sh                        # builds + strips the binary (YTSAURUS=<checkout>)
python3 yt_sync.py                # once: pipeline node, output queue (2 tablets), producer
jinjanate pipeline.yson.j2 > pipeline.yson   # every {{ VAR }} from the env; an unset one fails the render

# This scenario deploys its own binary instead of the released flow_server: render the spec
# and run that binary directly (it uploads itself to the vanilla jobs).
./transform_high_throughput_pipeline.stripped --config pipeline.yson
```

The launch streams the controller log; it keeps running until stopped. Then, from a second
terminal, measure (the script waits out a 60-second window by default; `python3 measure.py 120`
for a longer one):

```bash
python3 measure.py
```

What it does, in order:

1. `get-pipeline-state` must be `working` before and after the window — the pipeline sustains the
   load, not merely survives the sample.
2. Samples the output queue twice, `window` seconds apart: row count via `sum(1)`, bytes via the
   queue's `$cumulative_data_weight` system column (summed over tablets) — and prints **rows/s**
   and **MB/s** of the delta.
3. `states` must be non-empty; prints the row (distinct key) count.

The random source is an unthrottled load generator, so stop the pipeline once measured. From the
repo root:

```bash
./stop.sh transform_high_throughput
```

## Knobs

All in `pipeline.yson.j2`, all in the dynamic spec (changeable on a running pipeline with
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
$ python3 measure.py 60
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
*persisted* state therefore remains `working`, and a later re-launch of the stripped binary against
the same scenario resumes it. When the controller does answer, `stop.sh` performs the graceful
`stop-pipeline` first.

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
(enriches the spec, ships `lib/*` as the companion's classpath, fills stream schemas and companion
resources, execs the image's own `flow_server`) and the companion server inside the worker job.
`TJavaCompanionManager` names only `main_class` — no `jdk_bin_path`, no `classpath`: the launcher
ships the classpath jars and takes the job java from its own `java.home`. Both vanilla tasks
(`controller` and `worker`) run in `ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1` (this cluster has
no porto layers, so a docker image carrying the JDK is required), and `port_count = 3` (worker RPC +
monitoring + the companion gRPC port).

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
  `-PflowVersion` (default `0.2.1-SNAPSHOT`). The vanilla tasks and the launcher itself run in
  `ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1` (the released server image plus a JRE at
  `/opt/java/openjdk`), so one image holds both the `flow_server` the jobs run and the `java` the
  launcher and the companion run with.
- **The released state API is nullable, not `Optional`-valued.** `StateAccessor.get()` returns
  the state or `null`, so the reducer opens its state with
  `accessor.getOrDefault(new ReducerState())`. `ReducerTest`, which drives `Computation.doProcess`
  directly, also had to follow the internals: the state value type is `State` (constructed from a
  `ByteString`, read with `getBytes()`), `StatesHolder` is no longer generic, and modified states
  are collected with `collectModifiedStates()`.

### Build

Built with the official Gradle toolchain container, never a local or Arcadia-built JDK/Gradle,
from `companion_java`:

```bash
cd companion_java
podman run --rm -v "$PWD:/src" -w /src docker.io/library/gradle:8-jdk17 \
    gradle -q --refresh-dependencies test installLib -PflowVersion=0.2.1-SNAPSHOT
```

This resolves the SDK from the Sonatype snapshot repository (`build.gradle.kts`), runs the offline
tests (`ReducerTest`, above), and syncs the pipeline jar plus its runtime deps into `lib/`
(gitignored) — the classpath both the launch command and the Flow runner's companion-jar shipping
use.

### Run

From `companion_java`, with your env file sourced:

```bash
python3 yt_sync.py   # once: pipeline node, queues, consumer, producer, under transform_high_throughput_java/
```

On this demo cluster, check the pipeline system tables after `yt_sync.py` and, if any carries an
`@erasure_codec` / `@hunk_erasure_codec` other than `none`, run the erasure-codec workaround (see
`word_count_sync/README.md`): clear both and remount; empty tablets stuck `transient` after ~60 s
need `yt unmount-table --force` + `yt mount-table`. A current `yt_sync_mini` bootstraps them
without erasure, so on the run below there was nothing to do.

```bash
jinjanate pipeline.yson.j2 > pipeline.yson   # every {{ VAR }} from the env; an unset one fails the render

podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" \
    ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1 -cp 'lib/*' \
    tech.ytsaurus.flow.demo.transformhighthroughput.TransformHighThroughputMain --config pipeline.yson
```

The launcher enriches the spec (companion classpath), uploads the released `flow_server` and
launches the controller+worker vanilla operation, then streams the controller log; Ctrl-C only
detaches, the pipeline keeps running. From a second terminal:

```bash
python3 feed.py --duration 600 --rate 8000   # keep it running…
python3 measure.py                           # …while this measures
```

From the repo root: `./stop.sh transform_high_throughput_java` stops the pipeline and aborts the
vanilla operation.

### Observed output

Recorded from the live run on the demo cluster end to end on the released 0.2.1 artifacts — the SDK
from the Sonatype snapshot repository, `flow_server` and the job image out of
`ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1` — one worker. The launcher uploaded the released
`flow_server` and the vanilla operation (`4c0c03de-507cea60-103e8-447b0e05`) reached `working` about
32 seconds after launch (the usual `GetCompanionInfo` connection-refused retries while the companion
binds its port cleared on their own, no `Job failed`); `vanilla/current_spec` confirms both
`controller` and `worker` ran `docker_image = ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1`, and the
controller log's `FlowCoreVersion` reports `CommitHash: 6f22b54593c142c0…`, `Tag: flow-test/0.2.1`
— the released binary.

One 60-second window, measured while `feed.py --duration 100 --rate 6000` (~6058 rows/s achieved)
kept a backlog in front of the pipeline:

```
$ python3 measure.py 60
pipeline state: working
t0 sample: 73965 rows, 9134632 cumulative bytes, input backlog 98994 rows; measuring for 60s ...
throughput: 6972 rows/s, 0.821 MB/s (+418948 rows in 60.1s, queue at 492913 rows)
input backlog: 98994 rows at t0 -> 69089 rows at t1
ok: states table has 6819 Reducer keys
OK: sustained `working`, 6972 rows/s with a non-empty input backlog, states table has 6819 keys
```

The backlog shrank inside the window, so ~7000 rows/s here is a *feed*-set lower bound (one
feeder process at `--rate 6000`, well below the ~19K rows/s two feeders drove on an earlier
0.1.0-SNAPSHOT run of this same variant), not the pipeline's ceiling.

The exactly-once ledger, after the feed stopped and the backlog drained to zero — input rows ever
written, consumer offsets committed, and output rows agree to the row:

```
written=600000 consumed=600000 output=600000 backlog=0
```

The per-key state, filtered as the C++ check is (`6927` keys by the end of the run, the same key
neighbourhood around one million; the `@Entity` codec keeps the C++ field names inside the
binary-YSON payload):

```
$ yt select-rows "computation_id, key, state from [...pipeline/states] where computation_id = 'Reducer' limit 1" --format json
{"computation_id":"Reducer","key":[798995128167919,"998816"],"state":{"payload":"{count=480;last_data=xmhniangkipdjvfmiwgrpqaeupjhahdhvkstzrdenjszkfbovulgpurbyzcktzkekqtwnpxabzlakdiakrjjikndikvdjkiizboq}"}}
```

For shape, the earlier checkout-build comparison across languages (same pipeline shape, same
partition counts, one worker, ~123 bytes/row): C++ (in-binary) ~9,500 rows/s, a companion-hosted Go
reducer ~8,000 rows/s, Python ~5,700 rows/s, and Java on 0.1.0-SNAPSHOT ~19,000–19,600 rows/s
against a two-feeder backlog. Each figure is a lower bound set by a different limiter (the C++
figure is the self-generating random source's pull rate; the others were feed-set), not a
controlled shoot-out — this repo ships only the C++ and Java variants of this scenario, so the
Go/Python numbers are carried over from that other checkout for context, not reproducible here.
What the Java figures across both releases do establish: the companion gRPC hop plus JVM YSON
re-encoding is not the transform path's bottleneck at several times the rate the feed alone could
sustain on this cluster.

Tablet-memory practicalities of running the fed benchmark on this cluster, in the order they bit
across runs: the single active tablet node sits within ~300 MB of its 5 GiB memory limit at rest
(a block cache pinned by earlier work holds the floor), so a sustained feed can meet a write
freeze (1703) sooner or later — `feed.py` retries on it rather than dying. `yt freeze-table` +
`unfreeze-table` on the fat queues and system tables reclaims the dynamic-store part (freeze is
async — poll `@tablet_state`, and empty tablets can wedge in `freezing`/`transient`, fixed by
`yt unmount-table --force` + mount). No freeze was hit on this run. A backlog can also be
pre-filled with the pipeline paused (`yt flow pause-pipeline`, feed in flushed bursts,
`start-pipeline`).
