# working_pipeline_telemetry

A two-computation pipeline whose subject is the *engine's* telemetry about a working pipeline:
`describe-pipeline` reporting a job failure with a recognizable comment, the flow view exposing
per-job buffer/epoch statistics, and `get-worker-backtraces` returning live stacks from a worker.

`reader` is `NYT::NFlow::NDemo::TReader` (`pipeline/main.cpp`), a `TSwiftOrderedSourceComputation`
over the built-in `NYT::NFlow::TRandomSource`. It forwards every message into the `data` stream —
except that it throws on messages whose key equals the spec-injected `fail_key`, tagging the error
with `fail_comment`. `processor` is `NYT::NFlow::NDemo::TProcessor`, a `TTransformComputation` that
consumes the stream and drops it, so the flow view has an inter-computation stream whose buffers
and stores it can report on.

The random source draws keys from `Poisson(message_key_range = 1000)`, so `fail_key = "1100"`
(+3.2σ) is hit roughly once per ten thousand messages: the pipeline is healthy most of the time and
fails a job every now and then — exactly the situation the telemetry is about. The failure is
transient by construction: the restarted job draws fresh random keys, so it never gets stuck on a
poison message.

**Not covered by the 0.2.1 artifact run.** This C++ variant needs a source build (its own
`flow_server`, since the stock binary does not link `connectors/random`) and is not verified
against the released artifacts on this repo's CI — the "Run" section below documents the
source-build procedure as-is, unverified against 0.2.1. The Java-companion variant further down
runs entirely on the released artifacts and is verified there.

## This scenario ships its own binary

The failure injection is user C++ code (a computation that throws on a spec-provided key), so the
stock `flow_server` cannot run it — and the stock binary does not link `connectors/random` anyway.
`pipeline/ya.make` is deliberately minimal: the runner, the random connector and the two
computations. `build.sh` stages the sources into your ytsaurus checkout, builds with `ya make` and
strips the result back here (see `secret_env/README.md` for why staging is needed).

## Run

From this dir, with `YTSAURUS` pointing at a checkout set up for `ya make` (see `build.sh`):

```bash
./build.sh                                   # builds + strips the binary into working_pipeline_telemetry_pipeline.stripped
python3 yt_sync.py                           # once: the pipeline node (no queues or tables)
jinjanate pipeline.yson.j2 > pipeline.yson   # every {{ VAR }} from the env; an unset one fails the render

./working_pipeline_telemetry_pipeline.stripped --config pipeline.yson
```

This scenario deploys its own binary instead of the released `flow_server`: the binary is both the
runner and the vanilla jobs' executable (it uploads itself). It streams the controller log;
Ctrl-C only detaches, the pipeline keeps running.

Then, from a second terminal, run the checks (each mirrors one upstream assert; the first three
minutes' patience is upstream's own `wait(...)` timeout):

```bash
python3 verify.py
```

What it checks, in order:

1. `describe-pipeline` → `computations/reader/messages` carries the injected failure's comment
   inside a job-failure message.
2. The flow view (`get_flow_view`) → `feedback/partition_job_statuses/<partition>/current_job_status`:
   - `epoch_part_times` sums positive on a reader job (epoch machinery is turning);
   - `input_limits/input_buffer_bytes` used > 0 on a processor job;
   - `output_limits/{output_buffer_bytes,output_store_bytes,output_store_count}` used > 0 on a
     reader job.
3. `describe-workers` lists the worker; `get-worker-backtraces` returns a non-empty stack dump
   for it.

`TRandomSource` is an unthrottled load generator, so stop the pipeline once verified. From the
repo root:

```bash
./stop.sh working_pipeline_telemetry
```

## Observed output

Recorded from an earlier run on the demo cluster (unverified against 0.2.1 — see the note above).
The pipeline reached `working` within seconds; `fail_key = "1100"` fired about every fifteen
seconds, and the controller log kept reporting it (guids and the `origin`/`datetime` attribute
block elided):

```
E	PublicFlowController	Job failed (JobId: …, PartitionId: …, ComputationId: reader)
Got fail key 1100. Comment: TELEMETRY_DEMO_INTENTIONAL_FAIL
```

`verify.py`, first run, no retries needed except the flow-view samples upstream also waits for:

```
$ python3 verify.py
    job-failure message: Job failed (JobFinishReason: Failed): Got fail key 1100. Comment: TELEMETRY_DEMO_INTENTIONAL_FAIL
ok: fail comment in a describe-pipeline reader job-failure message
ok: reader epoch_part_times in flow view
ok: processor input_buffer_bytes in flow view
ok: reader output_buffer_bytes in flow view
ok: reader output_store_bytes in flow view
ok: reader output_store_count in flow view
ok: describe-workers lists 1 worker(s)
ok: get-worker-backtraces returned 38702 bytes for [10.112.134.139]:10080
OK: failure comment reported, buffer/epoch telemetry exposed, worker backtraces work

$ yt flow get-pipeline-state "$YT_DEV_ROOT/working_pipeline_telemetry/pipeline"
working
```

A flow-view sample of the epoch telemetry the checks read (a processor job's
`epoch_part_times`, seconds per epoch part):

```
{'Accounting': 0.0024, 'Commit': 0.1466, 'FinalizeTransaction': 0.0002, 'GenerateGlobalUniqueSeqNo': 0.0656, ...}
```

Note that a single flow-view sample is a snapshot: a buffer's `used` is often zero at any given
instant, and the reader's `current_job_status` disappears for a few seconds after each injected
failure while the job restarts — which is why `verify.py` (like the upstream test) polls each
condition rather than asserting on one sample.

## Java companion variant

`companion_java/` re-runs the scenario with the failure-injecting user code written in **Java**
(`tech.ytsaurus:flow-*`, the Flow Java SDK), hosted by the stock `flow_server` — no custom
binary. `TelemetryMain` registers both computations: `FailingRead` on the swift-source path (a
`SourceComputation` behind `TSwiftOrderedSourceCompanionComputation`, as in the
`word_count_sync` Java variant) forwards each input row and fails on the spec-injected keys;
`SleepyDrop` (`TTransformCompanionComputation`) consumes the stream, sleeping
`sleep_per_message_ms` per message. The telemetry subject and every assert are unchanged:
`companion_java/verify.py` is the reference `verify.py` with only the pipeline path switched to
this variant's own root `$YT_DEV_ROOT/working_pipeline_telemetry_java`.

The plumbing is the other `companion_java` variants': one entry point for the runner and the
companion (`FlowApplication.run` picks the role from `YT_FLOW_MODE`), the pipeline jar and its
runtime deps synced into `lib/` (gitignored) — the classpath both the launch command and the Flow
runner's companion-jar shipping use — `TJavaCompanionManager` naming only `main_class` (and this
scenario's `backoff` block, which the manager's base config carries), `port_count = 3`, the
release's own `flow-java` docker image on both vanilla tasks — the `flow` image plus a JRE at
`/opt/java/openjdk`, so one image holds both the `flow_server` the jobs run and the `java` the
launcher and the companion run with — and `abort_on_specs_parseability_error = %false` (startup
logs the usual single `E SimpleRunner … Static spec has unrecognized fields` naming exactly the
user parameters — logged unconditionally, refuses nothing, and the parameters do reach the
companion, as every injected failure proves).

The adaptation is the one proven by the Python- and Go-companion variants, unweakened: the input
is a queue fed by `companion_java/feed.py` (~800 rows/s of keys from `range(1000)`, plus one
`fail_key` row with a unique `data` value every `--fail-every` seconds), the failure heals after
exactly `fail_attempts = 8` process-local attempts per unique fail row (the companion JVM is per
worker and survives job restarts) against the CompanionManager's
`backoff = {invocation_count = 5}` retry budget, and the processor's 2 ms sleep makes its input
buffer visibly hold data.

### What a Java failure looks like in `describe-pipeline` — both shapes

This port's novel question: Java user code can fail two ways — throwing an **`Exception`** and
throwing an **`Error`** — and the spec injects both (`fail_key = "1100"` throws a
`RuntimeException`, `error_key = "1101"` throws an `AssertionError`). On the released SDK
(`0.2.1-SNAPSHOT`) the companion server treats them **identically**: it rejects the call with a
gRPC `INTERNAL` status whose description names the computation and the exception class ahead of
the user message. Observed verbatim on the live run:

```
Job failed (JobFinishReason: Failed): Error processing batch (ComputationId: reader): java.lang.RuntimeException: Got fail key 1100. Comment: TELEMETRY_DEMO_INTENTIONAL_FAIL
Job failed (JobFinishReason: Failed): Error processing batch (ComputationId: reader): java.lang.AssertionError: Got error key 1101. Comment: TELEMETRY_DEMO_INTENTIONAL_FAIL
```

(and, while the retry budget lasts, the same texts behind
`Received job retryable error (Component: /operations/DoProcess, …)` — either message satisfies
the first assert.) Consequences, compared to the C++ variant's
`Job failed (JobFinishReason: Failed): Got fail key 1100. Comment: …`, the Python variant's
`… Error processing batch: Got fail key 1100. Comment: …` and the Go variant's
`… flow: process batch failed: computation "reader": OnMessage on input "<id>": …`:

- **The message carries the computation, the exception class and the user message.** That is
  Go's shape minus the failing input's message id, and more than Python's — an exception with a
  null message still shows its class. The cause chain and the stack trace are dropped; the
  describe message carries one flat error — code 1, `status_code: 13`, with the failing gRPC
  call's attributes (`method: ProcessBatch`, `service: …CompanionService`). The stack trace
  stays in the worker job's stderr.
- **An `Error` is reported exactly like an `Exception`.** The server catches it, so a failing
  assertion is as diagnosable as a thrown exception — same description shape, same attributes.
- **Both shapes are retried identically and heal identically.** The worker retries either
  status; the injected `Error` row healed as designed — the controller log shows the same
  `AssertionError` text for the attempts of one injected row and then silence, the companion
  JVM surviving its own escaped `Error`s.

The injection logic is proven offline first: `TelemetryTest` drives both computations through
the SDK's `TestComputationHarness` (`flow-test-utils`) — passthrough, the bounded
raise-then-pass behaviour of both failure shapes (the harness propagates the raw
`RuntimeException` and `AssertionError`, messages intact), per-row budget isolation, and the
processor's drop — no cluster needed.

### Build

Built with the official Gradle toolchain container, never a local or Arcadia-built JDK/Gradle,
from `companion_java`:

```bash
cd companion_java
podman run --rm -v "$PWD:/src" -w /src docker.io/library/gradle:8-jdk17 \
    gradle -q --refresh-dependencies test installLib -PflowVersion=0.2.1-SNAPSHOT
```

This resolves the SDK from the Sonatype snapshot repository (`build.gradle.kts`), runs the offline
tests (`TelemetryTest`, above), and syncs the pipeline jar plus its runtime deps into `lib/`
(gitignored) — the classpath both the launch command and the Flow runner's companion-jar shipping
use.

### Run

From `companion_java`:

```bash
python3 yt_sync.py   # once: pipeline node, input_queue + consumer, under working_pipeline_telemetry_java/
jinjanate pipeline.yson.j2 > pipeline.yson   # every {{ VAR }} from the env; an unset one fails the render
```

On a cluster with fewer than six online data nodes, check the system tables after `yt_sync.py`
and clear any `erasure_codec` / `hunk_erasure_codec` that is not `none` before the first deploy
(see `state_joiner/README.md` for the full story). A current `yt_sync_mini` already bootstraps
them without erasure.

Then, from a second terminal, feed and verify while the launch command below runs in the first:

```bash
podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" \
    ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1 -cp 'lib/*' \
    tech.ytsaurus.flow.demo.telemetry.TelemetryMain --config pipeline.yson
```

```bash
python3 feed.py --duration 900   # keep it running…
python3 verify.py                # …while this checks

# The Error-shaped failure, once, on demand:
echo '{"key": "1101", "data": "error-manual-0001", "$$tablet_index": 0}' \
    | yt insert-rows --format json "$YT_DEV_ROOT/working_pipeline_telemetry_java/input_queue"
```

The launcher enriches the spec (companion classpath, java binary), uploads the released
`flow_server` and launches the controller+worker vanilla operation, then streams the controller
log; it stays attached since this variant's `reader` is infinite (`Ctrl-C` detaches). From the
repo root: `./stop.sh working_pipeline_telemetry_java` stops the pipeline and aborts the vanilla
operation.

### Observed output

Recorded from the live run on the demo cluster, `flow-test/0.2.1` (commit `6f22b545...`) end to
end — the SDK resolved from the Sonatype snapshot repository, `flow_server` and both vanilla
tasks' `docker_image` confirmed as `ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1` from
`vanilla/current_spec`, the runner's own log carrying
`FlowCoreVersion CommitHash: 6f22b54593c142c07b92fe542a489db1a8f5b233, Tag: flow-test/0.2.1`. One
worker, feed at ~800 rows/s (`feed.py --duration 900`, ~131k rows and 4 fail rows fed over the
session). The pipeline reached `working` within seconds of launch; `verify.py`, first pass, no
check needed a retry beyond the flow-view samples upstream also waits for:

```
$ python3 verify.py
    job-failure message: Job failed (JobFinishReason: Failed): Error processing batch (ComputationId: reader): java.lang.RuntimeException: Got fail key 1100. Comment: TELEMETRY_DEMO_INTENTIONAL_FAIL
ok: fail comment in a describe-pipeline reader job-failure message
ok: reader epoch_part_times in flow view
ok: processor input_buffer_bytes in flow view
ok: reader output_buffer_bytes in flow view
ok: reader output_store_bytes in flow view
ok: reader output_store_count in flow view
ok: describe-workers lists 1 worker(s)
ok: get-worker-backtraces returned 43030 bytes for [10.112.146.167]:24580
OK: failure comment reported, buffer/epoch telemetry exposed, worker backtraces work

$ yt flow get-pipeline-state "$YT_DEV_ROOT/working_pipeline_telemetry_java/pipeline"
working
```

The manual error row was injected while the run was going and healed the same way: seven
`java.lang.AssertionError: Got error key 1101. Comment: TELEMETRY_DEMO_INTENTIONAL_FAIL` reports
(the controller log's `Received job retryable error` / `Job failed` lines), then silence, with the
pipeline never leaving `working` — the scenario's terminal state, since its source is infinite. A
second `verify.py` pass afterwards passed all eight checks again (`get-worker-backtraces` returned
46314 bytes that time). A flow-view sample of the epoch telemetry the checks read (a processor
job's `epoch_part_times`, seconds per epoch part):

```
{'Accounting': 0.024274, 'Commit': 0.253617, 'Distribute.Start': 0.0, 'FinalizeTransaction': 0.003152, 'GenerateGlobalUniqueSeqNo': 0.492256, ...}
```
