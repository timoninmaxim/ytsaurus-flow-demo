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

## Deviation from the upstream test, deliberate

The upstream test (`tests/working_pipeline_telemetry`) uses `fail_key = "42"` — which
`TRandomSource` can never produce: keys are Poisson samples around `message_key_range`, and the
test's source knobs are additionally misplaced in its dynamic spec (not nested under
`parameters`), so the source runs on defaults (λ = 1024). The injected failure therefore never
fires upstream; its assert `FAIL_COMMENT in str(messages)` passes anyway because
`describe-pipeline` emits a per-computation `Spec` info message that echoes the static spec —
`fail_comment` included.

This scenario keeps the upstream code and spec shape but picks a *reachable* fail key and nests the
source parameters correctly, so the failure genuinely fires; `verify.py` then requires the comment
inside an actual job-failure message, excluding the `Spec` echo. That is strictly stronger than the
upstream assert while observing the same engine machinery.

## This scenario ships its own binary

The failure injection is user C++ code (a computation that throws on a spec-provided key), so the
stock `flow_server` cannot run it — and the stock binary does not link `connectors/random` anyway.
`pipeline/ya.make` is deliberately minimal: the runner, the random connector and the two
computations. `build.sh` stages the sources into your ytsaurus checkout, builds with `ya make` and
strips the result back here (see `secret_env/README.md` for why staging is needed).

## Run

From the repo root:

```bash
working_pipeline_telemetry/build.sh             # builds + strips the binary (YTSAURUS=<checkout>)
python3 working_pipeline_telemetry/yt_sync.py   # once: the pipeline node (no queues or tables)
FLOW_BIN=working_pipeline_telemetry/working_pipeline_telemetry_pipeline.stripped \
    ./run.sh working_pipeline_telemetry         # deploy + stream the controller log; Ctrl-C detaches
```

Then, from a second terminal, run the checks (each mirrors one upstream assert; the first three
minutes' patience is upstream's own `wait(...)` timeout):

```bash
python3 working_pipeline_telemetry/verify.py
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

`TRandomSource` is an unthrottled load generator, so stop the pipeline once verified:

```bash
./stop.sh working_pipeline_telemetry
```

## Observed output

Recorded from the live run on the demo cluster. The pipeline reached `working` within seconds;
`fail_key = "1100"` fired about every fifteen seconds, and the controller log `run.sh` streams kept
reporting it (guids and the `origin`/`datetime` attribute block elided):

```
E	PublicFlowController	Job failed (JobId: …, PartitionId: …, ComputationId: reader)
Got fail key 1100. Comment: TELEMETRY_DEMO_INTENTIONAL_FAIL
```

`verify.py`, first run, no retries needed except the flow-view samples upstream also waits for:

```
$ python3 working_pipeline_telemetry/verify.py
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

The plumbing is the other `companion_java` variants', unchanged: one entry point for the runner
and the companion (`FlowApplication.run` picks the role from `YT_FLOW_MODE`), the composite
Gradle build substituting the unpublished SDK with a sibling ytsaurus checkout, the
`collectRuntime` jar directory the runner ships from `java.library.path`,
`TJavaCompanionManager` with `main_class` (and this scenario's `backoff` block, which the
manager's base config carries), `port_count = 3`, the `eclipse-temurin:17-jre` docker image plus
the `YT_FLOW_JDK_LAYERS='[]'` / `YT_FLOW_JDK_BIN_PATH=/opt/java/openjdk/bin/java` overrides in
`run.sh`, and `abort_on_specs_parseability_error = %false` (startup logs the usual single
`E SimpleRunner … Static spec has unrecognized fields` naming exactly the user parameters —
logged unconditionally, refuses nothing, and the parameters do reach the companion, as every
injected failure proves).

The adaptation is the one proven by the Python- and Go-companion variants, unweakened: the input
is a queue fed by `companion_java/feed.py` (~800 rows/s of keys from `range(1000)`, plus one
`fail_key` row with a unique `data` value every `--fail-every` seconds), the failure heals after
exactly `fail_attempts = 8` process-local attempts per unique fail row (the companion JVM is per
worker and survives job restarts) against the CompanionManager's
`backoff = {invocation_count = 5}` retry budget, and the processor's 2 ms sleep makes its input
buffer visibly hold data.

### What a Java failure looks like in `describe-pipeline` — both shapes

This port's novel question: Java user code can fail two ways that the SDK server treats very
differently — throwing an **`Exception`** and throwing an **`Error`** — and the spec injects
both (`fail_key = "1100"` throws a `RuntimeException`, `error_key = "1101"` throws an
`AssertionError`). The SDK server's ProcessBatch handler catches `Exception` only
(`CompanionService.processBatch`'s `catch (Exception e)`) and rejects the call with a flat
gRPC `INTERNAL` status whose description is `"Error processing batch: " + e.getMessage()`.
Observed verbatim on the live run:

```
Job failed (JobFinishReason: Failed): Error processing batch: Got fail key 1100. Comment: TELEMETRY_DEMO_INTENTIONAL_FAIL
Job failed (JobFinishReason: Failed): Application error processing RPC
```

(and, while the retry budget lasts, the same texts behind
`Retryable error in component "/operations/DoProcess": ` — either message satisfies the first
assert for the exception shape.) Consequences, compared to the C++ variant's
`Job failed (JobFinishReason: Failed): Got fail key 1100. Comment: …`, the Python variant's
`… Error processing batch: Got fail key 1100. Comment: …` and the Go variant's
`… flow: process batch failed: computation "reader": OnMessage on input "<id>": …`:

- **The exception shape preserves the user message and nothing else.** The description is
  the outermost exception's `getMessage()` behind the same `Error processing batch: ` wrapper
  text Python uses — but the exception **class name, cause chain and stack trace are all
  dropped** (Go at least prefixes the computation and the failing input's message id; an
  exception with a null message would surface as `Error processing batch: null`). The describe
  message carries one flat error — code 1, `status_code: 13`, with the failing gRPC call's
  attributes (`method: ProcessBatch`, `service: …CompanionService`).
- **The `Error` shape loses the user text entirely.** `catch (Exception)` does not see an
  `AssertionError`: it escapes into grpc-java, which closes the call as `UNKNOWN`
  (`status_code: 2`) with the generic description `Application error processing RPC` — the
  message, class and comment never reach `describe-pipeline` or the controller log. The only
  trace is in the worker job's stderr, printed by the JVM's default handler as the executor
  thread dies (grpc replaces it; the companion survives):

  ```
  Exception in thread "grpc-default-executor-21" java.lang.AssertionError: Got error key 1101. Comment: TELEMETRY_DEMO_INTENTIONAL_FAIL
      at tech.ytsaurus.flow.demo.telemetry.FailingRead.onMessage(FailingRead.java:61)
      at tech.ytsaurus.flow.computation.Computation.doProcessMessages(Computation.java:161)
      ...
  ```

- **Both shapes are retried identically and heal identically.** The worker retries either
  status; the injected `Error` row healed exactly as designed — the stderr shows attempts up to
  precisely 8 for one injected error row (six in the failed job, two in its restarted
  successor), then silence, with the same companion pid throughout: the JVM survives its own
  escaped `Error`s.

The injection logic is proven offline first: `TelemetryTest` drives both computations through
the SDK's `TestComputationHarness` (`flow-test-utils`) — passthrough, the bounded
raise-then-pass behaviour of both failure shapes (the harness propagates the raw
`RuntimeException` and `AssertionError`, messages intact), per-row budget isolation, and the
processor's drop — no cluster needed.

### Run

From the repo root (the sibling `~/ytsaurus` checkout provides the SDK through the composite
build in `settings.gradle.kts`):

```bash
working_pipeline_telemetry/companion_java/build.sh   # gradle test + collectRuntime (JDK 17+)
python3 working_pipeline_telemetry/companion_java/yt_sync.py  # once: pipeline node, input_queue + consumer
```

On a cluster with fewer than six online data nodes, clear the erasure codec the pipeline preset
puts on the system tables before the first deploy (see `state_joiner/README.md` for the full
story; the empty-table `unmounting` hang applies — `--force` the tablets still `transient`
after ~20 s).

Then deploy and, from a second terminal, feed and verify:

```bash
working_pipeline_telemetry/companion_java/run.sh    # deploy + stream the runner log; Ctrl-C detaches

python3 working_pipeline_telemetry/companion_java/feed.py --duration 900   # keep it running…
python3 working_pipeline_telemetry/companion_java/verify.py                # …while this checks

# The Error-shaped failure, once, on demand:
echo '{"key": "1101", "data": "error-manual-0001", "$$tablet_index": 0}' \
    | yt insert-rows --format json "$YT_DEV_ROOT/working_pipeline_telemetry_java/input_queue"

./stop.sh working_pipeline_telemetry_java   # stop the pipeline + abort the vanilla operation
```

### Observed output

Recorded from the live run on the demo cluster, `flow_server` built from ytsaurus commit
`baaaeedbe3c` (heads/main), one worker, feed at 800 rows/s with a fail row every 45 s. The
pipeline reached `working` ~40 s after launch; `verify.py`, complete first-pass run, no check
needed a retry beyond the flow-view samples upstream also waits for:

```
$ python3 working_pipeline_telemetry/companion_java/verify.py
    job-failure message: Retryable error in component "/operations/DoProcess": Error processing batch: Got fail key 1100. Comment: TELEMETRY_DEMO_INTENTIONAL_FAIL
ok: fail comment in a describe-pipeline reader job-failure message
ok: reader epoch_part_times in flow view
ok: processor input_buffer_bytes in flow view
ok: reader output_buffer_bytes in flow view
ok: reader output_store_bytes in flow view
ok: reader output_store_count in flow view
ok: describe-workers lists 1 worker(s)
ok: get-worker-backtraces returned 49810 bytes for [10.112.153.196]:24580
OK: failure comment reported, buffer/epoch telemetry exposed, worker backtraces work

$ yt flow get-pipeline-state "$YT_DEV_ROOT/working_pipeline_telemetry_java/pipeline"
working
```

A second pass after two manual error rows found the `Application error processing RPC` job
failure quoted above and passed all checks again; the feeder delivered ~449k rows (13 fail
rows) over its 900 s. A flow-view sample of the epoch telemetry the checks read (a processor
job's `epoch_part_times`, seconds per epoch part):

```
{'Accounting': 0.0315, 'Commit': 0.5615, 'Distribute.Start': 0.0002, 'FinalizeTransaction': 0.0084, 'GenerateGlobalUniqueSeqNo': 0.5737, ...}
```
