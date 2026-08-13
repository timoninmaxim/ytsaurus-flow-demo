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

## Python companion variant

`companion_py/` re-runs the scenario with the failure-injecting user code written in **Python**,
hosted by the stock `flow_server` — no custom binary. `main.py` registers both computations with
the Flow Python companion SDK: `Read` on the swift-source path
(`TSwiftOrderedSourceCompanionComputation`) forwards each input row and raises on the
spec-injected `fail_key`, `Drop` (`TTransformCompanionComputation`) consumes the stream. The
telemetry subject and every assert are unchanged: `companion_py/verify.py` is the reference
`verify.py` with only the pipeline path switched to this variant's own root
`$YT_DEV_ROOT/working_pipeline_telemetry_py`. The companion delivery is the launcher + bundle
pair the other `companion_py` scenarios use (`entrypoint = ./py_companion`, two `local_files`,
worker `port_count = 3`).

Three deliberate adaptations against the C++ variant — the asserts are not weakened:

- **The input is a queue fed from the dev host, not `TRandomSource`.** The stock binary does not
  link the random connector, and the input transport is not the subject. `companion_py/feed.py`
  stands in for the generator: ~800 rows/s of random keys drawn from `range(1000)` (so an
  ordinary key can never equal `fail_key = "1100"`), plus one fail-key row with a unique `data`
  value every `--fail-every` seconds.
- **The injected failure must be transient by construction, and that takes two knobs.** With a
  queue, a row that always raises would be re-read forever — a companion exception is retried,
  first by the worker's gRPC retry loop, then by the restarted job — and would poison the
  pipeline (the C++ variant sidesteps this because its restarted job draws fresh random keys).
  So the raise repeats per unique fail row exactly `fail_attempts` times (a process-local count
  in the companion, which is per worker and survives job restarts) and then lets the row pass.
  The worker's retry budget is `invocation_count + 1` attempts, so the spec pairs
  `fail_attempts = 8` with `backoff = {invocation_count = 5}` on the CompanionManager resource:
  six raises exhaust the first budget — one genuine job failure fires —  and the restarted job's
  re-read spends the remaining two raises inside its own budget and passes. (With the default
  `invocation_count = 30` the same exception never fails the job at all: it stays a
  worker-level retryable error for up to ~4 minutes per firing.)
- **The processor sleeps 2 ms per message** (`sleep_per_message_ms`), so it consumes slightly
  slower than the feed and its input buffer visibly holds data — the C++ variant got the same
  effect for free from an unthrottled in-process generator.

What the failure looks like from the engine's side, in the order it unfolds: while the retry
budget lasts, `describe-pipeline` shows a **retryable-error** message on the reader; once the
budget is exhausted the job genuinely fails and the message the first assert requires appears
(and persists) — the Python exception text intact behind the gRPC prefix, with the failing
gRPC call's attributes (method `ProcessBatch`, service `CompanionService`) in the error chain:

```
Retryable error in component "/operations/DoProcess": Error processing batch: Got fail key 1100. Comment: TELEMETRY_DEMO_INTENTIONAL_FAIL
Job failed (JobFinishReason: Failed): Error processing batch: Got fail key 1100. Comment: TELEMETRY_DEMO_INTENTIONAL_FAIL
```

The C++ variant's counterpart is `Job failed (JobFinishReason: Failed): Got fail key 1100.
Comment: …` — the only difference is the SDK's `Error processing batch: ` prefix.

### Run

From the repo root:

```bash
working_pipeline_telemetry/companion_py/build.sh        # companion_bundle.tgz: CPython + SDK + main.py
python3 working_pipeline_telemetry/companion_py/yt_sync.py   # once: pipeline node, input_queue + consumer
```

On a cluster with fewer than six online data nodes, clear the erasure codec the pipeline preset
puts on the system tables (see `state_joiner/README.md` for the full story of this sharp edge)
before the first deploy:

```bash
for t in $(yt find "$YT_DEV_ROOT/working_pipeline_telemetry_py" --type table); do
    yt set "$t/@erasure_codec" none
    yt set "$t/@hunk_erasure_codec" none
    yt remount-table "$t"
done
```

Then deploy and, from a second terminal, feed and verify:

```bash
FLOW_BIN=~/ytsaurus/yt/yt/flow/bin/flow_server/flow_server.stripped \
    ./run.sh working_pipeline_telemetry py    # deploy + stream the controller log; Ctrl-C detaches

python3 working_pipeline_telemetry/companion_py/feed.py --duration 540   # keep it running…
python3 working_pipeline_telemetry/companion_py/verify.py                # …while this checks

./stop.sh working_pipeline_telemetry_py       # stop the pipeline + abort the vanilla operation
```

### Observed output

Recorded from the live run on the demo cluster, server build `26.2.0-local-os~1bdcb82f3ab63fcb`,
one worker, feed at 800 rows/s with a fail row every 45 s. `verify.py`, complete first-pass run
against a pipeline in steady state (the retryable-error message of an in-flight firing satisfies
the first check too, as observed on an earlier run — both are genuine engine reports about the
failure, not the `Spec` echo the check excludes):

```
$ python3 working_pipeline_telemetry/companion_py/verify.py
    job-failure message: Job failed (JobFinishReason: Failed): Error processing batch: Got fail key 1100. Comment: TELEMETRY_DEMO_INTENTIONAL_FAIL
ok: fail comment in a describe-pipeline reader job-failure message
ok: reader epoch_part_times in flow view
ok: processor input_buffer_bytes in flow view
ok: reader output_buffer_bytes in flow view
ok: reader output_store_bytes in flow view
ok: reader output_store_count in flow view
ok: describe-workers lists 1 worker(s)
ok: get-worker-backtraces returned 47098 bytes for [10.112.149.45]:24580
OK: failure comment reported, buffer/epoch telemetry exposed, worker backtraces work

$ yt flow get-pipeline-state "$YT_DEV_ROOT/working_pipeline_telemetry_py/pipeline"
working
```

The worker job's stderr carries the companion's own log of the injection healing exactly as
designed — eight raises per fail row, six in the failed job and two in its restarted successor:

```
INFO:__main__:Raising on fail key (data: fail-bb6c219b-…, attempt: 1)
…
INFO:__main__:Raising on fail key (data: fail-bb6c219b-…, attempt: 8)
```

A flow-view sample of the epoch telemetry the checks read (a processor job's
`epoch_part_times`, seconds per epoch part):

```
{'Accounting': 0.0133, 'Commit': 0.4229, 'GenerateGlobalUniqueSeqNo': 0.211, 'Input.Fetch': 51.7665, 'Process': 17.1699, ...}
```
