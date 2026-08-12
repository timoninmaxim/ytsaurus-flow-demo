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
