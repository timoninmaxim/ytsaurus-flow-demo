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

## Go companion variant

`companion_go/` re-runs the scenario with the failure-injecting user code written in **Go**
(`go.ytsaurus.tech/yt/go/flow`), hosted by the stock `flow_server` — no custom binary. `main.go`
registers both computations with the Flow Go SDK: `read` on the swift-source path
(`TSwiftOrderedSourceCompanionComputation`) forwards each input row and fails on the spec-injected
keys, `drop` (`TTransformCompanionComputation`) consumes the stream. The telemetry subject and
every assert are unchanged: `companion_go/verify.py` is the reference `verify.py` with only the
pipeline path switched to this variant's own root `$YT_DEV_ROOT/working_pipeline_telemetry_go`.
**The pipeline binary is its own runner**, as in the other `companion_go` variants: the same
`main` calling `pipeline.Run()` is the companion served inside the worker job and the launcher run
on the dev host, so the spec has no `streams` block (the `data` schema is injected), no
`entrypoint` and no `local_files` — but it does keep a `backoff` block on the CompanionManager
resource, which the runner's injection preserves.

The variant mirrors the adaptation proven by the Python-companion variant (branch
`working-pipeline-telemetry-py`), unweakened:

- **The input is a queue fed from the dev host, not `TRandomSource`** (the stock binary does not
  link the random connector): `companion_go/feed.py` writes ~800 rows/s of keys drawn from
  `range(1000)`, plus one `fail_key = "1100"` row with a unique `data` value every `--fail-every`
  seconds.
- **The failure is transient by construction**: the failure repeats per unique fail row exactly
  `fail_attempts = 8` times (a process-local count in the companion, which survives job restarts)
  and then lets the row pass. Paired with `backoff = {invocation_count = 5}` on the
  CompanionManager resource — a retry budget of `invocation_count + 1 = 6` attempts per job —
  six failures exhaust the first budget (one genuine job failure fires) and the restarted job's
  re-read spends the remaining two inside its own budget and passes.
- **The processor sleeps 2 ms per message** (`sleep_per_message_ms`), so its input buffer visibly
  holds data.

### What a Go failure looks like in `describe-pipeline` — both shapes

This port's novel question: Go user code can fail two ways — **returning an error** from
`OnMessage` and **panicking** — and the spec injects both (`fail_key = "1100"` returns an error,
`panic_key = "1101"` panics; the SDK server recovers the panic, so the companion process survives
either). Both travel the same road: the Go server rejects the `ProcessBatch` call with a flat
`codes.Internal` gRPC status string — there is no structured `yt-error-bin` attachment as in the
Python SDK — and that one string, prefixed by the SDK's own wrapping, becomes the *entire* error
text on the worker side. Observed verbatim on the live run:

```
Job failed (JobFinishReason: Failed): flow: process batch failed: computation "reader": OnMessage on input "1a9f7cdb0000000e-queue:2320:00": Got fail key 1100. Comment: TELEMETRY_DEMO_INTENTIONAL_FAIL
Job failed (JobFinishReason: Failed): flow: process batch failed: flow: computation "reader" panicked: Got panic key 1101. Comment: TELEMETRY_DEMO_INTENTIONAL_FAIL
```

(and, while the retry budget lasts, the same texts behind
`Retryable error in component "/operations/DoProcess": ` — either message satisfies the first
assert.) Three consequences of the flat string, compared to the C++ variant's
`Job failed (JobFinishReason: Failed): Got fail key 1100. Comment: …` and the Python variant's
`… Error processing batch: Got fail key 1100. Comment: …`:

- The user error text is fully preserved (comment included — the assert holds), wrapped in the
  SDK's chain `flow: process batch failed: computation "reader": OnMessage on input "<message
  id>": …`; a panic instead reads `flow: process batch failed: flow: computation "reader"
  panicked: <panic value>`.
- The describe message carries **one** flat error — code 1 (generic), with the failing gRPC
  call's attributes (`method: ProcessBatch`, `service: …CompanionService`, `status_code: 13`) —
  not a structured chain with distinct codes per level; note also that the C++ worker's usual
  `Error processing batch` wrapper text is absent, replaced by the status string itself.
- The **panic stack trace is not in `describe-pipeline`** — the SDK server logs it
  companion-side (`stack_trace` field of the `process batch failed` log line); the engine only
  ever sees the one-line status string.

The panic healed exactly as designed: the controller log shows precisely eight `panicked` reports
for the one injected panic row — six in the failed job, two in its restarted successor — then
silence.

### Run

From the repo root (the sibling `~/ytsaurus` checkout provides the SDK through the `replace` in
`go.mod`; `./run.sh` does not fit the Go route — the Go runner is the pipeline binary itself and
needs `--flow-bin` — so the template is rendered with a one-liner and the binary launched
directly):

```bash
working_pipeline_telemetry/companion_go/build.sh    # go build; GO="ya tool go" if no system go
(cd working_pipeline_telemetry/companion_go && ${GO:-go} test ./...)  # offline injection-logic tests

python3 working_pipeline_telemetry/companion_go/yt_sync.py  # once: pipeline node, input_queue + consumer
```

On a cluster with fewer than six online data nodes, clear the erasure codec the pipeline preset
puts on the system tables (see `state_joiner/README.md` for the full story of this sharp edge)
before the first deploy:

```bash
for t in $(yt find "$YT_DEV_ROOT/working_pipeline_telemetry_go" --type table); do
    yt set "$t/@erasure_codec" none
    yt set "$t/@hunk_erasure_codec" none
    yt remount-table "$t"
done
```

Then deploy and, from a second terminal, feed and verify:

```bash
cd working_pipeline_telemetry
SCENARIO_DIR="$PWD" python3 -c 'import os, string, sys; sys.stdout.write(string.Template(sys.stdin.read()).substitute(os.environ))' \
    < pipeline_go.yson.template > pipeline_go.yson
./companion_go/working_pipeline_telemetry_go --config pipeline_go.yson \
    --flow-bin ~/ytsaurus/yt/yt/flow/bin/flow_server/flow_server.stripped
                                    # deploy + stream the controller log; Ctrl-C detaches

python3 working_pipeline_telemetry/companion_go/feed.py --duration 900   # keep it running…
python3 working_pipeline_telemetry/companion_go/verify.py                # …while this checks

# The panic-shaped failure, once, on demand:
echo '{"key": "1101", "data": "panic-manual-0001", "$$tablet_index": 0}' \
    | yt insert-rows --format json "$YT_DEV_ROOT/working_pipeline_telemetry_go/input_queue"

./stop.sh working_pipeline_telemetry_go   # stop the pipeline + abort the vanilla operation
```

### Observed output

Recorded from the live run on the demo cluster, `flow_server` built from ytsaurus commit
`1bdcb82f3ab` (heads/main), one worker, feed at 800 rows/s with a fail row every 45 s. The
pipeline reached `working` within a minute of launch; `verify.py`, complete first-pass run, no
check needed a retry beyond the flow-view samples upstream also waits for:

```
$ python3 working_pipeline_telemetry/companion_go/verify.py
    job-failure message: Retryable error in component "/operations/DoProcess": flow: process batch failed: computation "reader": OnMessage on input "1a9f7cdb0000000e-queue:2320:00": Got fail key 1100. Comment: TELEMETRY_DEMO_INTENTIONAL_FAIL
ok: fail comment in a describe-pipeline reader job-failure message
ok: reader epoch_part_times in flow view
ok: processor input_buffer_bytes in flow view
ok: reader output_buffer_bytes in flow view
ok: reader output_store_bytes in flow view
ok: reader output_store_count in flow view
ok: describe-workers lists 1 worker(s)
ok: get-worker-backtraces returned 54777 bytes for [10.112.146.65]:24580
OK: failure comment reported, buffer/epoch telemetry exposed, worker backtraces work

$ yt flow get-pipeline-state "$YT_DEV_ROOT/working_pipeline_telemetry_go/pipeline"
working
```

A second pass after the manual panic row found the panic-shaped `Job failed` message quoted
above and passed all checks again. A flow-view sample of the epoch telemetry the checks read
(a processor job's `epoch_part_times`, seconds per epoch part):

```
{'Accounting': 0.0131, 'Commit': 0.501, 'GenerateGlobalUniqueSeqNo': 0.1941, 'Input.Deduplicate': 0.1108, ...}
```

Startup noise matches the other Go runs' profile: one
`E SimpleRunner Found specs parseability error — Static spec has unrecognized fields` line naming
the user parameters (`fail_key`, `panic_key`, `fail_comment`, `fail_attempts`,
`sleep_per_message_ms`) — logged unconditionally, refuses nothing, and the parameters do reach
the companion, as every injected failure proves.
