# secret_env

A one-computation pipeline that proves a secret handed to the runner in an environment variable
reaches the vanilla job — and never lands in the pipeline's persisted spec on the way.

`checker` is `NYT::NFlow::NDemo::TSecretChecker` (`pipeline/main.cpp`), a
`TSwiftOrderedSourceComputation` over the built-in `NYT::NFlow::TRandomSource`, which generates
messages forever. For every message it reads `YT_MY_SECRET` from its own environment and throws
unless the value is the expected one, so **the assertion is that the pipeline keeps running**: a
missing or wrong secret makes every job fail, and the controller says so within seconds.

The spec asks for the secret with one line in the vanilla block:

```yson
"secret_env" = ["YT_MY_SECRET"];
```

The runner reads that name from *its own* environment at launch and puts the value into the
operation's secure vault. Inside the job YT delivers the vault as `YT_SECURE_VAULT`, and Flow
re-exports each entry as a plain environment variable — which is what the computation reads. The
copy of the spec that Flow persists under the pipeline node (the durable source a reanimated
operation is rebuilt from) has the vault stripped out and keeps only the *name*; the value itself
lives only in the operation's secure vault, which YT protects.

## This scenario is the repo's one custom binary

Every other scenario here runs on the stock `flow_server`. This one ships its own C++
(`pipeline/main.cpp` + `pipeline/ya.make`) and `build.sh` builds it, deliberately: it is the
scenario that shows an external engineer that their own computation compiles and links against the
public YT Flow libraries. It also has no alternative — the assertion is about the environment of
the flow job process itself, which a companion subprocess would only test indirectly (a companion
inherits the worker's environment, so a failure there could mean either link), and the stock
`flow_server` does not link `connectors/random`, so `TRandomSource` is not registered in it.

`pipeline/ya.make` is deliberately minimal: the runner, the random connector and this computation.
It is a template for *your* binary, not a copy of the stock `flow_server`, which additionally links
the companion host, the queue / static-table / sorted-dynamic-table / servicelog connectors and the
resources library — add the ones your spec names.

## Building your own binary: stage sources into the checkout

`ya` only builds targets that live inside the ytsaurus checkout, and there is no supported way to
build against installed Flow libraries from outside the source tree — no exported CMake package, no
headers/libs artifact. So `build.sh` copies `pipeline/main.cpp` and `pipeline/ya.make` into
`$YTSAURUS/yt/yt/flow/demo/secret_env/`, builds that target, strips the result back into this
directory and removes the staging copy again. Your sources still live here and are the thing you
edit; the copy is scratch.

This needs a checkout set up for the `ya make` build described in the ytsaurus repo's `BUILD.md`.
The CMake route cannot do it: the per-target `CMakeLists.txt` files are generated from the `ya.make`
graph rather than authored, so a directory you have just added has none.

## Run

From the repo root:

```bash
export YT_MY_SECRET=5           # the demo's fixed expected value; a real secret would come from
                                # your own store — the point is that it travels via your shell
secret_env/build.sh             # builds + strips the binary (YTSAURUS=<checkout>)
python3 secret_env/yt_sync.py   # once: the pipeline node (no queues or tables in this scenario)

# This scenario deploys its own binary instead of the stock flow_server, so name it:
FLOW_BIN=secret_env/secret_env_pipeline.stripped ./run.sh secret_env
```

The pipeline stays in `working` and the log keeps reporting healthy jobs — that is the assertion
holding. `TRandomSource` is a load generator with no rate limit (~25k messages/s on one worker
here), so stop the pipeline once you have seen what you came for. From a second terminal check the
two things the scenario claims:

```bash
yt flow get-pipeline-state "$YT_DEV_ROOT/secret_env/pipeline"

yt get "$YT_DEV_ROOT/secret_env/pipeline/vanilla/current_spec" --format json | python3 -c '
import json, sys
spec = json.load(sys.stdin)
print("secret_env   =", spec.get("secret_env"))
print("secure_vault =", spec.get("secure_vault"))'
```

When done, `./stop.sh secret_env` stops the pipeline and aborts the vanilla operation.

## Observed output

`run.sh` keeps streaming: the runner polls the pipeline, the controller reports its jobs (cluster
URL and guids elided):

```
I	FlowClient	Waiting pipeline to complete (CurrentState: Working, Pipeline: <…>$YT_DEV_ROOT/secret_env/pipeline)
I	PublicFlowController	Jobs status (PipelineState: Working, Workers: 1, WorkingOld: 0, WorkingYoung: 1, WorkingWithRetryableError: 0, Preparing: 0, Unknown: 0, Stopped: 0, FlowViewAge: …)
```

`WorkingWithRetryableError: 0` with no `Job failed` line is the assertion holding — every message
so far read the expected secret out of its own environment.

The two checks:

```
$ yt flow get-pipeline-state "$YT_DEV_ROOT/secret_env/pipeline"
working

$ yt get ".../vanilla/current_spec" ... 
secret_env   = ['YT_MY_SECRET']
secure_vault = None
```

The persisted spec keeps only the *name* of the secret.

### The failure paths, checked as well

With `YT_MY_SECRET` unset the runner refuses to launch, though only after it has uploaded the
binary:

```
(NYT::TErrorException) Secret environment variable "YT_MY_SECRET" (declared in "secret_env") is not set
```

With a wrong value (`YT_MY_SECRET=wrong`) the pipeline stays `working` and the controller log
repeats (guids elided, and the `origin`/`datetime` attribute block that follows the message):

```
E	PublicFlowController	Job failed (JobId: …, PartitionId: …, ComputationId: checker)
YT_MY_SECRET did not reach the vanilla job as expected (length 5, secure vault carries [YT_MY_SECRET, YT_TOKEN])
    origin          … (pid …, thread Jobs:3, fid …)
    datetime        …
```

That message never prints the value, and its vault-key list separates the two links of the chain:
the vault carried `YT_MY_SECRET`, so delivery into the job worked and only the value was wrong. An
empty vault list would have meant the secret never reached the job at all.

## Python companion variant

The same subject, one process further out: the user code is Python, hosted by the **stock**
`flow_server`, and runs as a *companion* — a separate process the worker spawns and drives over
gRPC. The question this variant answers is whether the secret survives that extra hop: the section
above notes that a companion tests the chain only indirectly; this variant makes the companion's
view direct evidence by reporting it instead of crashing on it.

**Verdict: the secret reaches the companion.** The chain, in the engine's code:

1. YT delivers the operation's secure vault to the job as the `YT_SECURE_VAULT` env var; at
   startup the flow job re-exports every entry as a plain env var
   (`yt/yt/flow/library/cpp/runner/init.cpp`, `Initialize`).
2. The worker spawns the companion entrypoint with a **full copy of its own environment** —
   `library/cpp/companion/companion_process_manager.cpp` creates the child with
   `TSimpleProcess(executable, /*copyEnv*/ true)`, which snapshots the worker's `environ` and adds
   `YT_FLOW_COMPANION_CONFIG` on top; nothing is scrubbed.

So the Python process inherits both the re-exported `YT_MY_SECRET` and the raw `YT_SECURE_VAULT`
text, and the run below confirms it empirically.

The moving parts (all under this scenario dir, alongside the C++ variant):

- `companion_py/main.py` — `SecretChecker`, a `RowFunction`: for every input message it writes
  what it observed into the output queue — `secret` (the value of `YT_MY_SECRET` in the
  companion's own environment) and `vault_carries_name` (whether the inherited `YT_SECURE_VAULT`
  text mentions the name; a substring probe, diagnostic only). The verification then matches the
  reported value from outside — stronger than the C++ variant's absence-of-failures, because the
  value in the queue can only have come from the companion's environment. (It also means the demo
  value lands in an output table; report a hash instead if your secret is real.)
- `companion_py/build.sh` — packs `companion_bundle.tgz`: a self-contained CPython plus the
  `ytsaurus-flow-companion` SDK wheel built from
  `$YTSAURUS_SRC/yt/yt/flow/tools/python_companion_package`, plus `main.py`.
- `companion_py/py_companion` — the entrypoint the worker spawns; unpacks the bundle and runs the
  companion gRPC server.
- `pipeline_py.yson.template` — stock C++ `TQueueSource` reader (finite) feeding a
  `TTransformCompanionComputation` that hosts the Python function; a `TSyncQueueSink` writes the
  observations. The input is a small prepared queue rather than the C++ variant's `TRandomSource`
  (the stock `flow_server` does not register it), and `finite = %true` makes the pipeline
  self-complete — so the assertion here is `completed` *plus* the matched value, not "keeps
  running". The `secret_env = ["YT_MY_SECRET"]` line in the vanilla block is unchanged: the
  launcher→vault→job mechanics are engine surface that companion hosting does not touch.
- `companion_py/yt_sync.py` — bootstrap under its own root `$YT_DEV_ROOT/secret_env_py`.

### Run

```bash
secret_env/companion_py/build.sh          # once: the companion bundle (YTSAURUS_SRC=<checkout>)
python3 secret_env/companion_py/yt_sync.py

# Temporary, demo-cluster only: the bootstrap preset creates the pipeline system tables with
# reed_solomon_3_3, which the currently degraded demo cluster cannot hold. Flip them to plain
# replication before the first run:
for t in $(yt find "$YT_DEV_ROOT/secret_env_py" --type table); do
    [ "$(yt get "$t/@erasure_codec")" = '"none"' ] || \
        { yt set "$t/@erasure_codec" none; yt remount-table "$t"; }
done

echo '{"key"="pos-1"};{"key"="pos-2"};{"key"="pos-3"}' | \
    yt insert-rows "$YT_DEV_ROOT/secret_env_py/input_queue" --format yson

export YT_MY_SECRET=5
./run.sh secret_env py                    # stock flow_server; waits for completion
```

The runner drains the queue and exits on its own:

```
I	FlowClient	Waiting pipeline to complete (CurrentState: Working, Pipeline: <…>/secret_env_py/pipeline)
I	FlowClient	Pipeline completed (Pipeline: <…>/secret_env_py/pipeline)
```

Then verify what the companion saw (`./stop.sh secret_env_py` afterwards aborts the vanilla
operation — `completed` is final, so there is no pipeline left to stop):

```
$ yt flow get-pipeline-state "$YT_DEV_ROOT/secret_env_py/pipeline"
completed

$ yt select-rows "* from [$YT_DEV_ROOT/secret_env_py/output_queue]" --format json
{... "key":"pos-1","secret":"5","vault_carries_name":"true" ...}
{... "key":"pos-2","secret":"5","vault_carries_name":"true" ...}
{... "key":"pos-3","secret":"5","vault_carries_name":"true" ...}
```

`secret = "5"` is the launcher's value read out of `os.environ` inside the Python process;
`vault_carries_name = "true"` says the raw vault text made it in as well.

### The failure paths, checked as well

With `YT_MY_SECRET` unset the runner refuses to launch, exactly as in the C++ variant:

```
(NYT::TErrorException) Secret environment variable "YT_MY_SECRET" (declared in "secret_env") is not set
```

With a wrong value (`YT_MY_SECRET=wrong`) the pipeline still completes — this variant reports
rather than asserts — and the observed column tracks the launcher verbatim, so verification fails
on the value:

```
{... "key":"neg-1","secret":"wrong","vault_carries_name":"true" ...}
{... "key":"neg-2","secret":"wrong","vault_carries_name":"true" ...}
```

(That second run needs a fresh pipeline node: `completed` is a final state the controller never
leaves, so re-running the same finite pipeline means `yt remove --recursive .../secret_env_py/pipeline`,
re-running `yt_sync.py`, and re-applying the erasure workaround; the queues and the consumer — and
its offsets — survive, so only the newly inserted rows are read.)
