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

## Go companion variant

The same subject again, with a launcher-side wrinkle the other variants do not have: the user code
is Go (`go.ytsaurus.tech/yt/go/flow`), and **the pipeline binary is its own runner** — the same
`main` calling `pipeline.Run()` is run on the dev host to deploy and served inside the worker job
as the companion. Deploying, the Go runner enriches the spec and then *replaces itself* with
`flow_server` (`yt/go/flow/runner/runner.go`: `syscall.Exec(flowBin, …, os.Environ())`), so the
secret exported in your shell must survive that exec before the usual chain even starts.

**Verdict: it does, end to end.** The chain for this shape, in the engines' code:

1. `syscall.Exec` hands the launcher's **full environment** to `flow_server` — `os.Environ()` is
   passed explicitly, nothing is filtered.
2. `flow_server` (runner mode) reads each name declared in the spec's `secret_env` from that
   inherited environment into the operation's secure vault
   (`yt/yt/flow/library/cpp/vanilla/spec.cpp`, `InjectSecureVaultFromEnv`) — and validates the
   names **up front**, before uploading anything (`ValidateSecretEnv` in
   `library/cpp/runner/vanilla_launcher.cpp`).
3. Inside the job YT delivers the vault as `YT_SECURE_VAULT` and Flow re-exports every entry as a
   plain env var (`library/cpp/runner/init.cpp`, `Initialize`) — unchanged from the C++ variant.
4. The worker spawns the shipped Go binary again — now as the companion — with a full copy of its
   own environment (`library/cpp/companion/companion_process_manager.cpp`, `copyEnv = true`), so
   `os.Getenv` in the Go function sees both the re-exported secret and the raw vault text.

The moving parts, alongside the C++ and Python variants:

- `companion_go/main.go` — `secretChecker`, a `flow.RowFunction` that *reports* like the Python
  variant: for every input message it writes `secret` (the value of `YT_MY_SECRET` in its own
  environment) and `vault_carries_name` (whether the inherited `YT_SECURE_VAULT` text mentions the
  name; a substring probe, diagnostic only) into the output queue. Verification matches the value
  from outside — it can only have come from the companion's environment. (The demo value lands in
  an output table; report a hash instead if your secret is real.)
- `pipeline_go.yson.template` — the Python variant's topology (native finite `TQueueSource` reader
  → `TTransformCompanionComputation` → `TSyncQueueSink`) minus everything `pipeline.Run()` injects
  itself: no `streams` block (the registered schemas are injected), no `entrypoint`, no
  `local_files`, no worker `port_count`. The `secret_env = ["YT_MY_SECRET"]` line is unchanged:
  the launcher→vault→job mechanics are engine surface the Go runner shape does not touch.
- `companion_go/main_test.go` — the checker offline through `flowtest.Harness`, pinning the
  reported columns for the correct, wrong, and absent environment shapes (`t.Setenv`, no cluster).
- `companion_go/yt_sync.py` — bootstrap under its own root `$YT_DEV_ROOT/secret_env_go`.
- The Go Flow SDK is not in a tagged `go.ytsaurus.tech/yt/go` release yet, so `go.mod` replaces
  the module with a sibling source checkout of `github.com/ytsaurus/ytsaurus`. `./run.sh` does not
  fit the Go route — it execs `$FLOW_BIN --config <spec>`, while the Go runner is the pipeline
  binary itself and needs `--flow-bin` on top — so the template is rendered with a one-liner and
  the binary is launched directly (below).

### Run

```bash
secret_env/companion_go/build.sh          # go build; GO="ya tool go" if there is no system go
(cd secret_env/companion_go && ${GO:-go} test ./...)   # offline checker tests

python3 secret_env/companion_go/yt_sync.py   # once: objects under secret_env_go/

echo '{"key"="pos-1"};{"key"="pos-2"};{"key"="pos-3"}' | \
    yt insert-rows "$YT_DEV_ROOT/secret_env_go/input_queue" --format yson

export YT_MY_SECRET=5
cd secret_env
python3 -c 'import os, string, sys; sys.stdout.write(string.Template(sys.stdin.read()).substitute(os.environ))' \
    < pipeline_go.yson.template > pipeline_go.yson
./companion_go/secret_env_go --config pipeline_go.yson \
    --flow-bin ~/ytsaurus/yt/yt/flow/bin/flow_server/flow_server.stripped
                                        # execs flow_server; returns when the pipeline completes

yt flow get-pipeline-state "$YT_DEV_ROOT/secret_env_go/pipeline"
yt select-rows "key, secret, vault_carries_name from [$YT_DEV_ROOT/secret_env_go/output_queue]" --format json
cd .. && ./stop.sh secret_env_go        # aborts the vanilla operation ("completed" is final)
```

On this demo cluster (degraded data nodes), run the erasure-codec workaround right after
`yt_sync.py` and before deploying: set `@erasure_codec = none` and `@hunk_erasure_codec = none` on
every table under `$YT_DEV_ROOT/secret_env_go` (queues, consumer and all pipeline system tables)
and remount each (`yt unmount-table --force` first if one wedges in `unmounting`). Without it
table writes stall hunting for erasure part replicas.

Recorded from the live run on the demo cluster, `flow_server` built from ytsaurus commit
`1bdcb82f3ab` (heads/main): runner launched 19:20:54 → vanilla operation started 19:20:56 →
pipeline `working` 19:21:08 → `completed` 19:22:07, 73 s end to end; the persisted
`vanilla/current_spec` again keeps only the *name* (`secret_env = ['YT_MY_SECRET']`,
`secure_vault = None`), and the stored pipeline spec shows what the Go runner injected
(`entrypoint = {executable = "./go_companion"}; run_process = %true`, worker `port_count: 3` with
a `go_companion` file entry):

```
$ yt flow get-pipeline-state "$YT_DEV_ROOT/secret_env_go/pipeline"
completed

$ yt select-rows "key, secret, vault_carries_name from [$YT_DEV_ROOT/secret_env_go/output_queue]" --format json
{"key":"pos-1","secret":"5","vault_carries_name":"true"}
{"key":"pos-2","secret":"5","vault_carries_name":"true"}
{"key":"pos-3","secret":"5","vault_carries_name":"true"}
```

`secret = "5"` is the launcher's value read out of `os.Getenv` inside the Go companion — after
surviving the launcher's own exec into `flow_server` on the dev host.

### The failure paths, checked as well

With `YT_MY_SECRET` unset the exec into `flow_server` happens anyway (the Go runner knows nothing
of `secret_env`), and `flow_server` refuses **before uploading anything** — the name check runs up
front, ahead of the vault assembly:

```
(NYT::TErrorException) Secret environment variable "YT_MY_SECRET" (declared in "secret_env") is not set
```

With a wrong value (`YT_MY_SECRET=wrong`) the pipeline still completes — this variant reports
rather than asserts — and the observed column tracks the launcher verbatim, so verification fails
on the value:

```
{"key":"neg-1","secret":"wrong","vault_carries_name":"true"}
{"key":"neg-2","secret":"wrong","vault_carries_name":"true"}
```

(As in the Python variant, that second run needs a fresh pipeline node: `completed` is final, so
`yt remove --recursive .../secret_env_go/pipeline` — after `./stop.sh secret_env_go` releases the
controller's lock on it — then re-run `yt_sync.py` and re-apply the erasure workaround; the queues
and the consumer offsets survive, so only the newly inserted rows are read.)
