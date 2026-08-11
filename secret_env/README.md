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
./run.sh secret_env             # deploy + stream the controller log; Ctrl-C detaches
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
