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

## Java companion variant

The same subject once more, for the Java shape. The user code is Java
(`tech.ytsaurus:flow-*`, the Flow Java SDK), and — as in the Go variant — the pipeline module is
its own runner: the same `SecretEnvMain` is launched on the dev host to deploy and inside the
worker job as the companion. The launcher-side wrinkle differs from Go's, though: the Java runner
does not *exec* `flow_server`, it spawns it as a **child process**
(`yt/java/flow/flow-runner/.../FlowLauncher.java`, `new ProcessBuilder(command)`), and a
`ProcessBuilder` hands the child a full copy of the parent's environment unless told otherwise.
The SDK itself never mentions `secret_env` — it does not need to: the secret exported in your
shell rides the inherited environment into `flow_server`, and the usual chain takes over.

**Verdict: the chain holds end to end, four hops deep.** In the engines' code:

1. `FlowLauncher` spawns `flow_server` with the JVM's full environment (`ProcessBuilder`'s
   default);
2. `flow_server` (runner mode) validates the names declared in `secret_env` **up front** and
   reads each from that inherited environment into the operation's secure vault
   (`library/cpp/runner/vanilla_launcher.cpp`, ValidateSecretEnv;
   `library/cpp/vanilla/spec.cpp`, InjectSecureVaultFromEnv);
3. inside the job, YT delivers the vault as `YT_SECURE_VAULT` and Flow re-exports each entry as
   a plain env var (`library/cpp/runner/init.cpp`, Initialize);
4. the worker spawns the companion JVM with a full copy of its own environment
   (`library/cpp/companion/java_process_manager.cpp`, copyEnv=true), so `System.getenv` in the
   process function sees both the re-exported secret and the raw vault text.

The moving parts, alongside the other variants:

- `companion_java/src/main/java/.../SecretCheckerFunction.java` — the checker, a `RowFunction`
  that *reports* like the Python and Go variants: for every input message it writes `secret`
  (the value of `YT_MY_SECRET` in its own environment) and `vault_carries_name` (whether the
  inherited `YT_SECURE_VAULT` text mentions the name; a substring probe, diagnostic only) into
  the output queue. Verification matches the value from outside — it can only have come from the
  companion JVM's environment. (The demo value lands in an output queue; report a hash instead
  if your secret is real.) The environment is injectable (`UnaryOperator<String>`, defaulting to
  `System::getenv`) because the JVM has no `setenv` for tests to use.
- `companion_java/src/main/java/.../SecretEnvMain.java` — the shared entry point: registers the
  `checker` computation and hands over to `FlowApplication.run`, which picks the role from
  `YT_FLOW_MODE`.
- `pipeline_java.yson.template` — the Python variant's topology (native finite `TQueueSource`
  reader → `TTransformCompanionComputation` → `TSyncQueueSink`) with the Java companion resource:
  `TJavaCompanionManager` naming only `main_class` (the runner completes the classpath and the
  JDK binary path), the worker running in a plain `eclipse-temurin:17-jre` docker image instead
  of the SDK's default JDK porto layers (this cluster has none — hence the two `YT_FLOW_*`
  overrides in `companion_java/run.sh`), and `port_count = 3` (worker RPC + monitoring + the
  companion gRPC port). The `secret_env = ["YT_MY_SECRET"]` line is unchanged: the
  launcher→vault→job mechanics are engine surface the Java runner shape does not touch.
- `companion_java/src/test/java/.../SecretEnvTest.java` — the checker offline through
  `TestComputationHarness`, pinning the reported columns for the correct, wrong and absent
  environment shapes (the injected-map equivalent of Go's `t.Setenv`; no cluster).
- `companion_java/yt_sync.py` — bootstrap under its own root `$YT_DEV_ROOT/secret_env_java`.
- The Flow Java SDK is not published to Maven Central yet, so `settings.gradle.kts`
  composite-includes a sibling source checkout of `github.com/ytsaurus/ytsaurus` and substitutes
  the `tech.ytsaurus:flow-*` coordinates with its Gradle subprojects — the Java equivalent of
  the Go variant's `go.mod` `replace`. `./run.sh` does not fit this route either (the runner is
  the JVM entry point, not `$FLOW_BIN`), so `companion_java/run.sh` renders the template and
  launches it directly.

### Run

```bash
cd secret_env/companion_java
./build.sh                      # gradle test + collectRuntime (JDK 17+, checkout next door)
python3 yt_sync.py              # once: objects under secret_env_java/

echo '{"key"="pos-1"};{"key"="pos-2"};{"key"="pos-3"}' | \
    yt insert-rows "$YT_DEV_ROOT/secret_env_java/input_queue" --format yson

export YT_MY_SECRET=5
./run.sh                        # spawns flow_server; returns when the pipeline completes

yt flow get-pipeline-state "$YT_DEV_ROOT/secret_env_java/pipeline"
yt select-rows "key, secret, vault_carries_name from [$YT_DEV_ROOT/secret_env_java/output_queue]" --format json
cd ../.. && ./stop.sh secret_env_java   # aborts the vanilla operation ("completed" is final)
```

On this demo cluster (degraded data nodes), run the erasure-codec workaround right after
`yt_sync.py` and before deploying: on every table under `$YT_DEV_ROOT/secret_env_java` still
carrying an erasure codec (with the current bootstrap that is only the pipeline system tables),
set `@erasure_codec = none` and `@hunk_erasure_codec = none` and remount (`yt unmount-table
--force` if one wedges in `unmounting`). Without it table writes stall hunting for erasure part
replicas.

Recorded from the live run on the demo cluster, `flow_server` and the SDK built from the same
checkout as the state_joiner Java variant (flow-core commit `baaaeedbe3c`, heads/main): vanilla
operation started 02:27:04 → pipeline `working` by 02:27:48 → `completed` 02:28:20, 76 s from
operation start; the persisted `vanilla/current_spec` again keeps only the
*name* (`secret_env = ['YT_MY_SECRET']`, `secure_vault = None`), and the stored pipeline spec
shows what the Java runner injected (the `java_companion/*.jar` file entries — 65 jars, 39 MB —
and the completed `TJavaCompanionManager` with `classpath` and `jdk_bin_path`):

```
$ yt flow get-pipeline-state "$YT_DEV_ROOT/secret_env_java/pipeline"
completed

$ yt select-rows "key, secret, vault_carries_name from [$YT_DEV_ROOT/secret_env_java/output_queue]" --format json
{"key":"pos-1","secret":"5","vault_carries_name":"true"}
{"key":"pos-2","secret":"5","vault_carries_name":"true"}
{"key":"pos-3","secret":"5","vault_carries_name":"true"}
```

`secret = "5"` is the launcher's value read out of `System.getenv` inside the companion JVM —
after riding the `ProcessBuilder` inheritance from the JVM runner into `flow_server` on the dev
host.

### The failure paths, checked as well

With `YT_MY_SECRET` unset the JVM runner enriches the spec and spawns `flow_server` anyway (the
Java SDK knows nothing of `secret_env`), and `flow_server` refuses **before uploading anything**
— the name check runs up front, ahead of the vault assembly:

```
(NYT::TErrorException) Secret environment variable "YT_MY_SECRET" (declared in "secret_env") is not set
```

With a wrong value (`YT_MY_SECRET=wrong`) the pipeline still completes — this variant reports
rather than asserts — and the observed column tracks the launcher verbatim, so verification
fails on the value:

```
{"key":"neg-1","secret":"wrong","vault_carries_name":"true"}
{"key":"neg-2","secret":"wrong","vault_carries_name":"true"}
```

(As in the other companion variants, that second run needs a fresh pipeline node: `completed` is
final, so `yt remove --recursive .../secret_env_java/pipeline` — after the vanilla operation is
aborted and the controller's ~5 s lock transaction expires — then re-run `yt_sync.py` and
re-apply the erasure workaround; the queues and the consumer offsets survive, so only the newly
inserted rows are read.)
