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

**Not covered by the 0.2.1 artifact run.** This variant needs a source build (its own C++ binary,
see below) and is not verified against the released artifacts on this repo's CI — the "Run" section
below documents the source-build procedure as-is, unverified against 0.2.1. The Java-companion
variant further down runs entirely on the released artifacts and is verified there.

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

# This scenario deploys its own binary instead of the released flow_server: render the spec
# and run that binary directly (it uploads itself to the vanilla jobs).
cd secret_env
jinjanate pipeline.yson.j2 > pipeline.yson
./secret_env_pipeline.stripped --config pipeline.yson
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

When done, `./stop.sh secret_env` (from the repo root) stops the pipeline and aborts the vanilla operation.

## Observed output

The runner keeps streaming: the runner polls the pipeline, the controller reports its jobs (cluster
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

The same subject once more, for the Java shape, entirely on the Flow 0.2.1 test-release artifacts
(see the repo README's "Released artifacts" — nothing here is built from a ytsaurus source
checkout). The user code is Java (`tech.ytsaurus:flow-*`, the Flow Java SDK), and the pipeline
module — `secret_env/companion_java/` — is its own runner: `SecretEnvMain` is launched by the Flow
Java SDK launcher, which spawns `flow_server` as a **child process**
(`FlowLauncher`, `new ProcessBuilder(command)`), and a `ProcessBuilder` hands the child a full copy
of the parent's environment unless told otherwise. The SDK itself never mentions `secret_env` — it
does not need to: the secret exported in your shell rides the inherited environment into
`flow_server`, and the usual chain takes over.

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

The moving parts:

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
- `companion_java/pipeline.yson.j2` — the Python variant's topology (native finite `TQueueSource`
  reader → `TTransformCompanionComputation` → `TSyncQueueSink`) with the Java companion resource:
  `TJavaCompanionManager` names only `main_class` — no `jdk_bin_path`, no `classpath`. The launcher
  fills both in for the vanilla launch: the classpath from the shipped jars, and the job java from
  (in order) a hand-written `jdk_bin_path`, `YT_FLOW_JDK_BIN_PATH`, or — the case here — its own
  `java.home`, since the launcher runs inside the same `flow-java-nightly` image as the job
  (`DockerJobEnvironment.doResolveJdkBinPath`). The vanilla tasks run in that same released image,
  and `port_count = 3` (worker RPC + monitoring + the companion gRPC port). The
  `secret_env = ["YT_MY_SECRET"]` line is unchanged: the launcher→vault→job mechanics are engine
  surface the Java runner shape does not touch.
- `companion_java/src/test/java/.../SecretEnvTest.java` — the checker offline through
  `TestComputationHarness`, pinning the reported columns for the correct, wrong and absent
  environment shapes (the injected-map equivalent of Go's `t.Setenv`; no cluster).
- `companion_java/yt_sync.py` — bootstrap under its own root `$YT_DEV_ROOT/secret_env_java`.
- **The SDK and the server come from the Flow test release, not from a source checkout.**
  `build.gradle.kts` resolves `tech.ytsaurus:flow-*` from Maven: released versions from Maven
  Central, test releases (`X.Y.Z-SNAPSHOT`) from the Sonatype snapshot repository; the version is
  `-PflowVersion` (default `0.2.1-SNAPSHOT`). The vanilla tasks and the launcher itself run in
  `ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1` (the released server image plus a JRE at
  `/opt/java/openjdk`), so one image holds both the `flow_server` the jobs run and the `java` the
  launcher and the companion run with.

### Build

Built with the official Gradle toolchain container, never a local or Arcadia-built JDK/Gradle:

```bash
cd secret_env/companion_java
podman run --rm -v "$PWD:/src" -w /src docker.io/library/gradle:8-jdk17 \
    gradle -q --refresh-dependencies test installLib -PflowVersion=0.2.1-SNAPSHOT
```

This resolves the SDK from the Sonatype snapshot repository (`build.gradle.kts`), runs the offline
tests, and syncs the pipeline jar plus its runtime deps into `lib/` (gitignored) — the classpath
both the launch command and the Flow runner's companion-jar shipping use.

### Run

From `secret_env/companion_java`:

```bash
python3 yt_sync.py           # once: pipeline node, input_queue + consumer, output_queue, under secret_env_java/
jinjanate pipeline.yson.j2 > pipeline.yson   # every {{ VAR }} from the env; an unset one fails the render

echo '{"key"="pos-1"};{"key"="pos-2"};{"key"="pos-3"}' | \
    yt insert-rows "$YT_DEV_ROOT/secret_env_java/input_queue" --format yson

export YT_MY_SECRET=5
podman run --rm -e YT_TOKEN -e YT_MY_SECRET -v "$PWD:/app/pipeline" \
    ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1 -cp 'lib/*' \
    tech.ytsaurus.flow.demo.secretenv.SecretEnvMain --config pipeline.yson
```

The launcher enriches the spec (companion classpath, java binary), uploads the released
`flow_server` and launches the controller+worker vanilla operation, then streams the controller
log; it returns once the pipeline completes — this variant's `reader` is finite. `-e YT_MY_SECRET`
forwards the secret from your shell into the podman container, which is what the launcher's
`ProcessBuilder` then inherits into `flow_server` and, from there, into the vault.

```bash
yt flow get-pipeline-state "$YT_DEV_ROOT/secret_env_java/pipeline"
yt select-rows "key, secret, vault_carries_name from [$YT_DEV_ROOT/secret_env_java/output_queue]" --format json
```

### Observed output

```
$ yt flow get-pipeline-state "$YT_DEV_ROOT/secret_env_java/pipeline"
completed

$ yt select-rows "key, secret, vault_carries_name from [$YT_DEV_ROOT/secret_env_java/output_queue]" --format json
{"key":"pos-1","secret":"5","vault_carries_name":"true"}
{"key":"pos-2","secret":"5","vault_carries_name":"true"}
{"key":"pos-3","secret":"5","vault_carries_name":"true"}
```

`secret = "5"` is the launcher's value read out of `System.getenv` inside the companion JVM — after
riding the `ProcessBuilder` inheritance from the JVM launcher into `flow_server` and back out into
the worker's companion.

### The failure paths, checked as well

With `YT_MY_SECRET` unset the JVM launcher enriches the spec and spawns `flow_server` anyway (the
Java SDK knows nothing of `secret_env`), and `flow_server` refuses **before uploading anything** —
the name check runs up front, ahead of the vault assembly:

```
(NYT::TErrorException) Secret environment variable "YT_MY_SECRET" (declared in "secret_env") is not set
```

With a wrong value (`YT_MY_SECRET=wrong`) the pipeline still completes — this variant reports
rather than asserts — and the observed column tracks the launcher verbatim, so verification fails
on the value alone:

```
{"key":"neg-1","secret":"wrong","vault_carries_name":"true"}
{"key":"neg-2","secret":"wrong","vault_carries_name":"true"}
```

(As in the other companion variants, a second run needs a fresh pipeline node: `completed` is
final, so `yt remove --recursive "$YT_DEV_ROOT/secret_env_java/pipeline"` — after the vanilla
operation is aborted and the controller's ~5 s lock transaction expires — then re-run `yt_sync.py`
and re-insert rows; the queues and the consumer offsets survive, so only the newly inserted rows
are read.)

### Stop

From the repo root:

```bash
./stop.sh secret_env_java
```

It stops the pipeline and aborts its vanilla operation (`completed` needs only the abort). To drop
the scenario's Cypress objects as well: `yt remove -r "$YT_DEV_ROOT/secret_env_java"`.
