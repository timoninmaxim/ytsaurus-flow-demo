# key_visitor

A pipeline whose subject is the engine's **key-visitor stream**: a per-computation background
sweep the worker runs over every key the computation holds state for, injecting a *visit* per key
into the user code on top of the ordinary message flow — Flow's answer to "iterate my keyed state
periodically" (what a Flink user would build with a processing-time timer re-registered per key).

```
key_reader (stock TSwiftPassthroughOrderedSourceComputation over TQueueSource, finite)
   → keys
tester (visit tester, key_visitor_streams: visit_iter, period 20 s)
   → visits → TSyncQueueSink → output_queue
```

`tester` groups by `farm_hash(key), key`. On a **message** it stores the payload in per-key
internal state; on a **visit** it emits the *stored* payload together with a per-key visit
counter (`visit_index`). The engine drives everything else: the worker tracks the computation's
keys in the pipeline's `key_visitor_states` table, sweeps the partition's hash range once per
`period`, and — because the source is finite and the visitor's `finite` flag defaults to `%true`
— arms one **final pass** after the input is drained, then lets the pipeline complete.

The choreography, ported from the upstream test (`tests/key_visitor/cpp`, `test_key_visitor`):
seed every key with a v1 payload and then again with a v2 payload *before* the pipeline starts;
wait for `completed`; assert that the **latest** visit of every key (highest `visit_index`)
carries the v2 payload. That proves the final pass swept the post-completion state — not a stale
snapshot taken while v1 was still current.

Three variants of `tester` exercise the same choreography and the same assert: the engine's own
C++ (`pipeline/`, this dir), a Go companion (`companion_go/`) and a Java companion
(`companion_java/`).

## C++ variant — not covered by the 0.2.1 artifact run

`pipeline/main.cpp` (`NYT::NFlow::NDemo::TVisitTesterFunction`, a `TProcessFunctionComputation`)
is this scenario's own binary, not the stock `flow_server`: the visit tester is user C++, so the
first candidate was the stock server plus a C++ companion, as in `word_count_sync`. Key visitors
are **not** contract-blocked for companions (unlike `state_joiners`): `companion_service.proto`
carries visits in the process-batch request (`repeated TVisit visits`),
`TTransformCompanionComputation` forwards them together with the states for the visited keys, and
the companion-side SDK routes them into the registered function's `ProcessVisit`
(`companion/server/job.cpp`). The own binary still won, because it is strictly smaller here: the
scenario's subject — key tracking, the periodic sweep, the finite final pass, completion — lives
entirely in the worker, identically under either hosting; the companion route would add a second
binary, the `CompanionManager` resource and a third worker port for zero extra coverage of the
subject.

This needs a source build (its own C++ binary, `pipeline/main.cpp` + `pipeline/ya.make`, staged
into a ytsaurus checkout and built with `ya make` — see `secret_env/README.md` for why staging is
needed) and is **not verified against the released 0.2.1 artifacts** on this repo's CI. `build.sh`
documents the source-build procedure as-is, unverified against 0.2.1. The Go and Java companion
variants below run entirely on the released artifacts and are verified there.

The upstream `lib` + `pipeline/main.cpp` split collapses into a single `pipeline/main.cpp`, and
the class names move from `NKeyVisitorTest` to `NYT::NFlow::NDemo`. Only the internal-state
variant is ported: the sibling upstream variants exercise the same sweep against a
`TSimpleExternalStateManager` table (`pipeline_external`) and a computation whose *only* work
source is the visitor (`pipeline_keyvisitor_only`); the visit choreography and the assert are
those of `test_key_visitor`. Spec shape, the 20 s `visit_iter` period, the finite queue source
and the seeded v1-then-v2 payloads mirror the test exactly.

## Go companion variant

`companion_go/` re-runs the same scenario with the visit tester written in **Go**, hosted
out-of-process by the **stock** `flow_server` through the same companion protocol. The topology,
choreography and asserts are identical:

- `companion_go/main.go` — the visit tester with the Go SDK (`go.ytsaurus.tech/yt/go/flow`): a
  computation implementing `flow.RowFunction` (`OnMessage` stores the payload in the mutable
  typed state `flow.OpenYSONState[userState]`) and `flow.RowVisitFunction` (`OnVisit` emits the
  stored payload with the incremented per-key `visit_index`). Unlike the Java SDK, mutations of
  the state value persist without an explicit write-back — the accessor diffs and flushes them
  itself, as in C++.
- **The pipeline binary is its own runner.** The same `main` calling `pipeline.Run()` is both the
  companion served inside the worker job and the launcher: with no Flow env vars set it enriches
  the spec (the schemas registered with `pipeline.AddStreams` fill `spec/streams`, the
  `CompanionManager` resource gets pointed at the shipped binary and the worker's `port_count` is
  bumped for the companion port — all without a hand-written entry in
  `companion_go/pipeline.yson.j2`) and execs `flow_server`.
- `companion_go/main_test.go` proves the visit logic offline through `flowtest.Harness`
  (message→state, visit→emission, unseeded-key silence, the v1-visit-v2-visit supersession,
  counter survival across payload updates) — no cluster needed.

Everything runs under its own Cypress root, `$YT_DEV_ROOT/key_visitor_go`;
`companion_go/{yt_sync,prepare_data,verify}.py` are the same bootstrap/seed/assert scripts pointed
at that root.

**Everything comes from the Flow 0.2.1 test release**, nothing is built from a ytsaurus source
checkout: the launch runs the pipeline binary itself inside
`ghcr.io/ytsaurus/flow-nightly:dev-0.2.1` (the released `flow_server`, the entrypoint the binary
execs), and the binary is built with the official Go toolchain against
`go.ytsaurus.tech/yt/go/flow@6f22b54593c1` (a pseudo-version of the release commit — the Go SDK is
not tagged for a test release).

### Build

```bash
cd key_visitor/companion_go
podman run --rm -v "$PWD:/src" -w /src -e CGO_ENABLED=0 docker.io/library/golang:1.24 \
    sh -c 'go get go.ytsaurus.tech/yt/go/flow@6f22b54593c1 && go mod tidy && go test ./... && go build -o key_visitor_go .'
```

This resolves the SDK straight from its public module path, runs the offline tests, and produces
the static binary `key_visitor_go` (gitignored) — the same binary is both the launcher and the
companion.

### Run

From `key_visitor/companion_go`:

```bash
python3 yt_sync.py           # once: pipeline node, input_queue + consumer, output_queue, under key_visitor_go/
python3 prepare_data.py      # 20 keys as v1, then the same 20 keys as v2
jinjanate pipeline.yson.j2 > pipeline.yson   # every {{ VAR }} from the env; an unset one fails the render

podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" --entrypoint ./key_visitor_go \
    ghcr.io/ytsaurus/flow-nightly:dev-0.2.1 --config pipeline.yson
```

(If a relative `--entrypoint` does not resolve in your podman version, use
`--entrypoint /app/pipeline/key_visitor_go` instead.) The binary enriches the spec, uploads itself
as the companion, launches the controller+worker vanilla operation and streams the controller
log; it returns once the pipeline completes — the source is finite, so budget about
one and a half minutes (one 20 s visitor period plus the final pass). Then:

```bash
python3 verify.py     # waits for `completed`, mirrors the upstream asserts
cd .. && ./stop.sh key_visitor_go   # aborts the vanilla operation (already completed)
```

### Observed output

Recorded from the live run on the demo cluster against the Flow 0.2.1 test release (GitHub commit
`6f22b54593c1`), image `ghcr.io/ytsaurus/flow-nightly:dev-0.2.1`. The pipeline started at 02:13:18
and printed `Pipeline completed` at 02:14:36 — about 78 seconds end to end; the vanilla spec's
`docker_image` on both tasks confirms the released image, and `verify.py` passed on its first run:

```
$ python3 verify.py
ok: pipeline reached `completed`
ok: all 20 seeded keys were visited
ok: the latest visit of every key carries the v2 payload
output rows: 30; per-key max visit_index range: 1..2
OK: the final key-visitor pass swept the post-completion state of every key
```

## Java companion variant

`companion_java/` re-runs the same scenario with the visit tester written in **Java**, hosted
out-of-process by the **stock** `flow_server` through the same companion protocol. The topology,
choreography and asserts are identical:

- `companion_java/src/main/java/.../VisitTester.java` — the visit tester with the Flow Java SDK
  (`tech.ytsaurus.flow`, modules `flow-core`/`flow-runner`): a `RowFunction` whose `onMessage`
  stores the payload in the per-key internal state `user_state`
  (`StateDescriptors.yson("user_state", UserState.class)`, an `@Entity` POJO) and whose `onVisit`
  emits the stored payload with the incremented per-key `visit_index`. Unlike Go, state mutations
  do **not** auto-flush — every change ends with an explicit `accessor.set(...)`, as in the
  word_count example.
- `companion_java/src/main/java/.../KeyVisitorMain.java` — the shared entry point: registers the
  `tester` computation and hands over to `FlowApplication.run`, which picks the role from
  `YT_FLOW_MODE` (unset means launcher; set inside the worker's spawned companion process).
- `companion_java/pipeline.yson.j2` — the `TJavaCompanionManager` resource names only
  `main_class`: the launcher fills the classpath from the shipped jars and takes the job java from
  its own `java.home`, since it runs inside the same `flow-java-nightly` image as the job — no
  `jdk_bin_path`, no `classpath` set by hand. `port_count = 3` on the worker (RPC + monitoring +
  the companion gRPC port) is spelled out explicitly: unlike the Go runner, the Java one does not
  bump it itself.
- `companion_java/src/test/java/.../VisitTesterTest.java` — the visit logic proven offline through
  `Computation.doProcess` with hand-built requests (message→state, visit→emission, unseeded-key
  silence, the v1-visit-v2-visit supersession, counter survival across payload updates) — no
  cluster needed. The SDK's `flow-test-utils` harness cannot inject visits yet, so the test builds
  `RequestContext`s directly.

Everything runs under its own Cypress root, `$YT_DEV_ROOT/key_visitor_java`;
`companion_java/{yt_sync,prepare_data,verify}.py` are the same bootstrap/seed/assert scripts
pointed at that root.

**Everything comes from the Flow 0.2.1 test release**, nothing is built from a ytsaurus source
checkout: `build.gradle.kts` resolves `tech.ytsaurus:flow-*:0.2.1-SNAPSHOT` from the Sonatype
snapshot repository, and the vanilla tasks (and the launcher itself) run in
`ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1` — the released server image plus a JRE at
`/opt/java/openjdk`, so one image holds both the `flow_server` the jobs run and the `java` the
launcher and the companion run with.

### Build

Built with the official Gradle toolchain container, never a local or Arcadia-built JDK/Gradle:

```bash
cd key_visitor/companion_java
podman run --rm -v "$PWD:/src" -w /src docker.io/library/gradle:8-jdk17 \
    gradle -q --refresh-dependencies test installLib -PflowVersion=0.2.1-SNAPSHOT
```

This resolves the SDK from the Sonatype snapshot repository, runs the offline tests, and syncs the
pipeline jar plus its runtime deps into `lib/` (gitignored) — the classpath the launch command's
`-cp 'lib/*'` uses.

### Run

From `key_visitor/companion_java`:

```bash
python3 yt_sync.py           # once: pipeline node, input_queue + consumer, output_queue, under key_visitor_java/
python3 prepare_data.py      # 20 keys as v1, then the same 20 keys as v2
jinjanate pipeline.yson.j2 > pipeline.yson   # every {{ VAR }} from the env; an unset one fails the render

podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" \
    ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1 -cp 'lib/*' \
    tech.ytsaurus.flow.demo.keyvisitor.KeyVisitorMain --config pipeline.yson
```

The launcher enriches the spec (companion classpath, java binary), uploads the released
`flow_server` and launches the controller+worker vanilla operation, then streams the controller
log; it returns once the pipeline completes. Then:

```bash
python3 verify.py     # waits for `completed`, mirrors the upstream asserts
cd .. && ./stop.sh key_visitor_java   # aborts the vanilla operation (already completed)
```

### Observed output

Recorded from the live run on the demo cluster against the Flow 0.2.1 test release (GitHub commit
`6f22b54593c1`), image `ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1`. The vanilla spec's
`docker_image` on both tasks confirms the released image, and `verify.py` passed on its first run:

```
$ python3 verify.py
ok: pipeline reached `completed`
ok: all 20 seeded keys were visited
ok: the latest visit of every key carries the v2 payload
output rows: 25; per-key max visit_index range: 1..2
OK: the final key-visitor pass swept the post-completion state of every key
```
