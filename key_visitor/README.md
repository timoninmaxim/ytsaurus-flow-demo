# key_visitor

A pipeline whose subject is the engine's **key-visitor stream**: a per-computation background
sweep the worker runs over every key the computation holds state for, injecting a *visit* per key
into the user code on top of the ordinary message flow — Flow's answer to "iterate my keyed state
periodically" (what a Flink user would build with a processing-time timer re-registered per key).

```
key_reader (stock TSwiftPassthroughOrderedSourceComputation over TQueueSource, finite)
   → keys
tester (TProcessFunctionComputation hosting NYT::NFlow::NDemo::TVisitTesterFunction,
        key_visitor_streams: visit_iter, period 20 s)
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

## This scenario ships its own binary — but a companion would have worked

The visit tester is user C++, so the first candidate was the stock `flow_server` plus a C++
companion, as in `word_count_sync`. Key visitors are **not** contract-blocked for companions
(unlike `state_joiners`): `companion_service.proto` carries visits in the process-batch request
(`repeated TVisit visits`), `TTransformCompanionComputation` forwards them together with the
states for the visited keys, and the companion-side SDK routes them into the registered
function's `ProcessVisit` (`companion/server/job.cpp`).

The own binary still won, because it is strictly smaller here. The scenario's subject — key
tracking, the periodic sweep, the finite final pass, completion — lives entirely in the worker,
identically under either hosting; the companion route would add a second binary, the
`CompanionManager` resource and a third worker port for zero extra coverage of the subject. So:
`build.sh` stages `pipeline/` into your ytsaurus checkout, builds with `ya make` and strips the
result back here, exactly like `working_pipeline_telemetry` (see `secret_env/README.md` for why
staging is needed).

## Deviation from the upstream test, deliberate

The upstream `lib` + `pipeline/main.cpp` split collapses into a single `pipeline/main.cpp`, and
the class names move from `NKeyVisitorTest` to `NYT::NFlow::NDemo`. Only the internal-state
variant is ported: the sibling upstream variants exercise the same sweep against a
`TSimpleExternalStateManager` table (`pipeline_external`) and a computation whose *only* work
source is the visitor (`pipeline_keyvisitor_only`); the visit choreography and the assert are
those of `test_key_visitor`. Spec shape, the 20 s `visit_iter` period, the finite queue source
and the seeded v1-then-v2 payloads mirror the test exactly.

## Run

From the repo root:

```bash
key_visitor/build.sh              # builds + strips the binary (YTSAURUS=<checkout>)
python3 key_visitor/yt_sync.py    # once: pipeline node, input/output queues, consumer
python3 key_visitor/prepare_data.py  # 20 keys as v1, then the same 20 keys as v2

FLOW_BIN=key_visitor/key_visitor_pipeline.stripped \
    ./run.sh key_visitor          # deploys and streams the log until the pipeline completes
```

The source is finite, so `run.sh` returns on its own — budget about three minutes (one 20 s
visitor period has to elapse, plus the final pass). Then:

```bash
python3 key_visitor/verify.py     # waits for `completed`, then mirrors the upstream asserts
./stop.sh key_visitor             # aborts the vanilla operation (the pipeline is already completed)
```

The raw output is also one query away:

```bash
yt select-rows "key, payload, visit_index from [$YT_DEV_ROOT/key_visitor/output_queue]" --format json
```

## Observed output

Recorded from the live run on the demo cluster, server build `26.2.0-local-os~5c69dd1804e43fe5`
(printed by `run.sh` on the way in). The pipeline was deployed with both payload batches already
in the queue and reached `completed` in about three minutes; `verify.py` passed on its first run:

```
$ python3 key_visitor/verify.py
ok: pipeline reached `completed`
ok: all 20 seeded keys were visited
ok: the latest visit of every key carries the v2 payload
output rows: 38; per-key max visit_index range: 1..2
OK: the final key-visitor pass swept the post-completion state of every key
```

The 1..2 spread is the sweep made visible: keys whose hash range the periodic pass reached while
the pipeline was still working were visited twice (once with whatever payload was current, once
by the final pass), the rest exactly once — by the final pass, every one of them with v2. The
tail of the runner log:

```
2026-08-12 23:57:32,434273 I PublicFlowController Job completed (..., ComputationId: tester)
2026-08-13 02:58:01,146507 I FlowClient Pipeline completed (Pipeline: .../key_visitor/pipeline)
```

## Go companion variant

`companion_go/` re-runs the same scenario with the visit tester written in **Go**, hosted
out-of-process by the **stock** `flow_server` through the same companion protocol as the Python
variant. The topology, choreography and asserts are identical; two things change:

- `companion_go/main.go` — the visit tester with the Go SDK (`go.ytsaurus.tech/yt/go/flow`): a
  computation implementing `flow.RowFunction` (`OnMessage` stores the payload in the mutable
  typed state `flow.OpenYSONState[userState]`) and `flow.RowVisitFunction` (`OnVisit` emits the
  stored payload with the incremented per-key `visit_index`). Unlike the Python SDK, mutations
  of the state value persist without an explicit write-back — the accessor diffs and flushes
  them itself, as in C++.
- **The pipeline binary is its own runner.** The same `main` calling `pipeline.Run()` is both
  the companion served inside the worker job and the launcher run on the dev host: with no Flow
  env vars set it parses `--config`/`--flow-bin`, enriches the spec and execs `flow_server`.
  That is why `pipeline_go.yson.template` is *smaller* than the Python variant's: no `streams`
  block (the schemas registered with `pipeline.AddStreams` are injected into `spec/streams`),
  no `entrypoint` in the `CompanionManager` parameters, no `local_files`, no worker
  `port_count`. Verified against the live operation spec — the runner did all of it unaided:
  the worker task ran with `port_count: 3` and a `go_companion` file entry pointing at the
  uploaded pipeline binary, and the extended spec carried
  `entrypoint = {executable = "./go_companion"}` + `run_process = %true`.

Everything runs under its own Cypress root, `$YT_DEV_ROOT/key_visitor_go`;
`companion_go/{yt_sync,prepare_data,verify}.py` are the same bootstrap/seed/assert scripts
pointed at that root.

Adaptations, stated explicitly — the asserts are unchanged:

- **The Go Flow SDK is not in a tagged `go.ytsaurus.tech/yt/go` release yet** (checked at
  v0.0.33: `module ... found, but does not contain package go.ytsaurus.tech/yt/go/flow`), so
  `go.mod` replaces the module with a sibling source checkout of
  `github.com/ytsaurus/ytsaurus` (clone it next to this repo, or repoint with
  `go mod edit -replace`).
- **`./run.sh` does not fit the Go route** — it execs `$FLOW_BIN --config <spec>`, while the Go
  runner is the pipeline binary itself and needs `--flow-bin` on top. The template is rendered
  with the same one-liner and the binary is launched directly (below).
- The per-run visit count differs again (30 output rows here vs 38/27); the 1..2 `visit_index`
  spread and every assert are the same.

The visit logic is proven offline first: `companion_go/main_test.go` drives the computation
through `flowtest.Harness` (message→state, visit→emission, unseeded-key silence, the
v1-visit-v2-visit supersession, counter survival across payload updates) — no cluster needed.

Run, from the repo root:

```bash
key_visitor/companion_go/build.sh       # go build; GO="ya tool go" if there is no system go
(cd key_visitor/companion_go && ${GO:-go} test ./...)  # offline visit-logic tests

python3 key_visitor/companion_go/yt_sync.py       # once: Cypress objects under key_visitor_go/
python3 key_visitor/companion_go/prepare_data.py  # 20 keys as v1, then the same 20 as v2

cd key_visitor
SCENARIO_DIR="$PWD" python3 -c 'import os, string, sys; sys.stdout.write(string.Template(sys.stdin.read()).substitute(os.environ))' \
    < pipeline_go.yson.template > pipeline_go.yson
./companion_go/key_visitor_go --config pipeline_go.yson \
    --flow-bin ~/ytsaurus/yt/yt/flow/bin/flow_server/flow_server.stripped
                                        # execs flow_server; returns when the pipeline completes

python3 companion_go/verify.py
cd .. && ./stop.sh key_visitor_go       # aborts the vanilla operation
```

On this demo cluster (4/9 data nodes), run the erasure-codec workaround right after
`yt_sync.py` and before deploying: set `@erasure_codec = none` and `@hunk_erasure_codec = none`
on every table under `$YT_DEV_ROOT/key_visitor_go` (queues, consumer and all pipeline system
tables) and remount each. Without it table writes stall hunting for erasure part replicas; with
sync unmount some empty system tables still hang in `unmounting` and need
`yt unmount-table --force` before remounting.

Recorded from the live run on the demo cluster, server build `26.2.0-local-os~5c69dd1804e43fe5`,
first deploy; the runner launched at 16:14:05 and printed `Pipeline completed` at 16:15:46 —
about 100 seconds end to end:

```
$ python3 companion_go/verify.py
ok: pipeline reached `completed`
ok: all 20 seeded keys were visited
ok: the latest visit of every key carries the v2 payload
output rows: 30; per-key max visit_index range: 1..2
OK: the final key-visitor pass swept the post-completion state of every key
```

## Java companion variant

`companion_java/` re-runs the same scenario with the visit tester written in **Java**, hosted
out-of-process by the **stock** `flow_server` through the same companion protocol as the Python
and Go variants. The topology, choreography and asserts are identical:

- `companion_java/src/main/java/.../VisitTester.java` — the visit tester with the Flow Java SDK
  (`tech.ytsaurus.flow`, modules `flow-core`/`flow-runner`): a `RowFunction` whose `onMessage`
  stores the payload in the per-key internal state `user_state`
  (`StateDescriptors.yson("user_state", UserState.class)`, an `@Entity` POJO) and whose
  `onVisit` emits the stored payload with the incremented per-key `visit_index`. Unlike Go,
  state mutations do **not** auto-flush — every change ends with an explicit `accessor.set(...)`,
  as in the word_count example.
- **The pipeline entry point is also the runner**, like Go: `KeyVisitorMain` calls
  `FlowApplication.run(args, context)`, which serves the companion when the worker exports
  `YT_FLOW_MODE` and otherwise launches the pipeline (`--config`/`--flow-bin`), enriching the
  spec and execing `flow_server`. The runner ships every jar it finds on `java.library.path`
  into the worker's `local_files` under `java_companion/` and completes the
  `TJavaCompanionManager` resource (`classpath = "java_companion/*"`), so the launch script
  must point `-Djava.library.path` at the collected classpath directory
  (`companion_java/build/companion-libs`, produced by the `collectRuntime` Gradle task).

Adaptations, stated explicitly — the asserts are unchanged:

- **The Flow Java SDK is not on Maven Central yet** (checked: no `tech.ytsaurus:flow-core`
  artifact, and the checkout's flow modules carry no `maven-publish` config either), so
  `companion_java/settings.gradle.kts` composite-includes a sibling source checkout of
  `github.com/ytsaurus/ytsaurus` and substitutes the `tech.ytsaurus:flow-*` coordinates with
  its subprojects — the Java equivalent of the Go variant's `go.mod` `replace`.
- **JDK delivery into the job**: by default the Java runner mounts internal JDK *porto layers*,
  which do not exist on this cluster (its exec nodes run a CRI job environment). The overrides
  the SDK provides for its own local tests do the job here too: `YT_FLOW_JDK_LAYERS='[]'` drops
  the layers and `YT_FLOW_JDK_BIN_PATH` points at the java binary of the worker task's
  `docker_image` — `docker.io/library/eclipse-temurin:17-jre` in the template (the registry
  prefix is required: a bare `eclipse-temurin:17-jre` is resolved against the cluster's Cypress
  image registry and fails the operation).
- **`vanilla/controller` must be spelled out** (`count = 1`, the C++ launcher's own default):
  the Java runner unconditionally creates the `controller` map while patching JDK layers, and
  an empty map fails `flow_server` config parsing on the missing required `count`.
- Unlike the Go runner, the Java one does not bump the worker `port_count` for the companion
  port; the template sets `port_count = 3` explicitly (on this cluster the C++ launcher's
  no-network-project default would also cover it).

The visit logic is proven offline first: `companion_java/src/test/.../VisitTesterTest.java`
drives `Computation.doProcess` with hand-built requests (message→state, visit→emission,
unseeded-key silence, the v1-visit-v2-visit supersession, counter survival across payload
updates) — no cluster needed. The SDK's `flow-test-utils` harness cannot inject visits yet, so
the test builds `RequestContext`s directly.

Everything runs under its own Cypress root, `$YT_DEV_ROOT/key_visitor_java`;
`companion_java/{yt_sync,prepare_data,verify}.py` are the same bootstrap/seed/assert scripts
pointed at that root. On this demo cluster, run the erasure-codec workaround right after
`yt_sync.py` (see the Go section).

Run, from the repo root:

```bash
key_visitor/companion_java/build.sh     # gradle test + collectRuntime (JDK 17+; uses ../ytsaurus/gradlew)

python3 key_visitor/companion_java/yt_sync.py       # once: Cypress objects under key_visitor_java/
python3 key_visitor/companion_java/prepare_data.py  # 20 keys as v1, then the same 20 as v2

key_visitor/companion_java/run.sh       # renders the template, launches the runner, returns on completion

python3 key_visitor/companion_java/verify.py
./stop.sh key_visitor_java              # aborts the vanilla operation
```

Recorded from the live run on the demo cluster, flow core build `baaaeedb` (the commit hash the
server logs at startup); the runner launched at 23:57:50 and printed `Pipeline completed` at
00:00:29 — about 160 seconds
end to end, with `verify.py` passing on its first run:

```
$ python3 key_visitor/companion_java/verify.py
ok: pipeline reached `completed`
ok: all 20 seeded keys were visited
ok: the latest visit of every key carries the v2 payload
output rows: 25; per-key max visit_index range: 1..2
OK: the final key-visitor pass swept the post-completion state of every key
```
