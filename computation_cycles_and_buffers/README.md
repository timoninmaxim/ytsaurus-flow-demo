# computation_cycles_and_buffers

A **cycle** in the computation graph — a stream that closes back into a computation upstream of
itself — plus the buffering that makes a cycle survivable. All six computations run **outside**
`flow_server`, in one C++ companion process (`companion/main.cpp`); the pipeline binary is the
stock `flow_server`.

```
reader (NCompanion::TSwiftOrderedSourceCompanionComputation over TQueueSource, finite)
   → reader_output → transform_a (NCompanion::TTransformCompanionComputation)
                        → ta1 → swift_map_a (NCompanion::TSwiftMapCompanionComputation)
                        → sa1 → transform_b (NCompanion::TTransformCompanionComputation)
                        → tb1 → swift_map_b (NCompanion::TSwiftMapCompanionComputation)
                        → sb1 → transform_a          <-- the cycle closes here
                        → ta2 → reducer (NCompanion::TTransformCompanionComputation)
                                   → external state /state → state table
```

`transform_a` is entered twice by every message, and it routes by input stream:
`reader_output → ta1` sends the message once around the loop, `sb1 → ta2` releases it to the
reducer on the way back. The two swift maps sleep 6 ms and 12 ms per message so the loop is slow
enough for its buffers to fill. The reducer adds the size of each batch it sees to a per-key count
in an external state table.

The input is 1000 identical rows (`data = "payload"`), so there is exactly one grouping key. The
assertion is therefore sharp: the state table must end with **one row whose `count` is exactly
1000**. Anything lost in the cycle undercounts, anything replayed overcounts — though, the rows
being indistinguishable, one loss compensating one duplication would pass unseen. Upstream's
assertion has the same blind spot.

The source is finite: it reads the queue to its end and the pipeline reaches `completed` on its
own.

## What this scenario is really for

`word_count_sync` established the C++ companion; read its README first — spec wiring, binary
delivery, `port_count` and the version bar all apply here unchanged. Its naming rules apply with
**one refinement**, below: it says a function declared with `AddTransform` must be hosted by
`TTransformCompanionComputation`, and this scenario shows that `TSwiftMapCompanionComputation` is
equally correct for such a declaration. This scenario answers the two questions it left open, and
adds one host class:

| Question | Answer |
|---|---|
| Does a graph **cycle** survive the companion shims? | Yes. A cycle is spec-level stream topology (`input_stream_ids` / `output_stream_ids` / `streams_dependency`); the companion is handed one computation's batch at a time and never routes anything between computations, so the topology stays where it always was |
| Does `TSimpleExternalStateManager` work **inside** a companion? | Yes. `initContext->InitExternalStateClient(client, "/state")` in the companion, `external_state_managers` in the spec, exactly as in-process |
| Does `NCompanion::TSwiftMapCompanionComputation` work? | Yes, and it is declared companion-side with **`AddTransform`** — see below |

**There is no `AddSwiftMap`.** `TPipeline` declares *computations* with `AddSource` and
`AddTransform` only (`AddResource` declares resource classes, not computations), and the kind it
records is advertised in `CompanionInfo` but never cross-checked against the spec's host class. So
a swift map is declared with `AddTransform` and hosted by
`NCompanion::TSwiftMapCompanionComputation`; the host class alone decides that the output is not
materialized and is recomputed deterministically after a restart. Keep the function deterministic —
nothing enforces it.

**One function type can back several computations.** `TCyclePassthroughFunction` is registered four
times, under four computation ids, and differs only in `processing_function_parameters`.
`TPipeline::AddTransform` registers the *type* once per process and the *id* once each, so this is
supported by design; the computation id is what the spec matches on.

Everything a companion pipeline needs is still: `processing_function` naming the C++ type,
`required_resource_ids` listing `CompanionManager` on **every** companion computation, the binary
named once in the `CompanionManager` resource's `entrypoint/executable`, delivered by
`vanilla/worker/local_files`, and `vanilla/worker/port_count = 3`.

## Run — C++ companion (source build)

**Not covered by the 0.2.1 artifact run.** This variant needs a source build (its own C++
companion, `companion/main.cpp`, plus a `flow_server` from the same checkout) and is not verified
against the released artifacts on this repo's CI — the steps below document the source-build
procedure as-is, unverified against 0.2.1. The Java-companion variant further down runs entirely
on the released artifacts and is verified there.

From this dir, with `YTSAURUS` pointing at a checkout set up for `ya make` (see `build.sh`):

```bash
./build.sh                       # builds + strips the companion into computation_cycles_companion.stripped
python3 yt_sync.py                # once: pipeline node, queue + consumer, state table
jinjanate pipeline.yson.j2 > pipeline.yson   # every {{ VAR }} from the env; an unset one fails the render

python3 -c 'import json, sys
for _ in range(1000):
    sys.stdout.write(json.dumps({"data": "payload", "$$tablet_index": 0}) + "\n")' \
  | yt insert-rows --format json "$YT_DEV_ROOT/computation_cycles_and_buffers/input_queue"

"$YTSAURUS/yt/yt/flow/bin/flow_server/flow_server.stripped" --config pipeline.yson
```

The launch returns on its own when the pipeline completes — budget about two and a half minutes.
Point the last command at a **stripped** `flow_server` built from the same checkout: the runner
uploads that exact file on every deploy, and the unstripped build it defaults to is gigabytes.

Then check the state table, and finally `./stop.sh computation_cycles_and_buffers` (from the repo
root) to abort the vanilla operation (the pipeline is already `completed`, a final state, so there
is nothing to stop):

```bash
yt flow get-pipeline-state "$YT_DEV_ROOT/computation_cycles_and_buffers/pipeline"
yt select-rows "* from [$YT_DEV_ROOT/computation_cycles_and_buffers/state]" --format json
```

## Observed output

Recorded against a locally built server — the companion classes are newer than every release, so
the exact build is part of the observation:

```
flow_server: 26.2.0-local-os~5c69dd1804e43fe5
```

The launch ends with, and exits 0 on (cluster URL and Cypress root elided):

```
I	FlowClient	Pipeline completed (Pipeline: <…>/computation_cycles_and_buffers/pipeline)
```

```
$ yt flow get-pipeline-state "$YT_DEV_ROOT/computation_cycles_and_buffers/pipeline"
completed

$ yt select-rows "* from [$YT_DEV_ROOT/computation_cycles_and_buffers/state]" --format json
{"hash":8436339620933999394,"data":"payload","count":1000}
```

One row, `count == 1000` — the upstream assertion, met. Since the reducer's only input is `ta2`,
and `ta2` is produced only by the `sb1 → ta2` rule, every one of those 1000 counted messages had
already come back around the loop through `swift_map_a`, `transform_b` and `swift_map_b`.

Timings for that run (one worker, both binaries already in the cluster's file cache): launch
09:31:28 → pipeline `working` 09:31:52 → all ten jobs running 09:32:17 → `completed` 09:33:39. The
~25 s between `working` and the jobs running is the companion process starting; while it is
starting the controller logs one round of

```
W	PublicFlowController	Received worker error (Component: /resource_manager/resources/CompanionManager/common_companion_client/operations/GetCompanionInfo, WorkerAddress: …)
failed to connect to all addresses; last error: UNKNOWN: ipv4:0.0.0.0:24582: Failed to connect to remote host: Connection refused
```

which is the worker reaching the companion's port before the companion has bound it. A *repeating*
one means the companion died.

A healthy launch of this pipeline is noisy. None of the following is a problem, and none of it is
explained anywhere; everything below was read out of run 3's `controller_logs` queue
(`<pipeline>/controller_logs`, plain text rows — `yt flow show-logs` serves the same content while
the controller is alive). Whether the launch's streamed output shows the controller-side lines
depends on when the runner attaches: it caught them in one run and missed them in another.

Runner side, before anything is deployed:

- six `E SimpleRunner Found specs parseability error / Unknown processing function "…"`, one per
  companion-hosted computation. `flow_server` cannot resolve names that only exist in your
  companion; they are resolved inside it. `abort_on_specs_parseability_error` defaults to `%false`,
  so nothing is refused — this spec does not set the flag at all.
- ~28 × `E FlowClient Failed to update pipeline` over ~15 s — the runner retrying the spec update
  until the controller it has just launched answers.

Controller side, in its first ~10 s (06:38:54–06:39:05 of run 3):

- four `W Component became broken`: `/build_cache`, `/collect_feedback` and `/update_metrics` with
  the inner error `FlowViewKeeper is not initialized`, and `/schedule` with `Cannot read from
  tablet … of table …/flow_state_obsolete while it is in "unmounted" state`.
- six `E PublicFlowController` lines, two of each: `Scheduler Executor thread failed and
  restarted`, `Failed to confirm leader_controller_address`, and `Found new retryable errors in
  controller (Component: /schedule)`. All carry the same `flow_state_obsolete`/`FlowViewKeeper`
  causes. They stop by themselves and the pipeline reaches `working` normally.

Then, while the jobs start:

- `W PublicFlowController Some computations has partial traverse coverage (Computations: […all
  six…])` every 5 s for ~25 s. It stops once the jobs run.

## The buffers half: cutting the buffers mid-flight

The upstream test also runs the pipeline with the buffers cut in the middle of the flight, and
asserts the same exact count afterwards. It cuts them by pausing and restarting the pipeline
(upstream's own comment attributes the cut to the restart zeroing the stream demands; that
mechanism is not something this scenario verifies — what it verifies is the count). Reproduce it
by pausing while the count is still far from 1000:

```bash
# in a second terminal, with the launch streaming in the first
P="$YT_DEV_ROOT/computation_cycles_and_buffers/pipeline"
yt select-rows "count from [$YT_DEV_ROOT/computation_cycles_and_buffers/state]" --format json
yt flow pause-pipeline "$P"
until [ "$(yt flow get-pipeline-state "$P")" = "paused" ]; do sleep 2; done
yt flow start-pipeline "$P"
```

Observed: paused with the state table at

```
{"hash":8436339620933999394,"data":"payload","count":15}
```

— 15 of 1000 messages committed, the rest in flight or buffered inside the cycle. After
`start-pipeline` the pipeline went back to `working` and completed two minutes later with

```
{"hash":8436339620933999394,"data":"payload","count":1000}
```

Exactly-once held across the cut. The pipeline also does not complete in the first 30 s, which is
the upstream test's own precondition for this variant — the sleeps and the buffer guarantees in
`dynamic_spec/job_tracker/buffer_state_manager` are what keep it slow enough to catch.

**Check the count before you pause.** The window is short: a first attempt here paused at about
60 s and the state table already read `count = 1000`, so the buffers were empty and the variant
proved nothing. If that happens, recreate the scenario (below) and pause earlier — polling the
count every three seconds and pausing as soon as it is non-zero is enough.

## Rerunning

`completed` is a final state that refuses both `stop-pipeline` and a spec update, and the input
queue's consumer cannot be rewound, so a repeat run means recreating the scenario:

```bash
(cd .. && ./stop.sh computation_cycles_and_buffers)
yt remove -r "$YT_DEV_ROOT/computation_cycles_and_buffers"
python3 yt_sync.py   # may need a second run: consumer registration races master lag
# re-insert the 1000 rows, then re-launch as in "Run" above
```

Both papercuts bite here: `yt remove` failed with `Cannot take "exclusive" lock …
leader_controller_lock` for the first two attempts after the abort, and `yt_sync.py` failed once
with `Error resolving path #… No such object` from `register_queue_consumer` and succeeded on the
retry.

## Java companion variant

`companion_java/` re-runs the scenario with all six computations written in **Java**
(`tech.ytsaurus:flow-*`, the Flow Java SDK), hosted by the same stock `flow_server` through the
same four companion host classes — the host-class mapping of the C++ variant carries over
computation by computation: `TSwiftOrderedSourceCompanionComputation` drives `ReadData`,
`TTransformCompanionComputation` drives the two transforms and the reducer, and
`TSwiftMapCompanionComputation` drives the two swift maps — the first Java port to put functions
on the swift-map path of a cycle. The cycle needs nothing from the SDK at all: it is spec-level
stream topology (`input_stream_ids` / `output_stream_ids` / `streams_dependency`), and
`pipeline.yson.j2` carries it over unchanged; `transform_a` routes by
`message.getStreamId()`, exactly the C++ variant's passthrough rules. `ComputationCyclesMain`
registers `CyclePassthrough` four times under four computation ids, the Java counterpart of
registering `TCyclePassthroughFunction` four times. Everything runs under its own root
`$YT_DEV_ROOT/computation_cycles_java`.

Everything comes from the Flow 0.2.1 test release (see the repo README's "Released artifacts");
nothing here is built from source. One entry point serves both roles: `ComputationCyclesMain` main
registers the six computations and hands over to `FlowApplication.run`, which the Flow Java SDK
launcher drives — it enriches the spec (ships `lib/*` as the companion's classpath, fills stream
schemas and companion resources) and execs the image's own `flow_server` (`YT_FLOW_BIN`).
`TJavaCompanionManager` names only `main_class` — no `jdk_bin_path`, no `classpath`: the launcher
ships the classpath jars and takes the job java from its own `java.home`. Both vanilla tasks
(`controller` and `worker`) run in `ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1` (this cluster has
no porto layers, so a docker image carrying the JDK is required), and `port_count = 3`
(worker RPC + monitoring + the companion gRPC port).

Adaptations against the C++ variant, stated explicitly — the asserts are unchanged:

- **The routing tables travel in the spec's `parameters`, not in
  `processing_function_parameters`.** The Java SDK reads user configuration via
  `ctx.getComputationParameters()`, which serves `computations/<id>/parameters` — so
  `passthrough_rules` and `sleep_per_message` sit next to `processing_mode`, and the runner logs
  one startup `E SimpleRunner Found specs parseability error — Static spec has unrecognized
  fields` naming exactly those fields on all four cycle computations. As in the Go variant it is
  logged unconditionally and refuses nothing; the parameters do reach the companion — the routing
  proves it. `processing_function` is omitted too: the Java SDK dispatches by computation id.
- **The reducer groups its batch itself.** As in the Python and Go variants — and unlike the C++
  keyed-batch adapter, whose `ProcessKey` is called per key — `onMessages` gets the request's
  whole batch with keys mixed. The reducer groups by key in first-appearance order
  (a `LinkedHashMap`, never map-hash order) and opens `/state` once per group; there is
  effectively one key in this scenario, so the group is the batch.
- **"No count yet" is not an absent state row.** Live, the state manager hands the companion a
  present row with the key columns set and `count` null, so the null check is per column:
  `getOrDefault()` folds the truly-absent case into an all-null row of the state schema, and
  `row.get("count", Long.class) == null` is the direct translation of the C++
  `optional<i64>.value_or(0)`. `testReducerToleratesNullCount` pins that shape offline.
- **A missing passthrough rule throws `IllegalStateException`**, the port of the C++ variant's
  throw — with the same retried-forever caveat.
- **The SDK and the server come from the Flow test release, not from a source checkout.**
  `build.gradle.kts` resolves `tech.ytsaurus:flow-*` from Maven: released versions from Maven
  Central, test releases (`X.Y.Z-SNAPSHOT`) from the Sonatype snapshot repository; the version is
  `-PflowVersion` (default `0.2.1-SNAPSHOT`). The vanilla tasks and the launcher itself run in
  `ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1` (the released server image plus a JRE at
  `/opt/java/openjdk`), so one image holds both the `flow_server` the jobs run and the `java` the
  launcher and the companion run with.

### Build

Built with the official Gradle toolchain container, never a local or Arcadia-built JDK/Gradle,
from `companion_java`:

```bash
cd companion_java
podman run --rm -v "$PWD:/src" -w /src docker.io/library/gradle:8-jdk17 \
    gradle -q --refresh-dependencies test installLib -PflowVersion=0.2.1-SNAPSHOT
```

This resolves the SDK from the Sonatype snapshot repository (`build.gradle.kts`), runs the offline
tests (`ComputationCyclesTest`, below), and syncs the pipeline jar plus its runtime deps into
`lib/` (gitignored) — the classpath both the launch command and the Flow runner's companion-jar
shipping use.

### Run

The cycle logic is proven offline first: `ComputationCyclesTest` drives all six computations
through the SDK's `TestComputationHarness` (`flow-test-utils`) against a trimmed copy of the
pipeline spec — the routing rules of every computation (including transform_a sending a fresh
message around the loop and releasing a returned one), the missing-rule error, the reducer's
counting over fresh, seeded and null-count state, mixed-key grouping, and a 1000-message
simulation of the full cycle in batches of 30 ending at exactly `count == 1000` — no cluster
needed. The trimmed spec omits the live `sleep_per_message` values, which only pace the pipeline.
It runs as part of the Build step above.

From `companion_java`:

```bash
python3 yt_sync.py   # once: pipeline node, input_queue + consumer, state table, under computation_cycles_java/
jinjanate pipeline.yson.j2 > pipeline.yson   # every {{ VAR }} from the env; an unset one fails the render

python3 -c 'import json, sys
for _ in range(1000):
    sys.stdout.write(json.dumps({"data": "payload", "$$tablet_index": 0}) + "\n")' \
  | yt insert-rows --format json "$YT_DEV_ROOT/computation_cycles_java/input_queue"

podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" \
    ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1 -cp 'lib/*' \
    tech.ytsaurus.flow.demo.computationcycles.ComputationCyclesMain --config pipeline.yson
```

The launcher enriches the spec (companion classpath), uploads the released `flow_server` and
launches the controller+worker vanilla operation, then streams the controller log; it returns on
its own once the pipeline completes — this variant's `reader` is finite, budget about two and a
half minutes.

```bash
yt flow get-pipeline-state "$YT_DEV_ROOT/computation_cycles_java/pipeline"
yt select-rows "* from [$YT_DEV_ROOT/computation_cycles_java/state]" --format json
```

From the repo root: `./stop.sh computation_cycles_java` aborts the vanilla operation (`completed`
needs only the abort). To drop the scenario's Cypress objects as well:
`yt remove -r "$YT_DEV_ROOT/computation_cycles_java"`.

### Observed output

```
$ yt flow get-pipeline-state "$YT_DEV_ROOT/computation_cycles_java/pipeline"
completed

$ yt select-rows "* from [$YT_DEV_ROOT/computation_cycles_java/state]" --format json
{"hash":8436339620933999394,"data":"payload","count":1000}
```

One row, `count == 1000`, byte-identical to the C++ reference row including the hash — every one
of the 1000 messages came back around the loop exactly once. `vanilla/current_spec` confirms both
tasks ran `./flow_server` under `docker_image = ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1`, and
the worker's `file_paths` lists the launcher-shipped jars under `java_companion/`, including
`flow-server-0.2.1-SNAPSHOT.jar` — the released SDK, not a source build.
