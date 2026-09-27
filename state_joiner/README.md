# state_joiner

One computation reading **another computation's state**: an accumulator sums each user's amounts
into per-user state, and a joiner — a separate computation, with its own jobs and its own key
space — reads that state back and writes the totals out. Both process functions live in
`companion/main.cpp`, a separate binary the worker spawns inside its vanilla job and drives over
gRPC; the pipeline binary is the stock `flow_server`.

```
reader (stock TSwiftPassthroughOrderedSourceComputation over TQueueSource, finite)
   → events
accumulator (NCompanion::TTransformCompanionComputation)
   → state /user_total → user_totals table
   → users
joiner (NCompanion::TTransformCompanionComputation)
   → joins /user_total  (read-only)
   → results → NSortedDynamicTable::TSyncSink → output_table
```

The input is four rows, one per user (`user-0…user-3` with amounts 10, 20, 30, 40), so after the
join `output_table` must contain exactly `(UserId, Total)` = the input amounts. The source is
finite: it reads the queue to its end and the pipeline reaches `completed` on its own.

## The subject: joining state

Flow has **three** ways for a computation to reach per-key state, and they are easy to confuse
because they are all declared next to each other in the computation spec:

| Spec block | Client type | What it reads | Writable |
|---|---|---|---|
| `external_state_managers` | `TMutableStateKeyClient` | your own YT dynamic table, through a registered manager class | yes |
| `external_state_joiners` | `TJoinedStateKeyClient` | *someone's* YT dynamic table, through a registered joiner class | no |
| `state_joiners` | `TJoinedStateKeyClient` | another **computation's framework-managed internal state**, no class and no path — you name the computation id and the state name | no |

A **state joiner** is the third row. It exists because a computation's internal state (the one you
get from `initContext->InitClient(client, "total")`) has no user-visible table: the framework keeps
it in the pipeline's own `states` table, keyed by `(computation_id, key, name)`. A state joiner is
how a *different* computation reads those rows — it names `computation_id` + `state_name` and,
optionally, a `key_schema_override` that maps the reader's own columns onto the target's group-by
key. So the difference from an external state manager is not "read-only" but *whose* storage is
being read and who owns its layout: an external state manager is your table with your schema; a
state joiner is a peer computation's private state, which you can read but never write.

Nothing forces the two computations to share a key. The accumulator here groups by
`(farm_hash(UserId), UserId)`; the joiner could group by anything (upstream's second variant groups
by a constant `Bucket`, which is what the unused `Bucket` column in the `users` stream is for) and
name the mapping in `join_on/key_schema_override`.

Flink has no direct equivalent. Keyed state there is private to its operator, and reading another
operator's state at runtime is not expressible — you either route the data (a `connect`/`join` with
a broadcast or keyed stream) or park it in an external store. Flow's state joiner is closer to
Kafka Streams' *global store* read from another processor. A migrating Flink user should read
`state_joiners` as "read another operator's keyed state directly, without shipping it through a
stream".

## What this scenario found: `state_joiners` does not survive the companion

**`state_joiners` cannot be used from a companion process at all.** This scenario was first written
as a literal port — the accumulator keeping `total` as internal state (`InitClient(TotalClient_,
"total")`, `parameters/internal_states = ["total"]` on the host) and the joiner declaring

```yson
"state_joiners" = {
    "/user_total" = {
        "computation_id" = "accumulator";
        "state_name" = "/total";
        "join_on" = {};
    };
};
```

and calling `initContext->InitClient(TotalJoiner_, "user_total")`. It deploys, the accumulator half
works, and every joiner batch then fails:

```
W  PublicFlowController  Received job retryable error (Component: /operations/DoProcess, ComputationId: joiner)
Internal state joiners are not available in a companion process
    method          ProcessBatch
    service         NYT.NFlow.NProto.NCompanion.CompanionService
```

The pipeline stays `working` and retries forever (an exception in a companion is retryable — see
`word_count_sync`'s README), so the only way out is aborting the vanilla operation.

That message comes from `library/cpp/companion/server/runtime_init_context.cpp`, and the whole
mechanism is missing on both sides of the gRPC contract: the worker-side host
(`companion/transform_companion_computation.cpp`) ships internal states, external states and
*external* joined states with each batch, and nothing else. So the feature is unavailable to any
out-of-process language — C++, Python or Java.

The accumulator half does work, and it is worth seeing, because it is the storage a state joiner
reads. With the literal port deployed, the pipeline's own `states` table holds:

```
$ yt select-rows "computation_id, key, name, state from [$YT_DEV_ROOT/state_joiner/pipeline/states]" --format json
{"computation_id":"accumulator","key":[215895921132288444,"user-2"],"name":"/total","state":{"payload":"{\u0001\ntotal=\u0002<;}"}}
{"computation_id":"accumulator","key":[13410328023676382545,"user-0"],"name":"/total","state":{"payload":"{\u0001\ntotal=\u0002\u0014;}"}}
{"computation_id":"accumulator","key":[14023215990766783017,"user-1"],"name":"/total","state":{"payload":"{\u0001\ntotal=\u0002(;}"}}
{"computation_id":"accumulator","key":[17032180724435400857,"user-3"],"name":"/total","state":{"payload":"{\u0001\ntotal=\u0002P;}"}}
```

The payloads are binary YSON: `\u0002` marks an int64 and the byte after it is a zigzag varint, so
they read `total = 10, 20, 30, 40`. Internal state in a companion is fine — only *joining* someone
else's is not.

### What this scenario ships instead

The same shape, moved one row up the table: the accumulator's total lives in an **external state
manager** over the `user_totals` table, and the joiner reads it with an **external state joiner**
over the same table. The pipeline is the same graph, the assertion is unchanged, and the property
being demonstrated — a second computation reading the state a first computation wrote, in the same
pipeline, with the epoch guarantees intact — survives. What is lost is the part that makes a state
joiner cheap: you now have to create and own a table, keep its schema in step with the accumulator's
group-by key, and pay a lookup against it.

Ordering is not a worry in either form. The accumulator's state write and its output message land
in the *same* epoch transaction, so by the time a `users` message reaches the joiner the total it
refers to is already committed. That is why the assertion is exact rather than eventually exact.

### And a crash: `key_schema_override` in a companion kills the worker

Upstream's second variant groups the joiner by `Bucket` and maps the join key with
`join_on/key_schema_override`. Deployed here with the external state joiner, it does not just fail
to join — it **takes down the whole `flow_server` worker process**, repeatedly:

```
SIGILL (Illegal instruction) ... received by PID 137
 1. AssertTrapImpl(...)
 2. GetIteratorOrCrash<THashMap<TKey, TIntrusivePtr<TStateHolder<TSimpleExternalState>>>>(...)
 3. NYT::NFlow::TSimpleExternalStateJoiner::GetState(TKey const&)
 4. NYT::NFlow::TJoinedStateKeyClient<TSimpleExternalState>::GetState(TKey const&)
 5. …
 6. NYT::NFlow::NCompanion::TTransformCompanionComputation::DoProcess(...)
 …
/bin/bash: line 1:   137 Illegal instruction     (core dumped) ./flow_server --config node_config
```

followed by `Job is lost because worker is lost` for every job on that worker and
`Too few workers in worker group (Count: 0, Required: 1)` from then on. Reading the two host
classes explains the stack: `transform_ordered_source_companion_computation.cpp` calls
`stateClient.ResolveKey(message)` before fetching the joined state for the batch, while
`transform_companion_computation.cpp` passes `message->Key` — the computation's own key — straight
into `GetState`, and with an override those are different keys. `TSimpleExternalStateJoiner`
answers an unexpected key with `GetOrCrash`, i.e. by aborting the process rather than by returning
an empty state.

So, with a companion: no `state_joiners` at all, and `external_state_joiners` only without
`key_schema_override`. This scenario's spec therefore joins on the same key
(`"join_on" = {}`), and the joiner keeps the `Bucket` column only for fidelity to the upstream
stream schema.

Because a missing joined state is a real possibility and an exception in a companion is retried
forever, `TJoinerFunction` writes `Total = -1` rather than throwing. Two different misses collapse
into that sentinel:

- **no row for the key in `user_totals`.** This is the one that actually happens. The preload keeps
  missing rows (`LookupRowsOptions::KeepMissingRows = true` in
  `computation/simple_external_state_manager.cpp`), so the key still gets a state — an all-null row
  of the full width (`common/payload.cpp`) — and the host ships it. The accessor is *initialized*;
  it is `GetColumnValue<std::optional<i64>>("Total")` that comes back empty, hence `.value_or(-1)`.
- **no joined state for the key in the batch at all**, which surfaces as an uninitialized accessor.
  Unreachable today: the host adds a joined state for every key it processes, and the one case that
  would produce a mismatched key — `key_schema_override` — aborts the worker before any of this
  runs (below).

In the run below no `-1` appears, which is itself part of the assertion.

## Run — C++ companion (source build)

**Not covered by the 0.2.1 artifact run.** This variant needs a source build (its own C++
companion, `companion/main.cpp`, plus a `flow_server` from the same checkout) and is not verified
against the released artifacts on this repo's CI — the steps below document the source-build
procedure as-is, unverified against 0.2.1. The Java-companion variant further down runs entirely
on the released artifacts and is verified there.

`word_count_sync`'s README is the companion reference — spec wiring, binary delivery and the
version bar all apply here unchanged; `computation_cycles_and_buffers`
adds the multi-computation notes. This scenario adds three facts:

- `IBatchProcessFunction` (the whole-epoch granularity, used by the joiner) is hosted by the
  companion just like `IProcessFunction` and `IKeyedBatchProcessFunction`;
- one companion binary can back computations that use different state facilities — a mutable
  external state manager in one, a read-only joiner in another;
- an external state manager works against an **empty** table on the very first batch, which looks
  like it should not: the state schema is not known to the companion by itself
  (`TPayloadBuilder(state->Schema)` needs it), but the worker-side host fetches a state for every
  key of the batch and ships it *with the table's schema attached*, so a fresh key arrives as an
  all-null row of the right width. No pre-seeding of the state table is needed.

From this dir, with `YTSAURUS` pointing at a checkout set up for `ya make` (see `build.sh`):

```bash
./build.sh                       # builds + strips the companion into state_joiner_companion.stripped
python3 yt_sync.py                # once: pipeline node, queue + consumer, user_totals, output_table
jinjanate pipeline.yson.j2 > pipeline.yson   # every {{ VAR }} from the env; an unset one fails the render

python3 -c 'import json, sys
for i, amount in enumerate([10, 20, 30, 40]):
    sys.stdout.write(json.dumps({"UserId": "user-%d" % i, "Amount": amount, "$$tablet_index": 0}) + "\n")' \
  | yt insert-rows --format json "$YT_DEV_ROOT/state_joiner/input_queue"

"$YTSAURUS/yt/yt/flow/bin/flow_server/flow_server.stripped" --config pipeline.yson
```

The launch returns on its own when the pipeline completes — budget about two minutes. Point the
last command at a **stripped** `flow_server` built from the same checkout: the runner uploads that
exact file on every deploy, and the unstripped build it defaults to is gigabytes.

Then check the output, and finally `./stop.sh state_joiner` (from the repo root) to abort the
vanilla operation (the pipeline is already `completed`, a final state, so there is nothing to
stop):

```bash
yt flow get-pipeline-state "$YT_DEV_ROOT/state_joiner/pipeline"
yt select-rows "UserId, Total from [$YT_DEV_ROOT/state_joiner/output_table]" --format json
yt select-rows "UserId, Total from [$YT_DEV_ROOT/state_joiner/user_totals]" --format json
```

## Observed output

Recorded against a locally built server — the companion classes are newer
than every release, so the exact build is part of the observation:

```
flow_server: 26.2.0-local-os~5c69dd1804e43fe5
```

The launch ends with, and exits 0 on (cluster URL and Cypress root elided):

```
I	FlowClient	Pipeline completed (Pipeline: <…>/state_joiner/pipeline)
```

```
$ yt flow get-pipeline-state "$YT_DEV_ROOT/state_joiner/pipeline"
completed

$ yt select-rows "UserId, Total from [$YT_DEV_ROOT/state_joiner/output_table]" --format json
{"UserId":"user-2","Total":30}
{"UserId":"user-0","Total":10}
{"UserId":"user-1","Total":20}
{"UserId":"user-3","Total":40}

$ yt select-rows "UserId, Total from [$YT_DEV_ROOT/state_joiner/user_totals]" --format json
{"UserId":"user-2","Total":30}
{"UserId":"user-0","Total":10}
{"UserId":"user-1","Total":20}
{"UserId":"user-3","Total":40}
```

`output_table` matches the upstream test's assertion: `(UserId, Total)` sorted is
`[(user-0, 10), (user-1, 20), (user-2, 30), (user-3, 40)]`, the input amounts. The two tables agree
because every total the joiner wrote came out of the state the accumulator wrote — and no row is
`-1`, so every join hit.

The pipeline's own `states` table is empty in this variant, which is the flip side of the finding
above: no computation here keeps framework-managed internal state any more.

```
$ yt select-rows "computation_id, name from [$YT_DEV_ROOT/state_joiner/pipeline/states]" --format json
(no rows)
```

Timings for that run (one worker, both binaries already in the cluster's file cache): launch
10:47:42 → pipeline `working` 10:48:16 → all five jobs running 10:48:31 → `completed` 10:49:21. The
usual companion-startup noise appears once (`GetCompanionInfo` … `Connection refused` while the
companion binds its port, `partial traverse coverage` every 5 s until the jobs run, and `E`-level
runner and controller lines — here two `Found specs parseability error`, 29 `Failed to update
pipeline` and one `/schedule` retryable error — that clear by themselves); `word_count_sync` and
`computation_cycles_and_buffers` document them in full.

## Rerunning

`completed` is a final state that refuses both `stop-pipeline` and a spec update, and the input
queue's consumer cannot be rewound, so a repeat run means recreating the scenario. **Unregister the
consumer before deleting it**:

```bash
(cd .. && ./stop.sh state_joiner)
yt unregister-queue-consumer "$YT_DEV_ROOT/state_joiner/input_queue" "$YT_DEV_ROOT/state_joiner/consumer"
yt remove -r "$YT_DEV_ROOT/state_joiner"
python3 yt_sync.py
# re-insert the four rows, then re-render and re-launch as in "Run" above
```

Deleting first and re-running `yt_sync.py` did not converge here: four runs in a row failed with
`Error resolving path #<id> / No such object <id>` out of `register_queue_consumer`, leaving the
pipeline node uncreated, and only an explicit `yt unregister-queue-consumer` (against the paths,
which still resolve) made the next run succeed — twice, on two separate recreates. This is the
sharp edge of the transient failure `word_count_sync` records as "just re-run it".

## Java companion variant

`companion_java/` re-runs the scenario with the accumulator and the joiner written in **Java**,
hosted by the stock `flow_server` through the same `TTransformCompanionComputation` host class;
the reader stays the stock C++ source computation, exactly as in the C++ variant. The topology,
the external-state restructuring and the assertion are unchanged: `StateJoinerMain` registers both
functions, `companion_java/pipeline.yson.j2` is the same graph under its own root
`$YT_DEV_ROOT/state_joiner_java`. One entry point serves both roles: `main` registers the two
computations and hands over to `FlowApplication.run`, which the Flow Java SDK launcher drives — it
enriches the spec (ships `lib/*` as the companion's classpath, fills stream schemas and companion
resources) and execs the image's own `flow_server` (`YT_FLOW_BIN`). `TJavaCompanionManager` names
only `main_class` — no `jdk_bin_path`, no `classpath`: the launcher ships the classpath jars and
takes the job java from its own `java.home`. Both vanilla tasks (`controller` and `worker`) run in
`ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1` (this cluster has no porto layers, so a docker image
carrying the JDK is required), and `port_count = 3` (worker RPC + monitoring + the companion gRPC
port).

What this variant demonstrates on top of the C++ one: **`external_state_joiners` is fully usable
from Java.** The worker-side host ships the joined states with each batch, and the SDK surfaces
them as `StateDescriptors.externalReadOnly("/user_total")` — resolved through the same
`ctx.getState(descriptor, message)` call as every other state kind, but returning a
`ReadOnlyExternalStateAccessor` whose `set()`/`clear()` throw `UnsupportedOperationException`:
the SDK making "joiners never write back" explicit rather than a missing feature. The joiner is a
`BatchFunction` — the Java counterpart of the `IBatchProcessFunction` granularity the upstream
test uses — and receives the request's whole message batch keys mixed, which is fine here because
the join key is the message's own key (`join_on = {}`) and no grouping is needed. The two state
facilities coexist in one companion process: the accumulator's mutable
`StateDescriptors.external(...)` next to the joiner's read-only descriptor, mirroring the C++
`TMutableStateKeyClient` / `TJoinedStateKeyClient` pair as three factory methods on one class.

Everything in "What this scenario found" above applies unchanged, because the limits live in the
worker-side host and the wire contract, not in the user's language: `state_joiners` is unavailable
to Java exactly as to a C++ companion, and `key_schema_override` under an external state joiner
would hit the same worker-killing `GetOrCrash` before any Java code runs. This spec therefore
joins on the same key, like the C++ variant.

Adaptations against the C++ companion, stated explicitly — the assertion is unchanged:

- **`processing_function` is omitted.** The Java SDK dispatches by `computation_id`
  (`registerComputation(... .setComputationId("accumulator") ...)`), so the spec does not name
  the functions.
- **The missing-join sentinel handles both miss shapes with one check.** The C++ joiner
  distinguishes an uninitialized accessor from an all-null row; in Java both collapse into
  `state.get()` returning `null` or the `Total` column reading `null`, so one null check covers
  the un-shipped-state case and the reachable miss — no row in `user_totals`, shipped as an
  all-null payload — alike, reported as `Total = -1` for the same reason (an exception in a
  companion is retried forever). No `-1` appears in the run below.
- **The join is unit-tested offline.** `StateJoinerTest` drives both computations through
  `flow-test-utils`' `TestComputationHarness`, seeding the joined state with
  `TestDoProcessRequest.setState(JoinedExternalStateDescriptor, key, payload)` — the harness runs
  the real gRPC request mappers, so the joined-state path in the test is the wire path. The
  all-null-row miss and the absent-state miss are both pinned to `-1` there.
- **The SDK and the server come from the Flow test release, not from a source checkout.**
  `build.gradle.kts` resolves `tech.ytsaurus:flow-*` from Maven: released versions from Maven
  Central, test releases (`X.Y.Z-SNAPSHOT`) from the Sonatype snapshot repository; the version is
  `-PflowVersion` (default `0.2.1-SNAPSHOT`). The vanilla tasks and the launcher itself run in
  `ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1` (the released server image plus a JRE at
  `/opt/java/openjdk`), so one image holds both the `flow_server` the jobs run and the `java` the
  launcher and the companion run with.
- **Both state accessors are read with `get()` / `getOrDefault()`.** `StateAccessor.get()` hands
  back the state row or `null`; `getOrDefault()` substitutes an all-null row of the state schema.
  The accumulator takes the second (a fresh key must still be written back with the right width),
  the joiner the first (a null row is one of the two miss shapes above).

### Build

Built with the official Gradle toolchain container, never a local or Arcadia-built JDK/Gradle,
from `companion_java`:

```bash
cd companion_java
podman run --rm -v "$PWD:/src" -w /src docker.io/library/gradle:8-jdk17 \
    gradle -q --refresh-dependencies test installLib -PflowVersion=0.2.1-SNAPSHOT
```

This resolves the SDK from the Sonatype snapshot repository (`build.gradle.kts`), runs the offline
tests (`StateJoinerTest`, above), and syncs the pipeline jar plus its runtime deps into `lib/`
(gitignored) — the classpath both the launch command and the Flow runner's companion-jar shipping
use.

### Run

From `companion_java`:

```bash
python3 yt_sync.py   # once: pipeline node, queue + consumer, user_totals, output_table, under state_joiner_java/
jinjanate pipeline.yson.j2 > pipeline.yson   # every {{ VAR }} from the env; an unset one fails the render

python3 -c 'import json, sys
for i, amount in enumerate([10, 20, 30, 40]):
    sys.stdout.write(json.dumps({"UserId": "user-%d" % i, "Amount": amount, "$$tablet_index": 0}) + "\n")' \
  | yt insert-rows --format json "$YT_DEV_ROOT/state_joiner_java/input_queue"

podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" \
    ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1 -cp 'lib/*' \
    tech.ytsaurus.flow.demo.statejoiner.StateJoinerMain --config pipeline.yson
```

The launcher enriches the spec (companion classpath), uploads the released `flow_server` and
launches the controller+worker vanilla operation, then streams the controller log; it returns on
its own once the pipeline completes — budget about two minutes.

```bash
yt flow get-pipeline-state "$YT_DEV_ROOT/state_joiner_java/pipeline"
yt select-rows "UserId, Total from [$YT_DEV_ROOT/state_joiner_java/output_table]" --format json
yt select-rows "UserId, Total from [$YT_DEV_ROOT/state_joiner_java/user_totals]" --format json
```

From the repo root: `./stop.sh state_joiner_java` aborts the vanilla operation (`completed` needs
only the abort). To drop the scenario's Cypress objects as well (unregister the consumer first, it
cannot be rewound): `yt unregister-queue-consumer "$YT_DEV_ROOT/state_joiner_java/input_queue"
"$YT_DEV_ROOT/state_joiner_java/consumer"` then `yt remove -r "$YT_DEV_ROOT/state_joiner_java"`.

### Observed output

```
$ yt flow get-pipeline-state "$YT_DEV_ROOT/state_joiner_java/pipeline"
completed

$ yt select-rows "UserId, Total from [$YT_DEV_ROOT/state_joiner_java/output_table]" --format json
{"UserId":"user-2","Total":30}
{"UserId":"user-0","Total":10}
{"UserId":"user-1","Total":20}
{"UserId":"user-3","Total":40}

$ yt select-rows "UserId, Total from [$YT_DEV_ROOT/state_joiner_java/user_totals]" --format json
{"UserId":"user-2","Total":30}
{"UserId":"user-0","Total":10}
{"UserId":"user-1","Total":20}
{"UserId":"user-3","Total":40}
```

Identical to the C++ variant's assertion: `(UserId, Total)` sorted is the input amounts, the two
tables agree, no row is `-1`, and the pipeline's own `states` table is empty — no computation in
this variant keeps framework-managed internal state either. `vanilla/current_spec` confirms both
tasks ran `./flow_server` under `docker_image = ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1`, and
the worker's `file_paths` lists the launcher-shipped jars under `java_companion/`.
