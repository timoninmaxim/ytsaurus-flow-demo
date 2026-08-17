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

## Run

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

From the repo root:

```bash
state_joiner/build.sh          # builds + strips the companion (YTSAURUS=<checkout>)
python3 state_joiner/yt_sync.py  # once: pipeline node, queue + consumer, user_totals, output_table

python3 -c 'import json, sys
for i, amount in enumerate([10, 20, 30, 40]):
    sys.stdout.write(json.dumps({"UserId": "user-%d" % i, "Amount": amount, "$$tablet_index": 0}) + "\n")' \
  | yt insert-rows --format json "$YT_DEV_ROOT/state_joiner/input_queue"

FLOW_BIN=~/ytsaurus/yt/yt/flow/bin/flow_server/flow_server.stripped ./run.sh state_joiner
```

`run.sh` returns on its own when the pipeline completes — budget about two minutes. Set `FLOW_BIN`
to the **stripped** server: the runner uploads that exact file on every deploy, and the unstripped
build it defaults to is gigabytes.

Then check the output, and finally `./stop.sh state_joiner` to abort the vanilla operation (the
pipeline is already `completed`, a final state, so there is nothing to stop):

```bash
yt flow get-pipeline-state "$YT_DEV_ROOT/state_joiner/pipeline"
yt select-rows "UserId, Total from [$YT_DEV_ROOT/state_joiner/output_table]" --format json
yt select-rows "UserId, Total from [$YT_DEV_ROOT/state_joiner/user_totals]" --format json
```

## Observed output

Recorded against the server build `run.sh` prints on the way in — the companion classes are newer
than every release, so the exact build is part of the observation:

```
flow_server: 26.2.0-local-os~5c69dd1804e43fe5
```

`run.sh` ends with, and exits 0 on (cluster URL and Cypress root elided):

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

Timings for that run (one worker, both binaries already in the cluster's file cache): `run.sh`
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
./stop.sh state_joiner
yt unregister-queue-consumer "$YT_DEV_ROOT/state_joiner/input_queue" "$YT_DEV_ROOT/state_joiner/consumer"
yt remove -r "$YT_DEV_ROOT/state_joiner"
python3 state_joiner/yt_sync.py
# re-insert the four rows, then ./run.sh state_joiner
```

Deleting first and re-running `yt_sync.py` did not converge here: four runs in a row failed with
`Error resolving path #<id> / No such object <id>` out of `register_queue_consumer`, leaving the
pipeline node uncreated, and only an explicit `yt unregister-queue-consumer` (against the paths,
which still resolve) made the next run succeed — twice, on two separate recreates. This is the
sharp edge of the transient failure `word_count_sync` records as "just re-run it".

## Differences from the integration test this is ported from

Upstream: `yt/yt/flow/tests/state_joiner/` (`test.py`, `pipeline/lib/state_joiner_functions.cpp`,
`pipeline/pipeline_same_key.yson`, `pipeline/pipeline_override.yson`) in the ytsaurus repo. Same
graph, same stream names, same four users, same assertion.

- **The user code runs in a companion, not in-process**, and only the two process functions are
  shipped; the pipeline runs on the stock `flow_server`, because that is the path an opensource
  user should take. `TAccumulatorFunction` and `TJoinerFunction` are upstream's, with upstream's
  granularities (`IProcessFunction` and `IBatchProcessFunction`).
- **`state_joiners` became `external_state_joiners`** and the accumulator's internal state became an
  external state manager over `user_totals` — see "What this scenario found" above. This is the one
  substantive deviation: the upstream feature does not exist out of process.
- **Only the `same_key` variant is ported.** `key_schema_override` crashes the worker in a
  companion (above), and `manual_preload` — `auto_preload = false`, where upstream's function calls
  `PreloadKeyStates` itself — is the same defect once more: the flag switches off the worker-side
  preload, so the joiner's loaded states stay empty and the host's `GetState` walks into the same
  `GetOrCrash`. Companion user code cannot fill them either, because the companion's own
  `PreloadKeyStates` is a no-op (`companion/server/state_store.cpp`) — the joined states it sees are
  whatever the host already shipped. Not deployed here; the crash mechanism is the one demonstrated
  above. `cached` (a joiner TTL cache in the dynamic spec) is a performance knob over the same
  assertion and is left out.
- **The reader stays stock.** Upstream also uses `TSwiftPassthroughOrderedSourceComputation`, so no
  companion resource is needed on it: `required_resource_ids` names `CompanionManager` only on the
  two companion-hosted computations.
- **The queue schema has no `flow_queue_meta` column.** Upstream's `yt_sync` declares one; the
  queue source does not need it, and the other scenarios in this repo omit it too.
- **Rows are inserted in the JSON format**, where a literal `$` in a column name is doubled
  (`$$tablet_index`). Upstream uses the client's default YSON path, which needs the separate
  `ytsaurus-yson` bindings; this repo asks only for `ytsaurus-client`.
- **Partition counts are explicit** (`desired_partition_count` 1/2/2) where upstream leaves
  `parameters = {}`. With four rows it changes nothing but the job count.
- **The dynamic spec is tuned for a four-row demo**, and neither value belongs in a real pipeline
  unexamined: `batch_duration = 100` (ms) on all three computations instead of the 1 s default, so
  the pipeline finishes quickly, and `job_tracker/job_threads = 4` instead of the default 30,
  because five jobs on one worker need no more. Raise both for anything with real throughput.
