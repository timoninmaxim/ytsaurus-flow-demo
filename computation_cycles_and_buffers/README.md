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

## Run

From the repo root:

```bash
computation_cycles_and_buffers/build.sh          # builds + strips the companion (YTSAURUS=<checkout>)
python3 computation_cycles_and_buffers/yt_sync.py  # once: pipeline node, queue + consumer, state table

python3 -c 'import json, sys
for _ in range(1000):
    sys.stdout.write(json.dumps({"data": "payload", "$$tablet_index": 0}) + "\n")' \
  | yt insert-rows --format json "$YT_DEV_ROOT/computation_cycles_and_buffers/input_queue"

FLOW_BIN=~/ytsaurus/yt/yt/flow/bin/flow_server/flow_server.stripped \
    ./run.sh computation_cycles_and_buffers
```

`run.sh` returns on its own when the pipeline completes — budget about two and a half minutes. Set
`FLOW_BIN` to the **stripped** server: the runner uploads that exact file on every deploy, and the
unstripped build it defaults to is gigabytes.

Then check the state table, and finally `./stop.sh computation_cycles_and_buffers` to abort the
vanilla operation (the pipeline is already `completed`, a final state, so there is nothing to
stop):

```bash
yt flow get-pipeline-state "$YT_DEV_ROOT/computation_cycles_and_buffers/pipeline"
yt select-rows "* from [$YT_DEV_ROOT/computation_cycles_and_buffers/state]" --format json
```

## Observed output

Recorded against the server build `run.sh` prints on the way in — the companion classes are newer
than every release, so the exact build is part of the observation:

```
flow_server: 26.2.0-local-os~5c69dd1804e43fe5
```

`run.sh` ends with, and exits 0 on (cluster URL and Cypress root elided):

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

Timings for that run (one worker, both binaries already in the cluster's file cache): `run.sh`
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
the controller is alive). Whether `run.sh`'s streamed output shows the controller-side lines
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
# in a second terminal, with run.sh streaming in the first
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
./stop.sh computation_cycles_and_buffers
yt remove -r "$YT_DEV_ROOT/computation_cycles_and_buffers"
python3 computation_cycles_and_buffers/yt_sync.py   # may need a second run: consumer registration
                                                    # races master lag
# re-insert the 1000 rows, then ./run.sh computation_cycles_and_buffers
```

Both papercuts bite here: `yt remove` failed with `Cannot take "exclusive" lock …
leader_controller_lock` for the first two attempts after the abort, and `yt_sync.py` failed once
with `Error resolving path #… No such object` from `register_queue_consumer` and succeeded on the
retry.

## Differences from the integration test this is ported from

Upstream: `yt/yt/flow/tests/computation_cycles_and_buffers/` (`test.py`, `lib/test_base.py`,
`pipeline/main.cpp`, `pipeline/pipeline.yson`) in the ytsaurus repo. Same topology, same stream
names, same 1000 messages, same assertion.

- **The user code runs in a companion, not in-process.** Upstream builds its own pipeline binary
  with `TReader : TSwiftOrderedSourceComputation`, two `TCycleTransformComputation`, two
  `TCycleSwiftMapComputation` and `TReducer : TTransformComputation` linked into a runner. Here the
  pipeline runs on the stock `flow_server` and only the user code is shipped, because that is the
  path an opensource user should take. The four host classes used here are the out-of-process
  counterparts of upstream's, with the same semantics.
- **Computation classes became process functions.** `DoProcessMessage` became
  `IProcessFunction::ProcessMessage`, and the reducer's `DoProcessKey` became
  `IKeyedBatchProcessFunction::ProcessKey`; both granularities are hosted by the companion (it
  wraps whatever it is given into the whole-epoch batch form). The custom parameters
  (`passthrough_rules`, `sleep_per_message`) moved from the computation's `parameters` to
  `processing_function_parameters`, which is where a companion reads them; `processing_mode`
  stays in `parameters`, because it belongs to the host `TTransformComputation`.
- **A missing passthrough rule throws instead of crashing.** Upstream uses `GetOrCrash`; in a
  companion that would take the whole companion process down for every job on the worker, so the
  port raises `No passthrough rule for input stream …` instead. (Beware: an exception from a
  process function is retried forever — see `word_count_sync`'s README.)
- **Only the `exactly_once` variant is ported.** Upstream parametrises `processing_mode` and also
  runs `at_least_once_consistent`, which only weakens the assertion to `count >= 1000`. The
  `exactly_once_cut_buffers` variant *is* covered, as the manual sequence above.
- **State-table attributes are plain.** Upstream's `yt_sync` asks for `in_memory_mode =
  uncompressed` and a `mount_config` with `min_data_ttl = 0`, `enable_lookup_hash_table`,
  `enable_lookup_cache_by_default`, `merge_rows_on_flush` and `merge_deletions_on_flush`. Those are
  performance and compaction choices, one of which needs bundle memory; the assertion is unaffected
  because `select-rows` always sees the dynamic stores.
- **Rows are inserted in the JSON format**, where a literal `$` in a column name is doubled
  (`$$tablet_index`). Upstream uses the client's default YSON path, which needs the separate
  `ytsaurus-yson` bindings; this repo asks only for `ytsaurus-client`.
- **`update_info_period` is dropped** from the queue source; it only changes how often the source
  refreshes queue info.
- **`stream_id = "source"` is dropped, because there is no such field.** `TSourceSpec` registers
  only `source_class_name` and `parameters`, and the queue source's parameters have no `stream_id`
  either; the source stream is the `source_streams` map key, `queue`, unchanged. Upstream's line is
  an unrecognized key that is silently accepted and ignored.
- **The reader gets `desired_partition_count = 1`** in the dynamic spec, where upstream leaves
  `parameters = {}`. It is moot with a single-tablet input queue, and only makes the intent
  explicit alongside the other three computations.

## Go companion variant

`companion_go/` re-runs the scenario with all six computations written in **Go**
(`go.ytsaurus.tech/yt/go/flow`), hosted by the same stock `flow_server` through the same host
classes as the C++ companion, computation by computation: the reader under
`TSwiftOrderedSourceCompanionComputation`, the two transforms and the reducer under
`TTransformCompanionComputation`, the two swift maps under `TSwiftMapCompanionComputation` —
the first Go port to put functions on the swift-map path of a cycle. The topology, the
choreography and the asserts are unchanged; everything runs under its own root
`$YT_DEV_ROOT/computation_cycles_go`. The cycle again survives untouched: it is spec surface
(`input_stream_ids` / `output_stream_ids` / `streams_dependency`), and the Go function only
picks the output stream per message — `transform_a` routes by `msg.StreamID`, exactly the
C++ variant's passthrough rules.

**The pipeline binary is its own runner**, as in the other `companion_go` variants: the same
`main` calling `pipeline.Run()` is the companion served inside the worker job and the launcher
run on the dev host. `pipeline_go.yson.template` is accordingly smaller than the C++ spec: no
`streams` block (the six schemas registered with `pipeline.AddStreams` are injected), no
`entrypoint`, no `local_files`, no worker `port_count`, no `processing_function` names (the Go
SDK dispatches by `computation_id`). Verified live once more — the runner did all of it
unaided.

Adaptations against the C++ variant, stated explicitly — the asserts are unchanged:

- **The passthrough parameters travel in the spec's `parameters`.** The Go SDK reads user
  configuration via `rt.Parameters()`, which serves the computation's `parameters` map — there
  is no `processing_function_parameters` on the Go path. So `passthrough_rules` and
  `sleep_per_message` sit next to `processing_mode`, and the runner logs one startup
  `E SimpleRunner Found specs parseability error — Static spec has unrecognized fields` naming
  exactly those fields on all four cycle computations. As with the C++ variant's
  `Unknown processing function` errors, it is logged unconditionally and refuses nothing; the
  parameters do reach the companion — the routing proves it.
- **Group-by columns must not be `required` when the Go runner injects the stream schemas.**
  The C++ spec declares its streams itself with `data` required; the Go SDK infers stream
  schemas from the message structs and marks every column optional. A group-by column spelled
  `required = %true` then fails the deploy at spec validation with
  `Column "data" has inconsistent types in original and group-by schemas`
  (`source_type: Optional<String>`, `group_by_type: String`) — drop the `required` flag from
  plain key columns and keep it only on the computed `hash`, as in the other Go variants.
- **The reducer groups its batch itself.** As in the other Go ports — and unlike the C++
  keyed-batch adapter, whose `ProcessKey` is called per key — `OnMessages` gets the request's
  whole batch with keys mixed. The reducer groups by key in first-appearance order kept in a
  slice (never a map range: deterministic order) and opens `/state` once per group; there is
  effectively one key in this scenario, so the group is the batch.
- **"No count yet" is not an absent state row.** Live, the state manager hands the companion a
  present row with the key columns set and `count` **null**, so `state.Get()` returning `ok`
  must be followed by a per-column `row.Has("count")` check — the direct translation of the
  C++ `GetColumnValue<std::optional<i64>>("count").value_or(0)`.
  `TestReducerToleratesNullCount` pins that shape offline.
- The Go Flow SDK is not in a tagged `go.ytsaurus.tech/yt/go` release yet, so `go.mod` replaces
  the module with a sibling source checkout of `github.com/ytsaurus/ytsaurus` (clone it next to
  this repo, or repoint with `go mod edit -replace`). `./run.sh` does not fit the Go route — it
  execs `$FLOW_BIN --config <spec>`, while the Go runner is the pipeline binary itself and
  needs `--flow-bin` on top — so the template is rendered with a one-liner and the binary is
  launched directly (below).

The cycle logic is proven offline first: `companion_go/main_test.go` drives all six
computations through `flowtest.Harness` (the routing rules of every computation — including
transform_a sending a fresh message around the loop and releasing a returned one, the
missing-rule error, the reducer's counting over prior external state, the null-count row
shape, and a 1000-message simulation of the full cycle in batches of 30 ending at exactly
`count == 1000`) — no cluster needed. The simulation harnesses omit the live
`sleep_per_message` values, which only pace the pipeline.

Run, from the repo root:

```bash
computation_cycles_and_buffers/companion_go/build.sh   # go build; GO="ya tool go" if there is no system go
(cd computation_cycles_and_buffers/companion_go && ${GO:-go} test ./...)   # offline cycle tests

python3 computation_cycles_and_buffers/companion_go/yt_sync.py   # once: objects under computation_cycles_go/

python3 -c 'import json, sys
for _ in range(1000):
    sys.stdout.write(json.dumps({"data": "payload", "$$tablet_index": 0}) + "\n")' \
  | yt insert-rows --format json "$YT_DEV_ROOT/computation_cycles_go/input_queue"

cd computation_cycles_and_buffers
python3 -c 'import os, string, sys; sys.stdout.write(string.Template(sys.stdin.read()).substitute(os.environ))' \
    < pipeline_go.yson.template > pipeline_go.yson
./companion_go/computation_cycles_go --config pipeline_go.yson \
    --flow-bin ~/ytsaurus/yt/yt/flow/bin/flow_server/flow_server.stripped
                                        # execs flow_server; returns when the pipeline completes

yt flow get-pipeline-state "$YT_DEV_ROOT/computation_cycles_go/pipeline"
yt select-rows "* from [$YT_DEV_ROOT/computation_cycles_go/state]" --format json
```

**Cluster health note (verified run, 2026-08-13).** The demo cluster was running 4 of 9 data
nodes, while the pipeline-preset tables come out with `@erasure_codec` /
`@hunk_erasure_codec = reed_solomon_3_3`, which needs six nodes and wedges writes. After every
`yt_sync.py` run, reset both codecs on **every** created table and remount (`--force` because
a fresh empty table can wedge on a plain unmount):

```bash
for t in $(yt find "$YT_DEV_ROOT/computation_cycles_go" --type table); do
  yt unmount-table --force --sync "$t"
  yt set "$t/@erasure_codec" none
  yt set "$t/@hunk_erasure_codec" none
  yt mount-table --sync "$t"
done
```

### Observed output

Recorded against `flow_server` built from ytsaurus commit `1bdcb82f3ab` (the runner logs it as
`FlowCoreVersion`); the Go binary was built from the same checkout's `yt/go` SDK. The runner
returned on its own both times with

```
I	FlowClient	Pipeline completed (Pipeline: <…>/computation_cycles_go/pipeline)
```

Run 1 (uninterrupted, launched 17:24:12, completed 17:26:18 — ~126 s):

```
$ yt flow get-pipeline-state "$YT_DEV_ROOT/computation_cycles_go/pipeline"
completed

$ yt select-rows "* from [$YT_DEV_ROOT/computation_cycles_go/state]" --format json
{"hash":8436339620933999394,"data":"payload","count":1000}
```

One row, `count == 1000`, byte-identical to the C++ reference row including the hash.

Run 2 (cut buffers: scenario recreated, paused mid-flight per the sequence above): polling the
count every two seconds, the first non-zero value appeared 47 s after launch —
`pause-pipeline` at `count = 1`, `paused` three seconds later, `start-pipeline` immediately
after; the pipeline went back to `working` and completed 113 s later with the same single row:

```
{"hash":8436339620933999394,"data":"payload","count":1000}
```

Exactly-once held across the cut with 999 of 1000 messages still in flight or buffered inside
the cycle at the pause — a stricter cut than the reference run's `count = 15`.
