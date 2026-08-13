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

## Python companion variant

`companion_py/` re-runs the scenario with all six computations written in **Python**, hosted by
the same stock `flow_server` through the same four companion host classes — the host-class
mapping of the C++ variant carries over computation by computation:
`TSwiftOrderedSourceCompanionComputation` drives `ReadData`, `TTransformCompanionComputation`
drives the two transforms and the reducer, and `TSwiftMapCompanionComputation` drives the two
swift maps. The cycle needs nothing from the SDK at all: it is spec-level stream topology
(`input_stream_ids` / `output_stream_ids` / `streams_dependency`), and `pipeline_py.yson.template`
carries it over unchanged. `companion_py/main.py` registers the six functions —
`CyclePassthrough` four times under four ids, the Python counterpart of registering
`TCyclePassthroughFunction` four times — and the companion delivery is the launcher + bundle pair
from `word_count_sync/companion_py` (`entrypoint = ./py_companion`, two `local_files`, worker
`port_count = 3`).

Deliberate differences against the C++ variant — the asserts are unchanged:

- **The routing tables travel in the spec's `parameters`, not in
  `processing_function_parameters`.** The Python SDK reads a computation's parameters from
  `computations/<id>/parameters` (via `ctx.parameters`), so `passthrough_rules` and
  `sleep_per_message` moved one level up, next to `processing_mode`. Unrecognized keys in
  `parameters` are accepted silently, so the host classes do not object.
- **`processing_function` is omitted.** The Python SDK dispatches by `computation_id`
  (`pipeline.add("transform_a", …)`), so the spec does not name the functions — and the six
  startup `E SimpleRunner Found specs parseability error` lines of the C++ run do not appear.
- **The reducer groups by key itself.** The C++ keyed-batch adapter groups the epoch's input by
  key before invoking `ProcessKey`; the Python `BatchFunction` gets the request's whole message
  batch with keys mixed, so `ReduceCount.on_messages` does the grouping (one group here — the
  1000 rows are identical) and adds each group's size to its key's count.
- **External state is written back explicitly.** Only `state.set(...)` persists the external
  state "/state"; the reducer reads `state.get("count")`, builds the incremented payload and
  `set`s it.
- **A missing passthrough rule raises `RuntimeError`**, the port of the C++ variant's throw —
  with the same retried-forever caveat.

`build.sh` follows `word_count_sync/companion_py/build.sh`: it reuses the already-built
`ytsaurus-flow-companion` wheel from `companion_python/build/wheels/` when present and otherwise
builds it from `$YTSAURUS_SRC/yt/yt/flow/tools/python_companion_package`.

Run, from the repo root:

```bash
computation_cycles_and_buffers/companion_py/build.sh   # companion_bundle.tgz: CPython + SDK + main.py
python3 computation_cycles_and_buffers/companion_py/yt_sync.py  # once: objects under computation_cycles_py/

python3 -c 'import json, sys
for _ in range(1000):
    sys.stdout.write(json.dumps({"data": "payload", "$$tablet_index": 0}) + "\n")' \
  | yt insert-rows --format json "$YT_DEV_ROOT/computation_cycles_py/input_queue"

FLOW_BIN=~/ytsaurus/yt/yt/flow/bin/flow_server/flow_server.stripped \
    ./run.sh computation_cycles_and_buffers py    # stock binary; returns when the pipeline completes

yt flow get-pipeline-state "$YT_DEV_ROOT/computation_cycles_py/pipeline"
yt select-rows "* from [$YT_DEV_ROOT/computation_cycles_py/state]" --format json
./stop.sh computation_cycles_py                   # aborts the vanilla operation
```

Recorded from the live runs on the demo cluster, server build `26.2.0-local-os~5c69dd1804e43fe5`.
The plain run, first deploy:

```
$ yt flow get-pipeline-state "$YT_DEV_ROOT/computation_cycles_py/pipeline"
completed

$ yt select-rows "* from [$YT_DEV_ROOT/computation_cycles_py/state]" --format json
{"hash":8436339620933999394,"data":"payload","count":1000}
```

One row, `count == 1000` — the same assertion as the C++ variant, met from Python, in ~2 min
(deploy 08:39:03 → `completed` 08:41:08).

The **cut-buffers half** was reproduced on a recreated scenario with the pause sequence from the
C++ section above (poll the count every three seconds, pause as soon as it is non-zero):

```
08:43:59 count=1
pause requested at count=1
08:44:06 state=paused
{"hash":8436339620933999394,"data":"payload","count":2}
start requested
...
08:46:46 completed
{"hash":8436339620933999394,"data":"payload","count":1000}
```

Paused with two of 1000 messages committed and the rest in flight or buffered inside the cycle;
after `start-pipeline` the run completed two and a half minutes later at exactly 1000 —
exactly-once held across the cut with the user code out of process in Python.
