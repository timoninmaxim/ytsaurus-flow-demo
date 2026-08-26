# word_count_sync

Word counting with per-key external state, written entirely in **C++ that runs outside
`flow_server`**: the two process functions and the stop-words resource live in
`companion/main.cpp`, a separate binary the worker spawns inside its own vanilla job and drives
over gRPC. The pipeline binary itself is the stock `flow_server`.

```
reader (NCompanion::TSwiftOrderedSourceCompanionComputation over TQueueSource, finite)
   → words
counter (NCompanion::TTransformCompanionComputation)
   → external state /state → word_counts table
   → skipped → NSortedDynamicTable::TSyncSink → skipped_words table
```

`reader` splits each input line into words. `counter` groups by `farm_hash(word), word` and, per
word:

- drops it if the **`StopWords` resource** says so (`flow`, `to`) — that resource is also hosted by
  the companion process, so its parameters are parsed and read by *your* code, not by the server;
- otherwise, if it is shorter than `min_word_length = 4`, emits it into the `skipped` stream with
  its length instead of counting it;
- otherwise increments its count in the external state table.

The source is finite: it reads the queue to its end and the pipeline reaches `completed` on its
own. The assertion is then the content of the two tables.

## What this scenario is really for

It is the first scenario to run a **C++ companion** on this cluster, so it doubles as the
reference for every later scenario that ships user C++ without building its own `flow_server`.
Everything a companion needs is below; the summary is:

| Piece | Value |
|---|---|
| Host computation classes | `NCompanion::TSwiftOrderedSourceCompanionComputation`, `NCompanion::TTransformCompanionComputation` |
| Function selection | `processing_function` = the C++ type name; registered companion-side by `TPipeline::AddSource` / `AddTransform` |
| Computation id | the string given to `AddSource`/`AddTransform` **is** the spec's computation id |
| Companion binary | `CompanionManager` resource, `parameters/entrypoint/executable = "./word_count_sync_companion"` |
| Delivery | `vanilla/worker/local_files` — the exec bit propagates, no wrapper script and no archive |
| Worker ports | `vanilla/worker/port_count = 3` (rpc + monitoring + **companion**) |
| Spec parseability | top-level `abort_on_specs_parseability_error = %false` — explicit, but it is also the default |
| Companion-hosted resource | `NCompanion::TCompanionResource` + `parameters/companion_resource_class`, `dependencies = {CompanionManager = …}`; registered companion-side by `TPipeline::AddResource` |

Three things must agree between the spec and `companion/main.cpp`, and only the first one tells
you when it does not:

- **The computation id.** `AddTransform<TWordCountFunction>("counter")` and the spec's
  `computations/counter` are the same string. A job for an id the companion did not register is
  rejected with `Computation "…" is not registered in this companion`
  (`companion/server/companion_service.cpp`).
- **Source vs transform.** A computation declared with `AddSource` must be hosted by a
  `…SourceCompanionComputation` in the spec, and one declared with `AddTransform` by
  `TTransformCompanionComputation`. The companion advertises the kind it registered in its
  `CompanionInfo`, but nothing cross-checks it against the spec, so a mismatch here is silent —
  it surfaces as the host class doing the wrong thing with your batches, not as an error.
- **The build.** Server and companion talk `companion_service.proto`, which is not a stable
  published contract between versions; build both from the same checkout.

The worker port is the trap that costs a deploy: without a third port there is nowhere for the
companion's gRPC server to listen. `port_count = 3` asks YT to allocate it and the worker picks it
up from `YT_PORT_2`. The alternative — pinning `vanilla/node_config/companion/port` to a fixed
number, as `companion_python` does — also works, but only while no third port is allocated:
`YT_PORT_2` overrides the configured value (`library/cpp/runner/node.cpp`). Prefer `port_count`;
a fixed port collides as soon as two workers share a host.

`abort_on_specs_parseability_error` is **not** a trap, despite what the in-tree test harness
suggests by setting it. On every deploy the runner logs, once per companion-hosted function:

```
E	SimpleRunner	Found specs parseability error
Unknown processing function "NYT::NFlow::NDemo::TWordCountFunction" in computation "counter"
```

That is expected and harmless: `flow_server` cannot resolve names that only exist in your
companion, and the names are resolved inside the companion instead. The error is logged
unconditionally and the flag already defaults to `%false`, so nothing is refused
(`library/cpp/runner/simple_runner_program.cpp`). It is written out here only so that a spec
copied from a test config cannot arrive with `%true` and abort the runner.

**Version bar.** The C++ companion classes are newer than every published YTsaurus artifact —
the newest release tag at the time of writing predates them — so both `flow_server` and your
companion have to be built from a recent checkout. The pieces this scenario needs landed on
2026-07-29 (`library/cpp/companion/server`, the `TPipeline`/`RunCompanionMain` SDK) and 2026-08-06
(`NCompanion::TCompanionResource`, used here for `StopWords`); an older server fails the deploy
with `No resource "NYT::NFlow::NCompanion::TCompanionResource" is registered`. A scenario using
`NCompanion::TTransformOrderedSourceCompanionComputation` instead of the swift source needs
2026-08-09.

## Run

From the repo root:

```bash
word_count_sync/build.sh          # builds + strips the companion (YTSAURUS=<checkout>)
python3 word_count_sync/yt_sync.py  # once: pipeline node, input_queue + consumer, the two tables

printf '%s\n' '{"text": "hello to a world", "$$tablet_index": 0}' \
              '{"text": "flow is on it", "$$tablet_index": 0}' \
    | yt insert-rows --format json "$YT_DEV_ROOT/word_count_sync/input_queue"

./run.sh word_count_sync          # deploys and streams the log until the pipeline completes
```

Like `shuffle`, `run.sh` returns on its own — budget about a minute and a half.
Then check the two tables:

```bash
yt flow get-pipeline-state "$YT_DEV_ROOT/word_count_sync/pipeline"
yt select-rows "word, count from [$YT_DEV_ROOT/word_count_sync/word_counts]" --format json
yt select-rows "word, length from [$YT_DEV_ROOT/word_count_sync/skipped_words]" --format json
```

Finally `./stop.sh word_count_sync` aborts the vanilla operation (the pipeline is already
`completed`, a final state, so there is nothing to stop).

## Observed output

Recorded against the server build `run.sh` prints on the way in — the companion classes are newer
than every release, so the exact build is part of the observation:

```
flow_server: 26.2.0-local-os~5c69dd1804e43fe5
```

`run.sh` ends with, and exits 0 on (cluster URL and Cypress root elided):

```
I	FlowClient	Pipeline completed (Pipeline: <…>/word_count_sync/pipeline)
```

```
$ yt flow get-pipeline-state "$YT_DEV_ROOT/word_count_sync/pipeline"
completed

$ yt select-rows "word, count from [$YT_DEV_ROOT/word_count_sync/word_counts]" --format json
{"word":"hello","count":1}
{"word":"world","count":1}

$ yt select-rows "word, length from [$YT_DEV_ROOT/word_count_sync/skipped_words]" --format json
{"word":"a","length":1}
{"word":"is","length":2}
{"word":"it","length":2}
{"word":"on","length":2}
```

Both tables match the upstream test's assertions exactly. Read the two together and they also
prove the companion-hosted resource was applied: `flow` is four letters long, so without
`StopWords` it would be counted, and `to` would show up among the skipped words. Neither appears.

Timings for that run (one worker; the binaries were already in the cluster's file cache):
`run.sh` 03:03:13 → vanilla operation 03:03:15 → pipeline `working` 03:03:46 → all six jobs
running 03:04:01 → `completed` 03:04:39. The companion process costs the ~15 s between `working`
and the jobs running; while it is starting, the controller log carries one round of

```
W	PublicFlowController	Received worker error (Component: /resource_manager/resources/StopWords/companion_resource_client/operations/ResourceExecute, WorkerAddress: …)
failed to connect to all addresses; last error: UNKNOWN: ipv4:0.0.0.0:24582: Failed to connect to remote host: Connection refused
```

for both `ResourceExecute` and `GetCompanionInfo`. That is the worker reaching the companion's
port before the companion has bound it; it retries and recovers. A *repeating* one means the
companion died — check the operation's job stderr.

### What a failure inside your companion code looks like

An exception thrown by a process function is reported by the controller with your message intact,
wrapped in the gRPC call that carried the batch (verified by making the counter throw on a word):

```
W	PublicFlowController	Received job retryable error (Component: /operations/DoProcess, JobId: …, PartitionId: …, ComputationId: counter)
Failed to process message
    origin          (unknown) (pid 262, thread Companion:16, fid …)
    method          ProcessBatch
    key             [65522721553672589u;"boom";]
    address         0.0.0.0:24582
    service         NYT.NFlow.NProto.NCompanion.CompanionService

  Word "boom" is not welcome here
```

`origin (unknown)` is the companion process (it does not resolve its hostname); `key` is the
grouping key of the message that failed, which is usually enough to find the offending row. The
error is **retryable**: the batch is retried forever and the pipeline stays `working` rather than
failing. A poison message therefore also blocks `stop.sh` — the pipeline hangs in `draining`, and
the only way out is to abort the operation directly.

## Rerunning

`completed` is a final state that refuses both `stop-pipeline` and a spec update, and the input
queue's consumer cannot be rewound, so a repeat run means recreating the scenario:

```bash
./stop.sh word_count_sync
yt remove -r "$YT_DEV_ROOT/word_count_sync"
python3 word_count_sync/yt_sync.py   # may need a second run: consumer registration races master lag
# re-insert the two rows, then ./run.sh word_count_sync
```

The `yt remove` can fail with `Cannot take "exclusive" lock … leader_controller_lock` for a few
seconds after the abort, while the controller's lock transaction expires. Just repeat it.

## Differences from the integration test this is ported from

Upstream: `yt/yt/flow/tests/word_count_sync/` (`test_word_count_sync.py`, `pipeline/main.cpp`) in
the ytsaurus repo. Same input, same expected counts, same skipped words.

- **The user code runs in a companion, not in-process.** Upstream builds its own pipeline binary
  (`TProcessFunctionSourceComputation` + `TProcessFunctionComputation` linked into a runner). Here
  the pipeline runs on the stock `flow_server` and only the user code is shipped, because that is
  the path an opensource user should take: the demo repo builds a custom `flow_server` only for
  `secret_env`, where the subject is the job process's own environment. The two host classes used
  here are the out-of-process counterparts of upstream's, with the same semantics — upstream's
  `TProcessFunctionSourceComputation` is itself a swift ordered source
  (`TProcessFunctionComputationBase<TSwiftOrderedSourceComputation>`), so the reader's output is
  not materialized and is recomputed deterministically on restart in both versions.
- **The synchronous side-write became a sync sink.** Upstream's `TWordCountFunction` also
  implements `ISyncProcessFunction` and writes the skipped words itself, from `Sync()`, into the
  epoch's transaction. **A C++ companion refuses such a function**: the companion process has no
  YT client and no transaction, so it rejects the job at init rather than silently dropping the
  write —

  ```
  Process function "…" overrides Sync; sync process functions are not supported in companions
  ```

  (`library/cpp/companion/server/job.cpp`). So the skipped words are emitted into their own stream
  and written by `NSortedDynamicTable::TSyncSink`, which modifies rows inside the *same* epoch
  transaction that commits the counting state. The atomicity the scenario is about is preserved;
  it is expressed in the spec instead of in user code. If you need to write to YT from user code
  inside the epoch transaction, your computation cannot live in a companion.
- **`skipped_words_table_path` is gone from the function parameters** — the sink owns the path now,
  so the function only takes `min_word_length`.
- **Rows are inserted in the JSON format**, where a literal `$` in a column name is doubled
  (`$$tablet_index`). Upstream uses the client's default YSON path, which needs the separate
  `ytsaurus-yson` bindings; this repo asks only for `ytsaurus-client`.
- **Table attributes are plain.** Upstream's `yt_sync` asks for `optimize_for = scan`,
  `chunk_format = table_versioned_columnar`, `in_memory_mode = uncompressed` and
  `enable_dynamic_store_read` on `word_counts` and `skipped_words`. The first three are
  performance choices, one of which needs bundle memory. `enable_dynamic_store_read` is dropped
  too and is worth a word: it only affects bulk readers over a dynamic table, while the
  `select-rows` this scenario verifies with always sees the dynamic stores — so the checks below
  are exact without it.

## Go companion variant

`companion_go/` re-runs the scenario with the reader and the counter written in **Go**
(`go.ytsaurus.tech/yt/go/flow`), hosted by the same stock `flow_server` through the same two
companion host classes — including the swift source: `TSwiftOrderedSourceCompanionComputation`
drives `flow.NewRowSourceComputation("reader", …)` exactly as it drives the C++ and Python
readers, so this variant puts a Go function on the source path, not just on the transform path,
and is the first of the Go ports to do so (`key_visitor` and `swift_map_batching` kept their
readers native). The topology — the external state `/state` behind the `word_counts` table and
the skipped-words stream written by the sync sink inside the same epoch transaction — and the
choreography are unchanged; everything runs under its own root `$YT_DEV_ROOT/word_count_sync_go`.

**The pipeline binary is its own runner**, as in the other `companion_go` variants: the same
`main` calling `pipeline.Run()` is the companion served inside the worker job and the launcher
run on the dev host. `pipeline_go.yson.template` is accordingly smaller than the C++ and Python
specs: no `streams` block (the schemas registered with `pipeline.AddStreams` are injected), no
`entrypoint`, no `local_files`, no worker `port_count`, no `processing_function` names (the Go
SDK dispatches by `computation_id`). Verified against the live run once more — the runner did
all of it unaided: the stored spec carried the two injected stream schemas and
`entrypoint = {executable = "./go_companion"}` + `run_process = %true`, and the worker task ran
with `port_count: 3` and a `go_companion` file entry pointing at the uploaded pipeline binary.

Adaptations against the C++ variant, stated explicitly — the asserts are unchanged:

- **The stop words travel in the spec's `parameters`, as in the Python variant.** The Go SDK
  registers computations and streams only; there is no counterpart of the C++
  `TPipeline::AddResource`, so the `StopWords` resource is gone from the spec and
  `computations/counter/parameters` carries both `min_word_length` and `stop_words`, read via
  `rt.Parameters()`. One consequence worth knowing: the server-side host class does not
  recognize user parameters, so the runner logs a single startup
  `E SimpleRunner Found specs parseability error — Static spec has unrecognized fields` naming
  exactly these two fields. Like the C++ variant's `Unknown processing function` errors, it is
  logged unconditionally and refuses nothing (`abort_on_specs_parseability_error` defaults to
  `%false`); the parameters do reach the companion — the output tables prove it.
- **"No count yet" is not an absent state row.** The external-state accessor's
  `state.Get()` returns `ok` for a key that was never counted: live, the state manager hands
  the companion a present row with the key columns set and `count` **null**. The null check
  must therefore be per column (`row.Has("count")`) — the direct translation of the C++
  `GetColumnValue<std::optional<i64>>("count").value_or(0)`. The first deploy of this variant
  read `row.Int64("count")` whenever `Get()` said `ok` and looped forever on the retryable
  `flow: null value: column "count"` — a poison batch exactly as described for the C++ variant
  above, invisible offline because `flowtest.Harness` models an unseeded key as *no row at all*
  (`Get()` returns `false`). `TestCountingToleratesNullCount` in `main_test.go` now pins the
  live shape.
- **Only `Set` persists the state.** As in Python — and unlike the internal-state
  `OpenYSONState` accessor, which auto-flushes mutations — the external-state accessor writes
  back only what `state.Set(row)` is given; the counter rebuilds the row with
  `state.Builder().Set("count", count+1)`.
- The Go Flow SDK is not in a tagged `go.ytsaurus.tech/yt/go` release yet, so `go.mod` replaces
  the module with a sibling source checkout of `github.com/ytsaurus/ytsaurus` (clone it next to
  this repo, or repoint with `go mod edit -replace`). `./run.sh` does not fit the Go route — it
  execs `$FLOW_BIN --config <spec>`, while the Go runner is the pipeline binary itself and
  needs `--flow-bin` on top — so the template is rendered with a one-liner and the binary is
  launched directly (below).

The word logic is proven offline first: `companion_go/main_test.go` drives both computations
through `flowtest.Harness` (split order, stop-word filtering, skipped-words emission, counting
over prior external state, the null-count row shape, and an end-to-end pipe of the scenario's
two lines asserting exactly the two tables below) — no cluster needed.

Run, from the repo root:

```bash
word_count_sync/companion_go/build.sh       # go build; GO="ya tool go" if there is no system go
(cd word_count_sync/companion_go && ${GO:-go} test ./...)   # offline word-logic tests

python3 word_count_sync/companion_go/yt_sync.py   # once: objects under word_count_sync_go/

printf '%s\n' '{"text": "hello to a world", "$$tablet_index": 0}' \
              '{"text": "flow is on it", "$$tablet_index": 0}' \
    | yt insert-rows --format json "$YT_DEV_ROOT/word_count_sync_go/input_queue"

cd word_count_sync
SCENARIO_DIR="$PWD" python3 -c 'import os, string, sys; sys.stdout.write(string.Template(sys.stdin.read()).substitute(os.environ))' \
    < pipeline_go.yson.template > pipeline_go.yson
./companion_go/word_count_sync_go --config pipeline_go.yson \
    --flow-bin ~/ytsaurus/yt/yt/flow/bin/flow_server/flow_server.stripped
                                        # execs flow_server; returns when the pipeline completes

yt flow get-pipeline-state "$YT_DEV_ROOT/word_count_sync_go/pipeline"
yt select-rows "word, count from [$YT_DEV_ROOT/word_count_sync_go/word_counts]" --format json
yt select-rows "word, length from [$YT_DEV_ROOT/word_count_sync_go/skipped_words]" --format json
cd .. && ./stop.sh word_count_sync_go   # aborts the vanilla operation
```

On this demo cluster (4/9 data nodes), run the erasure-codec workaround right after `yt_sync.py`
and before deploying: set `@erasure_codec = none` and `@hunk_erasure_codec = none` on every table
under `$YT_DEV_ROOT/word_count_sync_go` (queues, consumer and all pipeline system tables) and
remount each. Without it table writes stall hunting for erasure part replicas; with sync unmount
some empty system tables still hang in `unmounting` and need `yt unmount-table --force` before
remounting.

Recorded from the live run on the demo cluster, `flow_server` built from ytsaurus commit
`1bdcb82f3ab` (heads/main), clean deploy of the fixed companion:

```
$ yt flow get-pipeline-state "$YT_DEV_ROOT/word_count_sync_go/pipeline"
completed

$ yt select-rows "word, count from [$YT_DEV_ROOT/word_count_sync_go/word_counts]" --format json
{"word":"hello","count":1}
{"word":"world","count":1}

$ yt select-rows "word, length from [$YT_DEV_ROOT/word_count_sync_go/skipped_words]" --format json
{"word":"a","length":1}
{"word":"is","length":2}
{"word":"it","length":2}
{"word":"on","length":2}
```

Identical to the C++ and Python variants' output, and the two tables again prove the stop words
were applied — this time from `rt.Parameters()`: `flow` is four letters long, so without the
filter it would be counted, and `to` would show up among the skipped words. Neither appears.
Timings: runner launched 14:09:29 UTC → pipeline `working` 14:09:44 → `completed` 14:10:43,
74 s end to end. Startup noise matches the other Go runs' shortened profile: the one
`unrecognized fields` parseability line described above and **no** companion
`Connection refused` at all — the 18 MB Go binary binds its port instantly, with no bundle to
unpack.

## Java companion variant

`companion_java/` re-runs the scenario with the reader and the counter written in **Java**
(`tech.ytsaurus:flow-*`, the Flow Java SDK), hosted by the same stock `flow_server` through the
same two companion host classes — including the swift source: `TSwiftOrderedSourceCompanionComputation`
drives a `SourceComputation` registered in `WordCountSyncMain` exactly as it drives the C++, Python
and Go readers. `key_visitor`'s Java variant kept its reader native, so this is the first of the
Java ports to put a Java function on the source path. The topology — the external state `/state`
behind the `word_counts` table and the skipped-words stream written by the sync sink inside the
same epoch transaction — and the choreography are unchanged; everything runs under its own root
`$YT_DEV_ROOT/word_count_sync_java`.

The plumbing is `key_visitor/companion_java`'s, unchanged: one entry point for the runner and the
companion (`FlowApplication.run` picks the role from `YT_FLOW_MODE`), the composite Gradle build
substituting the unpublished SDK with a sibling ytsaurus checkout, the `collectRuntime` jar
directory the runner ships from `java.library.path`, `TJavaCompanionManager` with only
`main_class` set, `port_count = 3`, and — this cluster having no porto layers — the
`eclipse-temurin:17-jre` docker image plus the `YT_FLOW_JDK_LAYERS='[]'` /
`YT_FLOW_JDK_BIN_PATH=/opt/java/openjdk/bin/java` overrides in `run.sh`.

Adaptations against the C++ variant, stated explicitly — the asserts are unchanged:

- **The stop words travel in the spec's `parameters`, as in the Python and Go variants.** The
  Java SDK registers computations only; there is no counterpart of the C++
  `TPipeline::AddResource`, so `computations/counter/parameters` carries both `min_word_length`
  and `stop_words`, read via `ctx.getComputationParameters()`. The runner logs the same single
  startup `E SimpleRunner … Static spec has unrecognized fields` naming exactly these two fields;
  as in the Go variant it is logged unconditionally and refuses nothing.
- **External state is `Payload`-shaped.** `StateDescriptors.external("/state")` returns an
  accessor over raw schema-carrying rows, not over a typed POJO like the internal-state
  `StateDescriptors.yson(...)` that `key_visitor` uses. `getOrDefault()` hands back the stored
  row, or an all-null row of the state schema when the key is absent — which folds the two live
  "no count yet" shapes (absent row, and present row with `count` null) into one per-column null
  check: `row.get("count", Long.class) == null`, the direct translation of the C++
  `optional<i64>.value_or(0)`.
- **Only `set` persists the state.** As in Python and Go, the counter writes back a fresh
  `PayloadBuilder(row.getSchema())` row with only `count` set; the state manager fills the key
  columns from the grouping key.

The word logic is proven offline first: `WordCountSyncTest` drives both computations through the
SDK's `TestComputationHarness` (`flow-test-utils`) against a trimmed copy of the pipeline spec —
split order, stop-word filtering, skipped-words emission, counting over seeded external state,
the null-`count` row shape, a repeated key read-modify-written twice inside one batch, and an
end-to-end pipe of the scenario's two lines asserting exactly the two tables — no cluster needed.

Run, from the repo root:

```bash
word_count_sync/companion_java/build.sh      # gradle test + collectRuntime (JDK 17+)
python3 word_count_sync/companion_java/yt_sync.py   # once: objects under word_count_sync_java/

printf '%s\n' '{"text": "hello to a world", "$$tablet_index": 0}' \
              '{"text": "flow is on it", "$$tablet_index": 0}' \
    | yt insert-rows --format json "$YT_DEV_ROOT/word_count_sync_java/input_queue"

word_count_sync/companion_java/run.sh        # deploys; returns when the pipeline completes

yt flow get-pipeline-state "$YT_DEV_ROOT/word_count_sync_java/pipeline"
yt select-rows "word, count from [$YT_DEV_ROOT/word_count_sync_java/word_counts]" --format json
yt select-rows "word, length from [$YT_DEV_ROOT/word_count_sync_java/skipped_words]" --format json
./stop.sh word_count_sync_java               # aborts the vanilla operation
```

On this demo cluster, run the erasure-codec workaround described for the Go variant right after
`yt_sync.py` — here it is needed only for the pipeline system tables (the user tables already
come out with `erasure_codec = none`), and the same empty-table `unmounting` hang applies: use a
plain async `yt unmount-table` and `--force` the tablets still `transient` after ~20 s.

Recorded from the live run on the demo cluster, `flow_server` and the SDK built from ytsaurus
flow-core commit `baaaeedbe3c` (heads/main):

```
$ yt flow get-pipeline-state "$YT_DEV_ROOT/word_count_sync_java/pipeline"
completed

$ yt select-rows "word, count from [$YT_DEV_ROOT/word_count_sync_java/word_counts]" --format json
{"word":"hello","count":1}
{"word":"world","count":1}

$ yt select-rows "word, length from [$YT_DEV_ROOT/word_count_sync_java/skipped_words]" --format json
{"word":"a","length":1}
{"word":"is","length":2}
{"word":"it","length":2}
{"word":"on","length":2}
```

Identical to the C++, Python and Go variants' output, and the two tables again prove the stop
words were applied from the spec parameters. Timings: runner launched → `completed` in 82 s,
with the usual one round of companion `Connection refused` while the JVM boots (~15 lines here —
between the Go binary's zero and the Python bundle's minute).

A second run re-created the scenario and fed **2000 lines / 16000 words** (a seeded random draw
over eight countable words, seven short ones and the two stop words, inserted in four
500-row batches) to exercise the state read-modify-write at depth instead of the two-line feed's
count-of-one: every per-word count matched the feed's exact statistics — 9570 counted
occurrences, per-key counts up to 1251 — and the skipped table held exactly the seven short
words with their lengths. Same 81 s end to end, so at this size the run is all startup and
drain, not throughput.
