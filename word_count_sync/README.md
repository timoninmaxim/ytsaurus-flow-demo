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
