# word_count_sync

Word counting with per-key external state and a synchronous side-write, hosted in a
**companion** — user code that runs outside `flow_server`, in its own process, driven by the
worker over gRPC:

```
reader (…SourceCompanionComputation over TQueueSource, finite)
   → words
counter (…TransformCompanionComputation)
   → external state /state → word_counts table
   → skipped → NSortedDynamicTable::TSyncSink → skipped_words table
```

`reader` splits each input line into words. `counter` groups by `farm_hash(word), word` and, per
word: drops it if it is a stop word (`flow`, `to`); otherwise, if it is shorter than
`min_word_length = 4`, emits it into the `skipped` stream with its length instead of counting it;
otherwise increments its count in the external state table. The source is finite, so the
pipeline reaches `completed` on its own once it has drained the queue, and the assertion is the
content of the two tables afterwards.

The scenario has three companion variants — a C++ one (`companion/`) and Go/Java ones
(`companion_go/`, `companion_java/`) that put the reader on the *source* path too, not just the
transform. **Only the Go and Java variants run against the Flow 0.2.1 test-release artifacts and
are verified below; the C++ variant needs a source build and is not covered by the artifact run**
(its user code predates every published release).

## C++ companion variant (not covered by the artifact run)

`companion/main.cpp` needs its own `flow_server` built from the same checkout — the companion
wire contract (`companion_service.proto`) is not a stable contract between versions, and the
external-state resource this scenario uses (`NCompanion::TCompanionResource`, for the stop-words
resource) is newer than every published artifact. From this dir, with `YTSAURUS` pointing at a
checkout set up for `ya make`:

```bash
./build.sh                  # builds + strips the companion into word_count_sync_companion.stripped
python3 yt_sync.py          # once: pipeline node, input_queue + consumer, the two tables
```

then insert the two feed lines below and launch `"$YTSAURUS/yt/yt/flow/bin/flow_server/flow_server.stripped" --config pipeline.yson`
(rendered from `pipeline.yson.j2` first). Unverified against 0.2.1.

## Go companion variant

`companion_go/` writes the reader and the counter in **Go** (`go.ytsaurus.tech/yt/go/flow`),
hosted by the same stock `flow_server` through the swift-source and transform companion host
classes. Everything runs under its own root `$YT_DEV_ROOT/word_count_sync_go`.

- **The pipeline binary is its own runner.** The same `main` calling `pipeline.Run()` is the
  companion served inside the worker job and the launcher on the dev host: it injects the
  registered stream schemas into the spec and points the `CompanionManager` resource at itself.
- **The stop words travel in the spec's `parameters`.** The Go SDK registers computations and
  streams only, with no counterpart of the C++ companion-hosted resource, so
  `computations/counter/parameters` carries both `min_word_length` and `stop_words`, read via
  `rt.Parameters()`.
- **"No count yet" is not an absent state row.** Live, the state manager hands the companion a
  present row with the key columns set and `count` **null**, so the null check is per column
  (`row.Has("count")`) rather than relying on `Get()`'s `ok` — the direct translation of the C++
  `optional<i64>.value_or(0)`. `main_test.go` pins this shape offline through
  `flowtest.Harness`, alongside split order, stop-word filtering, skipped-word emission and an
  end-to-end pipe of the scenario's two lines.

### Build

Built with the official Go toolchain container, never a local or Arcadia-built `go`, from
`companion_go`:

```bash
cd word_count_sync/companion_go
podman run --rm -v "$PWD:/src" -w /src -e CGO_ENABLED=0 docker.io/library/golang:1.24 \
    sh -c 'go get go.ytsaurus.tech/yt/go/flow@6f22b54593c1 && go mod tidy && go test ./... && go build -o word_count_sync_go .'
```

This resolves the SDK straight from its public module path, runs the offline tests, and produces
the static binary `word_count_sync_go` (gitignored) — the same binary is both the launcher and
the companion.

### Run

From `word_count_sync/companion_go`:

```bash
python3 yt_sync.py                # once: objects under word_count_sync_go/
jinjanate pipeline.yson.j2 > pipeline.yson   # every {{ VAR }} from the env; an unset one fails the render

printf '%s\n' '{"text": "hello to a world", "$$tablet_index": 0}' \
              '{"text": "flow is on it", "$$tablet_index": 0}' \
    | yt insert-rows --format json "$YT_DEV_ROOT/word_count_sync_go/input_queue"

podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" --entrypoint ./word_count_sync_go \
    ghcr.io/ytsaurus/flow-nightly:dev-0.2.1 --config pipeline.yson
```

(If a relative `--entrypoint` does not resolve in your podman version, use
`--entrypoint /app/pipeline/word_count_sync_go` instead.) The source is finite, so this command
returns on its own once the pipeline reaches `completed` — no Ctrl-C needed. Then:

```bash
yt flow get-pipeline-state "$YT_DEV_ROOT/word_count_sync_go/pipeline"
yt select-rows "word, count from [$YT_DEV_ROOT/word_count_sync_go/word_counts]" --format json
yt select-rows "word, length from [$YT_DEV_ROOT/word_count_sync_go/skipped_words]" --format json
cd .. && ./stop.sh word_count_sync_go   # aborts the vanilla operation (pipeline is already completed)
```

### Observed output

Recorded from the live run on the demo cluster against the Flow 0.2.1 test release (GitHub
commit `6f22b54593c142c07b92fe542a489db1a8f5b233`, `Tag: flow-test/0.2.1` — logged by the
controller's `FlowCoreVersion` line), image `ghcr.io/ytsaurus/flow-nightly:dev-0.2.1` confirmed on
both the controller and the worker task specs. Launched 04:00:26 UTC → `working` within seconds →
`completed` 04:01:35 (69 s end to end, one worker, both binaries already in the cluster's file
cache):

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

Both tables match the expected result exactly, and together they prove the stop words were
applied from the spec parameters: `flow` is four letters long, so without the filter it would be
counted, and `to` would show up among the skipped words. Neither appears.

## Java companion variant

`companion_java/` writes the reader and the counter in **Java** (`tech.ytsaurus:flow-*`), hosted
by the same stock `flow_server` through the same two companion host classes. Everything runs
under its own root `$YT_DEV_ROOT/word_count_sync_java`.

- **One entry point for both roles.** `WordCountSyncMain.main` registers the two computations and
  calls `FlowApplication.run(args, context)`, which acts as the launcher (enriches the spec, ships
  the companion jars, execs `flow_server`) with no Flow env vars set, and serves both
  computations over the companion gRPC protocol inside the worker job.
- **The stop words travel in the spec's `parameters`,** as in the Go variant — the Java SDK has no
  counterpart of the C++ companion-hosted resource, so `WordCount` reads `min_word_length` and
  `stop_words` from `ctx.getComputationParameters()`.
- **External state is `Payload`-shaped.** `StateDescriptors.external("/state")` hands back the
  stored row via `getOrDefault()`, or an all-null row of the state schema when the key is absent —
  which folds the two live "no count yet" shapes (absent row, present row with `count` null) into
  one per-column null check, the direct translation of the C++ `optional<i64>.value_or(0)`. Only
  `count` is set on the way back; the state manager fills the key columns from the grouping key.
- `WordCountSyncTest` drives both computations through the SDK's `TestComputationHarness`
  (`flow-test-utils`) against a trimmed copy of the pipeline spec — split order, stop-word
  filtering, skipped-word emission, counting over external state, the null-`count` row shape, and
  an end-to-end pipe of the scenario's two lines — no cluster needed.

### Build

Built with the official Gradle toolchain container, never a local or Arcadia-built JDK/Gradle,
from `companion_java`:

```bash
cd word_count_sync/companion_java
podman run --rm -v "$PWD:/src" -w /src docker.io/library/gradle:8-jdk17 \
    gradle -q --refresh-dependencies test installLib -PflowVersion=0.2.1-SNAPSHOT
```

This resolves the SDK from the Sonatype snapshot repository, runs the offline tests
(`WordCountSyncTest`), and syncs the pipeline jar plus its runtime deps into `lib/` (gitignored) —
the classpath the launch command's `-cp 'lib/*'` uses.

### Run

From `word_count_sync/companion_java`:

```bash
python3 yt_sync.py                # once: objects under word_count_sync_java/
jinjanate pipeline.yson.j2 > pipeline.yson   # every {{ VAR }} from the env; an unset one fails the render

printf '%s\n' '{"text": "hello to a world", "$$tablet_index": 0}' \
              '{"text": "flow is on it", "$$tablet_index": 0}' \
    | yt insert-rows --format json "$YT_DEV_ROOT/word_count_sync_java/input_queue"

podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" \
    ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1 -cp 'lib/*' \
    tech.ytsaurus.flow.demo.wordcountsync.WordCountSyncMain --config pipeline.yson
```

The launcher enriches the spec (companion classpath), uploads the released `flow_server` and
launches the controller+worker vanilla operation, then streams the controller log. The source is
finite, so this command returns on its own once the pipeline reaches `completed`. Then:

```bash
yt flow get-pipeline-state "$YT_DEV_ROOT/word_count_sync_java/pipeline"
yt select-rows "word, count from [$YT_DEV_ROOT/word_count_sync_java/word_counts]" --format json
yt select-rows "word, length from [$YT_DEV_ROOT/word_count_sync_java/skipped_words]" --format json
cd .. && ./stop.sh word_count_sync_java   # aborts the vanilla operation (pipeline is already completed)
```

### Observed output

Recorded from the live run on the demo cluster against the Flow 0.2.1 test release (same commit
and tag as the Go variant's), image `ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1` confirmed on
both task specs, worker `file_paths` carrying 64 launcher-shipped jars under `java_companion/`
including `flow-server-0.2.1-SNAPSHOT.jar`/`flow-runner-…`/`flow-core-…` — the released SDK, not a
source build. Launched 04:04:53 UTC → `completed` 04:06:00 (67 s end to end):

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

Identical to the Go variant's output, and the two tables again prove the stop words were applied
from the spec parameters.

## Rerunning

`completed` is a final state that refuses both `stop-pipeline` and a spec update, and the input
queue's consumer cannot be rewound, so a repeat run means recreating the scenario:

```bash
./stop.sh word_count_sync_go   # or word_count_sync_java
yt remove -r "$YT_DEV_ROOT/word_count_sync_go"
python3 word_count_sync/companion_go/yt_sync.py
# re-insert the two feed lines, then relaunch as in "Run" above
```

`yt remove` can fail with `Cannot take "exclusive" lock … leader_controller_lock` for a few
seconds after the abort, while the controller's lock transaction expires. Just repeat it.
