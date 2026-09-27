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

The scenario has two companion variants — a C++ one (`companion/`) and a Go one
(`companion_go/`) that puts the reader on the *source* path too, not just the transform. The Java
variant is its own scenario, `word_count_sync_java/`. **Only the Go variant runs against the Flow
0.2.1 test-release artifacts and is verified below; the C++ variant needs a source build and is not
covered by the artifact run**
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

## Rerunning

`completed` is a final state that refuses both `stop-pipeline` and a spec update, and the input
queue's consumer cannot be rewound, so a repeat run means recreating the scenario:

```bash
./stop.sh word_count_sync_go
yt remove -r "$YT_DEV_ROOT/word_count_sync_go"
python3 word_count_sync/companion_go/yt_sync.py
# re-insert the two feed lines, then relaunch as in "Run" above
```

`yt remove` can fail with `Cannot take "exclusive" lock … leader_controller_lock` for a few
seconds after the abort, while the controller's lock transaction expires. Just repeat it.
