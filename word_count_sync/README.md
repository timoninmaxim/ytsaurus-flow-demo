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

The companion here is written in C++ (`companion/`). The same pipeline with the companion in Go
and in Java is in `word_count_sync_go/` and `word_count_sync_java/` — those run on the released Flow
0.2.1 artifacts. **This C++ variant needs a source build and is not covered by the artifact run.**

## Build and run (source build, not covered by the artifact run)

`companion/main.cpp` needs its own `flow_server` built from the same checkout — the companion
wire contract (`companion_service.proto`) is not a stable contract between versions, and the
external-state resource this scenario uses (`NCompanion::TCompanionResource`, for the stop-words
resource) is newer than every published artifact. From this dir, with `YTSAURUS` pointing at a
checkout set up for `ya make`:

```bash
./build.sh                  # builds + strips the companion into word_count_sync_companion.stripped
python3 yt_sync.py          # pipeline node, input_queue + consumer, the two tables
printf '%s\n' '{"text": "hello to a world", "$$tablet_index": 0}' \
              '{"text": "flow is on it", "$$tablet_index": 0}' \
    | yt insert-rows --format json "$YT_DEV_ROOT/word_count_sync/input_queue"
jinjanate pipeline.yson.j2 > pipeline.yson
"$YTSAURUS/yt/yt/flow/bin/flow_server/flow_server.stripped" --config pipeline.yson
```

Unverified against 0.2.1. The expected tables are the same as in `word_count_sync_go/`.
