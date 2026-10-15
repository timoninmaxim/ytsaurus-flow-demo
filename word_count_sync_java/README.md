# word_count_sync_java

Word counting with per-key external state and a synchronous side-write, written in **Java** and
executed by the companion — a separate JVM the worker spawns inside its own vanilla job and drives
over gRPC:

```
reader (TSwiftOrderedSourceCompanionComputation over TQueueSource, finite)
   → words
counter (TTransformCompanionComputation)
   → external state /state → word_counts table
   → skipped → NSortedDynamicTable::TSyncSink → skipped_words table
```

`reader` splits each input line into words. `counter` groups by `farm_hash(word), word` and, per
word: drops it if it is a stop word (`flow`, `to`); otherwise, if it is shorter than
`min_word_length = 4`, emits it into the `skipped` stream with its length; otherwise increments
its count in the external state table. Both parameters come from the computation's `parameters`
in the spec. The source is finite, so the pipeline reaches `completed` on its own.

## How the companion gets into the job

`WordCountSyncMain.main` registers the two computations and calls `FlowApplication.run`: started
by hand it is the launcher, spawned by the worker it serves the computations.

The launcher prepares the pipeline for start: it enriches the spec with the user files — every jar
on its classpath goes to the worker, and the `CompanionManager` resource (`TJavaCompanionManager`)
is pointed at them — then hands the spec to the image's `flow_server`.

## Build

From this dir, in the official Gradle container — resolves the SDK from the Sonatype snapshot
repository, runs the offline tests (`WordCountSyncTest`) and collects the pipeline jar with its
dependencies into `lib/`:

```bash
podman run --rm -v "$PWD:/src" -w /src docker.io/library/gradle:8-jdk17 \
    gradle -q --refresh-dependencies test installLib
```

## Run

Needs the prerequisites and the sourced env file from the root README (`yt` CLI,
`ytsaurus-flow-yt-sync-mini` and `jinjanate` in one Python environment, podman).

From this dir: bootstrap the Cypress objects, feed the input queue, render the spec, launch through
the Java SDK launcher:

```bash
python3 yt_sync.py   # pipeline node, input_queue + consumer, word_counts, skipped_words
printf '%s\n' '{"text": "hello to a world", "$$tablet_index": 0}' \
              '{"text": "flow is on it", "$$tablet_index": 0}' \
    | yt insert-rows --format json "$YT_DEV_ROOT/word_count_sync_java/input_queue"
jinjanate pipeline.yson.j2 > pipeline.yson   # every {{ VAR }} from the env; an unset one fails the render
podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1 \
    -cp 'lib/*' tech.ytsaurus.flow.demo.wordcountsync.WordCountSyncMain --config pipeline.yson
```

The launch streams the controller log and returns on its own once the pipeline is `completed`.
Then read the two tables:

```bash
yt select-rows "word, count from [$YT_DEV_ROOT/word_count_sync_java/word_counts]" --format json
yt select-rows "word, length from [$YT_DEV_ROOT/word_count_sync_java/skipped_words]" --format json
```

```json
{"word":"hello","count":1}
{"word":"world","count":1}
{"word":"a","length":1}
{"word":"is","length":2}
{"word":"it","length":2}
{"word":"on","length":2}
```

Neither stop word shows up: without the filter `flow` (four letters) would be counted and `to`
would be among the skipped words.

## Stop

The pipeline is already `completed`, so only its vanilla operation is left — abort it by the id the
runner printed at launch, in the `Started vanilla operation (..., OperationId: <id>)` line:

```bash
yt abort-op <OperationId>
```

To drop the scenario's Cypress objects as well: `yt remove -r "$YT_DEV_ROOT/word_count_sync_java"`.
To run again, drop them and repeat Run from the start: `completed` is final and the input queue's
consumer cannot be rewound.
