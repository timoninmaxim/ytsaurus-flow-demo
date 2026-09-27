# swift_map_batching

A **swift map that merges** — one output message per key per epoch, carrying every input message
that key had in that epoch — and what merging costs. The merge is what
`allow_batching_with_relaxed_guarantees` on `TSwiftMapComputation` permits — the only knob on a
swift map that trades a delivery guarantee for throughput (a transform has its own,
`processing_mode = at_least_once_consistent`).

```
reader  (stock TSwiftPassthroughOrderedSourceComputation over TQueueSource, finite = %false)
   → event_in     → batcher (NCompanion::TSwiftMapCompanionComputation,
                             parameters/allow_batching_with_relaxed_guarantees = %true)
   → event_batched → writer (NCompanion::TTransformCompanionComputation)
   → sink_event   → TSyncQueueSink → output_queue
```

The batcher joins the `event_id`s of one key's epoch batch into a comma-separated string; the
writer explodes them back into one row per event and tags each row with `batch_size`, the size of
the batch it came out of. Input is `event_id = 0 … N-1` with `group_key = event_id % 10` over a
five-tablet queue; the assertion is upstream's — the `event_id` set in the output queue must equal
`range(N)`. `batch_size` is this port's addition and the only place the merging is visible from
outside: in the first run below every one of the 2000 events came out of a batch of **200**, so
the whole wave crossed the swift map as ten messages.

## Why a companion, and why not a stock-only spec

The flag only *permits* merging; the merge itself is user code. The three computations that need
no user code — `TPassthroughComputation`, `TSwiftPassthroughComputation` and
`TSwiftPassthroughOrderedSourceComputation`, the set `library/cpp/computation/register.cpp`
registers — emit exactly one output per input message, so on a stock-only spec
`allow_batching_with_relaxed_guarantees` never reaches its branch and changes nothing but one
warning line. Exploding the batch back into rows is beyond the stock vocabulary for the
same reason. So this scenario needs user code, and it runs it in a C++ companion on the stock
server (`companion/main.cpp`), not in a pipeline binary of its own: the merge is expressible
out-of-process because the companion wire contract carries an output *group* with several parent
ids (`TOutputGroup::ParentIds`), which `TSwiftMapCompanionComputation::DoProcess` replays into
`output->SetParents(groupParents, …)` on the worker side. `word_count_sync` and
`computation_cycles_and_buffers` cover the companion mechanics — spec wiring,
binary delivery, the version bar; read those first.

One companion-specific detail this scenario adds: the merge needs the *whole key group* as
parents, and that is exactly what `IKeyedBatchProcessFunction::ProcessKey` gets — the batch adapter
calls `output->SetParents(group.Messages, …)` before invoking it. A per-message `IProcessFunction`
can never produce a merged output, whatever the spec says.

## The subject: what the flag buys and what it costs

With the flag off, a swift map's outputs go through the **deterministic** meta setter: each
output's `MessageId` is derived from its single parent's (`GenerateInheritedMessageId`, over the
parent id, the stream and the output's index within that parent). Replay a partition and the same
input produces the same output ids, so the downstream input store recognises the replay and drops
it — exactly-once without materialising anything.

With the flag on, a merged output's `MessageId` is derived from the *sequence* of parents — the
lexicographically minimal parent id plus an **order-sensitive** 128-bit digest over the parent ids
(`TSwiftMergeMetaSetter`, `computation/meta_setter.cpp:400-420`). Deterministic in the parent
sequence, and it has to be: the merge tracker that marks the parents persisted needs the replay to
match the in-flight task. But the sequence itself is **not** deterministic — it is whatever the
epoch happened to hold, in the order it held it — so a replay that regroups the same events
differently produces different ids, and the downstream dedup does not fire. That is the whole
content of "relaxed guarantees": no loss, but at-least-once, and no per-key `MessageId` ordering.

This port makes that concrete rather than quoting it — see the cut experiment below, where 1796
events came out twice, every one of them from **two differently sized batches**.

### Against Flink

A Flink user should read Flow's *swift* as the normal case and Flow's *transform* as the unusual
one: every Flink operator hands records downstream in memory and relies on replay from the last
checkpoint, which is what swift does; a Flow transform additionally persists its output per epoch.
So this scenario is not "an exotic mode", it is the mode a Flink user already lives in.

Batching has no equivalent. The nearest thing is **mini-batch aggregation** in Flink SQL
(`table.exec.mini-batch.enabled` / `allow-latency` / `size`), which buffers records to amortise
state access, and Kafka Streams' record cache with `commit.interval.ms`. Both are pure performance
knobs: they change latency and how many updates a downstream sees, never the delivery guarantee,
because the buffering sits inside the same checkpoint/commit boundary. `allow_batching_with_relaxed
_guarantees` looks like the same knob and is not — it moves the operator from exactly-once to
at-least-once, permanently, for everything downstream of it. The name says so; nothing enforces
that the downstream is idempotent, and nothing at submit time asks.

The Flink counterpart of "merge a key's records into one" as *semantics* rather than as a knob is
a windowed aggregate (`keyBy().window(…).aggregate(…)`), where the batch boundary is declared —
count, time, or session — and therefore deterministic under replay. Flow's batch boundary is the
epoch, i.e. whatever arrived. A migrating user who wants merging without the semantic change
should reach for a keyed transform with state and its own explicit boundary, not for this flag.

## Run

From the repo root:

```bash
swift_map_batching/build.sh          # builds + strips the companion (YTSAURUS=<checkout>)
python3 swift_map_batching/yt_sync.py  # once: pipeline node, 5-tablet queue + consumer, output_queue

python3 swift_map_batching/prepare_data.py 2000 0   # event_id 0..1999 — insert *before* deploying
ALLOW_BATCHING=%true FLOW_BIN=~/ytsaurus/yt/yt/flow/bin/flow_server/flow_server.stripped \
    ./run.sh swift_map_batching
```

Set `FLOW_BIN` to the **stripped** server: the runner uploads that exact file on every deploy, and
the unstripped build the repo README defaults to is gigabytes.

Insert the events *before* `run.sh`, as the upstream test does. It is not a formality: batching can
only merge what one epoch holds, so a pipeline that is already draining a backlog merges in
hundreds while a pipeline fed drop by drop merges nothing and the scenario proves nothing.

The source is not finite, so `run.sh` streams until you Ctrl-C (which only detaches) and
`./stop.sh swift_map_batching` shuts the pipeline down. Feed more waves at any time —
`prepare_data.py <count> <start>` — as long as the id ranges do not overlap.

Verification is one query and one script, used unchanged after every wave below; the only argument
is the number of events fed so far. Every result fenced below is this script's output, except the
blocks that are quoted log lines.

```bash
Q="$YT_DEV_ROOT/swift_map_batching/output_queue"

yt flow get-pipeline-state "$YT_DEV_ROOT/swift_map_batching/pipeline"
yt select-rows "sum(1) as c from [$Q] group by 1" --format json

# Upstream's assertion, plus what the set alone cannot show: duplicates, where they sit, and the
# batch shapes the events came out of.
yt select-rows "event_id, batch_size from [$Q]" --format json | python3 -c '
import json, sys, collections
n = int(sys.argv[1])
rows = [json.loads(l) for l in sys.stdin]
sizes = collections.defaultdict(list)
for r in rows:
    sizes[r["event_id"]].append(r["batch_size"])
ids = set(sizes)
print("rows:", len(rows), "distinct:", len(ids), "equals range(%d):" % n, ids == set(range(n)))
dups = {i: tuple(sorted(v)) for i, v in sizes.items() if len(v) > 1}
print("duplicated event ids:", len(dups), "extra rows:", sum(len(sizes[i]) - 1 for i in dups),
      "max copies:", max((len(v) for v in sizes.values()), default=0))
if dups:
    print("duplicated id band:", (min(dups), max(dups)),
          "group_keys affected:", sorted({i % 10 for i in dups}), "of", sorted({i % 10 for i in ids}))
    for key in sorted({i % 10 for i in dups}):
        pairs = collections.Counter(v for i, v in dups.items() if i % 10 == key)
        print("  group_key", key, "batch-size pairs:", dict(pairs))
print("batch_size histogram:", collections.Counter(r["batch_size"] for r in rows).most_common(5))' 2000
```

## Observed output

Recorded against the server build `run.sh` prints on the way in — the companion classes are newer
than every published artifact, so the exact build is part of the observation:

```
flow_server: 26.2.0-local-os~5c69dd1804e43fe5
allow_batching_with_relaxed_guarantees: %true
```

First run, 2000 events inserted before the deploy:

```
rows: 2000 distinct: 2000 equals range(2000): True
duplicated event ids: 0 extra rows: 0 max copies: 1
batch_size histogram: [(200, 2000)]
```

Upstream's assertion, met — and every event came out of a batch of 200, i.e. each of the ten keys
was merged whole in a single epoch and the writer saw ten messages instead of 2000. Timings (one
worker, both binaries already in the cluster's file cache): `run.sh` 15:12:58 → vanilla operation
15:13:04 → `working` 15:13:20 → all eleven jobs running 15:13:45 (five reader partitions, one per
input tablet, five batcher partitions, one writer) → the 2000 events in the output queue by
15:13:54. A second wave of 20000 events, inserted into the running pipeline, drained in under ten
seconds.

A healthy launch of this pipeline is as noisy as the other companion scenarios and no more:
two `E SimpleRunner Found specs parseability error / Unknown processing function` (one per
companion-hosted computation), four `E FlowClient Failed to update pipeline` while the controller
publishes `leader_controller_address`, three `W Component became broken`
(`/build_cache`, `/collect_feedback`, `/update_metrics`, all `FlowViewKeeper is not initialized`),
one `E Failed to confirm leader_controller_address`, one `W Received worker error … GetCompanionInfo …
Connection refused` while the companion process binds its port, and six
`W Some computations has partial traverse coverage (Computations: [batcher, writer, reader])` at 5 s
intervals — **bursty, not continuous**: they run from the moment the pipeline is `working` until the
jobs are up (12:13:20–12:13:45 UTC in this run) and never appear again. A continuous stream of them
is a different situation; here they stop.

### Cutting the pipeline mid-flight

The point of a swift map is that its output is *not* materialised — after a restart it is
recomputed, not re-read. To make that visible, feed the pipeline continuously and pause it while
messages are in flight:

```bash
P="$YT_DEV_ROOT/swift_map_batching/pipeline"

# the second wave the timings above refer to; it also fills the gap between the first wave and
# the ids the loop below starts at, so the verification range stays contiguous
python3 swift_map_batching/prepare_data.py 20000 2000

# terminal 2: twenty waves of 2000 events, 1.5 s apart
for i in $(seq 0 19); do python3 swift_map_batching/prepare_data.py 2000 $((22000 + i*2000)); sleep 1.5; done

# terminal 3: pause once the output is moving, then resume
yt flow pause-pipeline "$P"
until [ "$(yt flow get-pipeline-state "$P")" = "paused" ]; do sleep 2; done
yt flow start-pipeline "$P"
```

Observed once the pipeline had drained again, with the pause landing at 42000 output rows out of
the 62000 events fed by then (the verification script above, `62000`):

```
rows: 63796 distinct: 62000 equals range(62000): True
duplicated event ids: 1796 extra rows: 1796 max copies: 2
duplicated id band: (40004, 41998) group_keys affected: [0, 1, 2, 3, 4, 5, 6, 7, 8] of [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
  group_key 0 batch-size pairs: {(200, 203): 199}
  group_key 1 batch-size pairs: {(199, 200): 199}
  group_key 2 batch-size pairs: {(199, 200): 199}
  group_key 3 batch-size pairs: {(199, 200): 199}
  group_key 4 batch-size pairs: {(200, 1000): 200}
  group_key 5 batch-size pairs: {(200, 500): 200}
  group_key 6 batch-size pairs: {(200, 500): 200}
  group_key 7 batch-size pairs: {(200, 500): 200}
  group_key 8 batch-size pairs: {(200, 500): 200}
```

Nothing was lost — upstream's assertion holds across the cut — and 1796 events were delivered
twice. **Not one of the 1796 had the same batch size on both copies.** That is the mechanism in
one line: the batcher recomputed the events it had not yet had marked persisted, the epochs after
the restart held different messages, so the merged outputs had different parent sequences,
different `MessageId`s, and the writer's dedup had nothing to match.

The per-key breakdown is what makes that conclusive rather than suggestive, and it is worth reading
carefully — it is the sharpest thing in this scenario:

- **Nine of the ten keys duplicated; `group_key 9` did not.** Its in-flight batch replayed into the
  same parent sequence, produced the same digest and the same `MessageId`, and was deduplicated —
  exactly what the design says should happen. Within the band `40004..41998` every id is duplicated
  *except* the 199 ids ≡ 9 mod 10. So the same cut, in the same epoch, on the same worker, both
  duplicated and deduplicated, and the discriminator is whether the batch was reshaped.
- **Each key has exactly one pair signature**, e.g. `group_key 4` is `(200, 1000)` on all 200 of its
  ids: its 200-message batch came back as part of a 1000-message one. A key that had merely been
  redelivered — a sink retry, a double write — would show its original size twice.
- **The arithmetic closes.** Nine keys × 200 in-flight events would be 1800; the observed 1796 is
  short by exactly four, and exactly four ids sit below the band start (40000..40003, keys 0–3 —
  precisely the four keys reporting 199 rather than 200). Those four were already persisted in an
  earlier batch and were not replayed at all.

The inference rests on one premise worth naming: the *reader's* message ids are replay-stable, so
"different `MessageId`" cannot be blamed on the source. They are — the ordered source registers the
unique seq no against the queue offset in its persisted `OffsetMemory` and extracts the same value
back on a replay (`connectors/common/ordered_source_base.cpp:744-745`), and the swift source stamps
its outputs with the deterministic setter (`computation/swift_ordered_source_computation.cpp:273`).
A replayed offset therefore reaches the batcher with the id it had before the cut.

Reasoning, not a run: had the batcher been a transform, its output would have been read back from
its output store on restart (`computation/computation_base.cpp:1685`) with the ids it already had
and deduplicated downstream. That arm was not executed here; the mechanism is read from the code,
while everything above it is measured.

Two honesty notes. The duplicates are the outcome of *this* cut: the first two waves and the
recovery run below produced none, and a cut that lands between epochs would produce none either.
And this is a pause/resume, not a crash — no worker was killed and no job was lost.

### What the flag gates

Deploy the same graph with the flag off, with unprocessed events waiting in the queue — the flag
lives in the *static* spec, so the pipeline has to be stopped for the runner to accept the change:

```bash
./stop.sh swift_map_batching
python3 swift_map_batching/prepare_data.py 2000 62000   # the batcher needs something to merge
ALLOW_BATCHING=%false ./run.sh swift_map_batching
```

Every batcher job then fails, forever, on the first key that carries more than one message:

```
E  PublicFlowController  Job failed (JobId: …, PartitionId: …, ComputationId: batcher)
Message should have exactly one parent message (not timer)
```

`yt flow describe-pipeline` carries the same as a `warning` message on the `batcher` computation
with `stream_id = event_batched`. Three things about it are worth knowing before you meet it:

- **The error does not name the flag, and the error that does is unreachable.**
  `swift_map_computation.cpp` has a check that says `Output message has %v parents; merging
  requires allow_batching_with_relaxed_guarantees=true`, but the deterministic meta setter throws
  first, from inside `Process`, with a message that mentions neither batching nor the parameter.
- **Nothing writes and nothing stops.** The pipeline sits in `working` with `Jobs status …
  WorkingWithRetryableError: 0`, the output queue stays exactly where it was, and the batcher
  partitions fail on a loop — 40 failures over three minutes here.
- **`stop.sh` will not stop it, and redeploying a fixed spec will not fix it.** `stop-pipeline`
  hangs in `draining` — the failing jobs never finish their epoch — and was still draining four
  minutes later; aborting the vanilla operation is the only exit. And after that, a plain
  `./run.sh swift_map_batching` with the flag back on **does not recover**: the runner's default
  update path stops the pipeline before touching the spec, so it logs `Sent stop` and then `Still waiting (CurrentState:
  Draining, TargetState: Stopped)` indefinitely, and never uploads the corrected spec at all. The
  way out is the runner's non-graceful update, which pauses instead of stopping:

  ```bash
  ALLOW_BATCHING=%true YT_FLOW_GRACEFUL_UPDATE=0 ./run.sh swift_map_batching
  ```

  That recovered the pipeline here, and the 2000 events stranded by the misconfiguration came out
  intact: 63796 → 65796 rows, and the verification script with `64000` gives the end state of the
  whole scenario — every event ever fed delivered, the same 1796 duplicates from the cut and not
  one more:

  ```
  rows: 65796 distinct: 64000 equals range(64000): True
  duplicated event ids: 1796 extra rows: 1796 max copies: 2
  duplicated id band: (40004, 41998) group_keys affected: [0, 1, 2, 3, 4, 5, 6, 7, 8] of [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
  batch_size histogram: [(200, 26400), (500, 21500), (1000, 6000), (199, 1393), (550, 1100)]
  ```

  (per-key pair lines elided; they are the nine shown above, unchanged). This is the state the
  output queue is left in, so the whole analysis can be re-run against it at any time.

## Rerunning

The pipeline never completes, so nothing has to be recreated between runs:
`./stop.sh swift_map_batching`, feed a fresh id range, `./run.sh swift_map_batching`. The output
queue keeps every previous run's rows, so pass the total event count to the verification snippet
rather than the size of the last wave.

To start from an empty queue, drop the consumer registration **before** deleting the nodes it
names, or the unregister may be refused afterwards:

```bash
./stop.sh swift_map_batching
yt unregister-queue-consumer "$YT_DEV_ROOT/swift_map_batching/input_queue" \
                             "$YT_DEV_ROOT/swift_map_batching/consumer"
yt remove -r "$YT_DEV_ROOT/swift_map_batching"
python3 swift_map_batching/yt_sync.py
```

## Differences from the integration test this is ported from

Upstream: `yt/yt/flow/tests/swift_map_batching/` (`test_pipeline.py`, `pipeline/main.cpp`,
`pipeline/pipeline.yson`) in the ytsaurus repo. Same graph, same stream names, same group-by, same
five input tablets, same ten grouping keys, same assertion.

- **The user code runs in a companion, not in-process.** Upstream builds its own pipeline binary
  with `TBatcher : TSwiftMapComputation` and `TWriter : TTransformComputation`; here the pipeline
  runs on the stock `flow_server` and only the two process functions are shipped, because that is
  the path an opensource user should take. `DoProcessKey` became
  `IKeyedBatchProcessFunction::ProcessKey` and `DoProcessMessage` became
  `IProcessFunction::ProcessMessage`; the host classes in the spec
  (`TSwiftMapCompanionComputation`, `TTransformCompanionComputation`) are the out-of-process
  counterparts of upstream's base classes.
- **The output carries `batch_size`.** Upstream's output queue has `event_id` alone, so the merge
  it is testing leaves no trace in what it asserts on — the test would pass identically against a
  swift map that never merged anything. One extra int64 column makes the merge observable, and it
  is what shows that the duplicates after a cut come from a *different* batching.
- **The events are fed in waves into a long-running pipeline** rather than written once by the
  test harness: 2000 before the deploy (upstream's count, and its `EVENT_COUNT` for a
  non-sanitised run), then further waves to drive the cut experiment. Upstream stops at the first
  wave.
- **The pause/resume cut and the flag-off contrast are additions.** Upstream has neither; its
  single test writes 2000 events and waits for the set. Both are the parts of this port that test
  the flag's semantics instead of restating them.
- **`ALLOW_BATCHING` is a template substitution** passed on the command line, so the contrast run is
  the same spec with one value changed. Changing it is a *static* spec change: the pipeline must
  be stopped for the runner to accept it (and see the recovery caveat above).
- **The writer declares `processing_mode = "exactly_once"`** explicitly, where upstream leaves
  `parameters` empty and takes the default. Same value, spelled out because this scenario is about
  delivery semantics.
- **One worker, one controller.** Upstream runs the same 11-partition layout under its process
  federation.
