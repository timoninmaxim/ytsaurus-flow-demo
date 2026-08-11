# sorted_dynamic_table

Writing a stream into a **sorted YT dynamic table** — a keyed store, not an append-only queue. The
same graph runs three times, once per sink mode, and the only thing that differs between the three
is a couple of lines in the sink's `parameters`:

```
reader (stock TSwiftPassthroughOrderedSourceComputation
        over TQueueSource, finite = %true)
   → data → NYT::NFlow::NSortedDynamicTable::TSyncSink → output_table
```

| variant | sink `parameters` | what the table holds afterwards |
|---------|-------------------|---------------------------------|
| `swift` | `table_path` only | one row per key, the value of the **last** message for that key |
| `delete` | `+ delete_rows = %true; column_filter = ["data"]` | every key the stream carried is **gone** |
| `aggregate` | `+ aggregate_columns = ["i"]` | one row per key, `i` = the **sum** of every message's `i` |

Nothing here is custom — no companion, no pipeline binary of its own, not one line of user code:
all three variants are specs over the stock `flow_server`, and `connectors/sorted_dynamic_table` is
unconditionally linked into it.

The input is 1000 keys `payload_0 … payload_999`; key `payload_n` appears `(n % 13) + 1` times,
carrying `i = 0 … repeat-1`, for 6994 queue rows in total. That fan-out is the whole point of the
payload: it is what makes "last write wins", "delete the same key repeatedly" and "sum the values
of one key" three different answers over the same stream. `payload_12` (13 messages, `i = 0…12`)
comes out as `i = 12` under `swift` and `i = 78` under `aggregate`.

## The subject: a sink that does more than append

A queue sink appends; there is nothing to decide. A sorted dynamic table has a key, so every
message is a *modification* of a row, and this connector exposes three of them through the spec:

- **Write (upsert).** The default. `PackRowModifications` (in the connector's `sink.cpp`) turns
  each message into a `TWriteRow` over a name table built from the input
  stream's schema, and `TSyncSink::DoDistribute` hands the whole batch to
  `transaction->ModifyRows` in one call. Duplicate keys inside a batch are not collapsed — they are
  handed over in order and the last one wins. Observed: the surviving `i` is `repeat-1` for all
  1000 keys, i.e. queue order survives to the table.
- **Delete.** `delete_rows = %true` swaps `TWriteRow` for `TDeleteRow` for the entire sink — it is
  a property of the sink, not of a message, so a pipeline that both writes and deletes needs two
  sinks fed by two streams. `column_filter` is load-bearing here rather than an optimisation: it
  makes `GenerateNameTable` build the name table from the listed columns instead of the whole
  stream schema, so the delete modification carries the key alone. Without it the sink would put
  `i` into a delete row, and a delete takes key columns only: through the pipeline that row is
  rejected by `ValidateClientKey` as `Unexpected column "i"`, and the same row through
  `yt delete-rows` — whose consumer is bound to the table schema instead — answers
  `No column "i" in table schema`. (Only the CLI form was run here; the pipeline form is read from
  the client's validation path.)
- **Aggregate.** `aggregate_columns = ["i"]` sets `EValueFlags::Aggregate` on those values, which
  turns each write into a read-modify-write **inside the tablet node**, using the aggregate function
  declared on the column in the *table's* schema (`{"name": "i", "aggregate": "sum"}` — the
  pipeline spec never names the function). So the running sum lives in the target table and the
  pipeline keeps no state at all: after a run that produced 1000 correct sums, the pipeline's
  `states` table is empty.

All three go through `transaction->ModifyRows` on the epoch transaction the sink is handed
(`TSyncSink::DoDistribute`), which is what makes this a *sync* sink: the rows land in the same
commit as the source's consumer offsets. Nothing in this scenario injects a fault, so what is
demonstrated is the happy path — the sums and the row set are exact, not "exact despite a retry".
And all three variants write from a **single-partition source computation**: one queue tablet, one
output tablet, one worker. Concurrent writers into one sorted table — the case the upstream
`transform` variant covers and this port does not — is out of scope here.

Three things the spec does **not** do, all worth knowing before pointing a pipeline at a real table:

- **`aggregate_columns` needs the table column to agree, and finds out at the first commit.** The
  aggregate *function* is declared on the table column (`{"name": "i", "aggregate": "sum"}`); the
  spec only names which columns to flag. Nothing pairs the two before the pipeline runs — the sink
  validates its columns against the input *stream* schema, and its controller reads no more of the
  table than `tablet_count` and `type`. Point this variant at a table whose `i` is **not** declared
  `aggregate` and the pipeline reaches `working` normally, then every job dies on its first epoch
  commit:

  ```
  E  PublicFlowController  Job failed (ComputationId: reader)
  Commit attempt failed, error is not retryable
    Error committing transaction …
      Error preparing rows for table …/output_table
        "aggregate" flag is set for value in column "i" which is not aggregating
  ```

  The batch is rejected whole, so nothing is written at all — the output table stays empty. The job
  is then recreated on the same partition and fails again about every 20 s, and the pipeline sits
  in `working` for as long as you let it (13 failures over the five minutes this was run). Loud,
  not silent, and diagnosable: `yt flow describe-pipeline` carries the whole error chain under a
  `warning`-level `Job failed (JobFinishReason: Failed)` entry. What it is not is early — nothing
  catches the mismatch at submit, and `Jobs status` keeps reporting
  `WorkingWithRetryableError: 0`.
- **Nothing relates the writing computation's key to the table's key.** The sink checks that the
  filtered columns exist in the stream schema and nothing else; it never looks at the table's sort
  columns. A `group_by_schema` that does not contain the table key is accepted, and then two
  partitions can modify the same row from two transactions. For `aggregate` that is fine by
  construction (addition commutes); for a plain write it is last-commit-wins with no ordering
  guarantee between partitions.
- **The output table's tablet count feeds partitioning.** `TSinkController` polls the table's
  `tablet_count` every `update_partition_count_period` (60 s by default) and reports it as the
  sink's receiver channel count, which is one of the proposals the auto-partitioner weighs for the
  computation that owns the sink (the "sink channels" criterion in `universal_controller.cpp`).
  Widening the sink table is therefore also a way to widen its writer. Not observable in this
  scenario — one queue tablet, one output tablet, one worker, one partition everywhere.

### Against Flink

Flink's counterpart is an **upsert table sink** — `upsert-kafka`, JDBC, HBase, Elasticsearch — fed
by a changelog stream. Three differences a migrating user should expect.

**Who decides the modification kind.** In Flink it travels with the row: the planner tags each
record `INSERT` / `UPDATE_AFTER` / `DELETE` (`RowKind`), and one sink applies all of them; the key
comes from the DDL's `PRIMARY KEY … NOT ENFORCED`. In Flow the kind is a spec flag on the sink and
applies to every message it sees, and the key is simply whatever the target table's sort columns
are. A Flink changelog with mixed kinds has to be split into two Flow streams with two sinks.

**Where the aggregation runs.** Flink has no analogue of `aggregate_columns`: a running per-key sum
is a keyed aggregate operator whose state is checkpointed, emitting a retract or upsert stream into
the sink. Flow pushes it into the storage engine, so the pipeline holds no state for it — which is
why the `aggregate` run leaves `states` empty. The trade is real in both directions: nothing to
size, checkpoint or restore, but also no way for the pipeline to read the running value, and the
function is limited to the aggregates YT columns support (`sum` here). It also means the storage
engine is the only thing that validates the arrangement, at commit time — see the first bullet
above.

**Kafka Streams** maps more directly on the delete side: `delete_rows` is a `KTable` tombstone, a
null value against the key.

## Run

Each variant is a separate pipeline in a separate Cypress subtree
(`$YT_DEV_ROOT/sorted_dynamic_table/<variant>/`), because they differ in the *static* part of the
spec — and `aggregate` also needs a different output-table schema. Run them one at a time:

```bash
python3 yt_sync.py   swift      # once per variant: pipeline node, input_queue + consumer, output_table
python3 prepare_data.py swift   # 6994 queue rows over 1000 keys

FLOW_BIN=~/ytsaurus/yt/yt/flow/bin/flow_server/flow_server.stripped ./run.sh swift
```

and the same three commands with `delete` or `aggregate` in place of `swift`. For `delete`,
`prepare_data.py` additionally seeds `output_table` with 1001 rows, `payload_0 … payload_1000` —
one key more than the stream carries, so the survivor proves the sink deleted the keys it saw and
not the table.

Set `FLOW_BIN` to the **stripped** server. The runner uploads that exact file on every deploy, and
an unstripped build is gigabytes — the repo README's default path points at the build output, not
at a stripped copy.

`run.sh` returns on its own when the pipeline completes — the source is finite; budget one to two
and a half minutes. Its first seconds look alarming and are not: 27-45
`E ... Failed to update pipeline` from the runner (it polls until the controller publishes
`leader_controller_address`), one `E ... Failed to confirm leader_controller_address` and three
`W ... Component became broken` (`/build_cache`, `/collect_feedback`, `/update_metrics`) from the
controller, all recovered within five seconds, then `W ... Some computations has partial traverse
coverage (Computations: [reader])` four times at 5 s intervals from the moment the pipeline reaches
`working`, silent for the last ~20 s. One more reading aid: the runner's own lines carry the host's
local time while the controller's lines come from the vanilla job and carry UTC, so a three-hour
jump in the middle of the stream is the timezone, not a stall.

Then check the output, and finally `./stop.sh <variant>` to abort the vanilla operation (the
pipeline is already `completed`, a final state, so there is nothing to stop).

```bash
V=swift   # or delete, or aggregate
T="$YT_DEV_ROOT/sorted_dynamic_table/$V"

yt flow get-pipeline-state "$T/pipeline"

# swift: one row per key, every key present exactly once, and the value of the last message.
yt select-rows "data, i from [$T/output_table]" --format json | python3 -c '
import json, sys
rows = [json.loads(l) for l in sys.stdin]
got = sorted(r["data"] for r in rows)
print("rows:", len(got), "matches expected:", got == sorted("payload_%d" % i for i in range(1000)))
stale = [r for r in rows if r["i"] != int(r["data"].split("_")[1]) % 13]
print("rows where i is not the last message of its key:", len(stale))'

# delete: only the key the stream never carried is left.
yt select-rows "* from [$T/output_table]" --format json

# aggregate: the value column is the sum over each key's messages.
yt select-rows "data, i from [$T/output_table]" --format json | python3 -c '
import json, sys
got = {r["data"]: r["i"] for r in (json.loads(l) for l in sys.stdin)}
expected = {"payload_%d" % i: sum(range((i % 13) + 1)) for i in range(1000)}
print("rows:", len(got), "matches expected:", got == expected)'

# Upstream's secondary assertion, for every variant: the pipeline keeps no state.
yt select-rows "* from [$T/pipeline/states] limit 1" --format json
```

## Observed output

Recorded against the server build `run.sh` prints on the way in:

```
flow_server: 26.2.0-local-os~5c69dd1804e43fe5
```

Each of the three runs ended with, and exited 0 on (cluster URL and Cypress root elided):

```
I	FlowClient	Pipeline completed (Pipeline: <…>/sorted_dynamic_table/<variant>/pipeline)
```

```
$ python3 prepare_data.py swift
inserted 6994 rows into …/sorted_dynamic_table/swift/input_queue (1000 distinct keys)

$ yt flow get-pipeline-state "…/sorted_dynamic_table/swift/pipeline"
completed

$ yt select-rows "sum(1) as cnt from [.../swift/output_table] group by 1" --format json
{"cnt":1000}

rows: 1000 matches expected: True          # data column == sorted(payload_0 … payload_999)
rows where i is not the last message of its key: 0
```

```
$ python3 prepare_data.py delete
inserted 1001 rows into …/sorted_dynamic_table/delete/output_table
inserted 6994 rows into …/sorted_dynamic_table/delete/input_queue (1000 distinct keys)

$ yt flow get-pipeline-state "…/sorted_dynamic_table/delete/pipeline"
completed

$ yt select-rows "* from [.../delete/output_table]" --format json
{"data":"payload_1000","i":1000}
```

```
$ yt flow get-pipeline-state "…/sorted_dynamic_table/aggregate/pipeline"
completed

rows: 1000 matches expected: True          # i == sum(range((n % 13) + 1)) for every payload_n

$ yt select-rows "data, i from [.../aggregate/output_table] where data in (\"payload_12\", \"payload_13\")" --format json
{"data":"payload_12","i":78}
{"data":"payload_13","i":0}
```

The same two keys out of the `swift` run read `{"i":12}` and `{"i":0}` — 13 messages collapsed to
the last one instead of summed.

`states` came back empty for all three, upstream's secondary assertion: none of these variants
keeps per-key state in the pipeline, the target table is the only state there is.

Timings, one worker, the stock binary already in the cluster's file cache (a first deploy of a
binary the cluster has not seen adds its ~200 MB upload on top):

| variant | runner start | vanilla operation | `working` | `completed` |
|---------|--------------|-------------------|-----------|-------------|
| `swift` | 12:01:11 | 12:01:12 | 12:01:45 | 12:02:23 |
| `delete` | 12:03:30 | 12:03:32 | 12:04:04 | 12:04:42 |
| `aggregate` | 12:05:10 | 12:05:12 | 12:06:49 | 12:07:27 |

The pipeline itself takes **38 s in all three** — the same 6994 rows, the same graph, and the
sink's mode makes no measurable difference. What varies is how long YT takes to start the jobs:
33 s, 32 s, then 97 s for the third run.

## Rerunning

`completed` is a final state that refuses both `stop-pipeline` and a spec update, so a repeat run
means recreating that variant's subtree. Drop the queue's consumer registration **before** deleting
the nodes it names, or you can be locked out of dropping it for a while. Deleting and recreating
the pair at the same paths leaves the proxy's table mount cache holding the *old* table ids, and
`unregister-queue-consumer` checks `remove` permission on the ids the cache hands it, so it fails
on ids nothing resolves any more:

```
Error resolving path #5c-12103-10191-ef9dd740
    No such object 5c-12103-10191-ef9dd740
```

The registration row itself holds paths, not ids — the `#<id>` comes from the client — so this is
cache staleness, the same family as the `Tablet … is not known` reads below, not a broken row.
Deleting the recreated nodes again makes the unregister succeed, because with neither path
resolvable the client skips the permission check entirely; how long simply waiting takes was not
measured. So:

```bash
V=swift
./stop.sh "$V"
yt unregister-queue-consumer "$YT_DEV_ROOT/sorted_dynamic_table/$V/input_queue" \
                             "$YT_DEV_ROOT/sorted_dynamic_table/$V/consumer"
yt remove -r "$YT_DEV_ROOT/sorted_dynamic_table/$V"
python3 yt_sync.py "$V" && python3 prepare_data.py "$V"
FLOW_BIN=~/ytsaurus/yt/yt/flow/bin/flow_server/flow_server.stripped ./run.sh "$V"
```

Recreating the tables invalidates the proxies' mount cache, so the first `insert-rows` or
`select-rows` afterwards can fail with `Tablet … is not known` / `No such object <id>`; repeat it a
few seconds later.

## Differences from the integration test this is ported from

Upstream: `yt/yt/flow/tests/sorted_dynamic_table/` (`test.py`, `pipeline/pipeline_swift.yson`,
`pipeline/pipeline_delete.yson`, `pipeline/pipeline_aggregate.yson`, `pipeline/main.cpp`) in the
ytsaurus repo.

- **No user code, where upstream has a pipeline binary — because the fan-out was moved into the
  input data.** Upstream's `TReader` is a `TSwiftOrderedSourceComputation` that reads one queue row
  `{data, repeat}` and emits `repeat` messages `{data, i}`. That fan-out is how the test *shapes its
  input*; it is not a property of the sink under test. This port writes those messages into the
  queue directly (`prepare_data.py`), so the stock `TSwiftPassthroughOrderedSourceComputation`
  delivers the identical message stream to the sink and all three assertions survive verbatim on a
  stock server. What changes is only where the messages are cut into source messages: upstream
  produces `repeat` messages from one queue row, here each is its own row with its own offset.
  Neither the upsert, the delete nor the sum depends on that grouping. One property is genuinely
  lost, though: upstream's fan-out is user code in a *swift* source, so replaying a partition has
  to re-expand one row into the same `repeat` messages in the same order — the determinism that
  swift exactly-once rests on. Here the expansion is already in the queue, so nothing exercises it.
  That costs nothing in this run only because no fault is injected and no partition is ever
  replayed.
- **Three of the five upstream spec variants are ported: `swift`, `delete`, `aggregate`.**
  - `transform` (upstream `transform_2c_4w_unstable`) is skipped, and it is the most substantive
    omission here. It hangs the same sink off a `TPassthroughComputation` keyed by
    `group_by_schema = [farm_hash(data), data]`, so the table is written by a keyed, repartitioned
    computation — **several partitions modifying one sorted table**, which is precisely the hazard
    the second "what the spec does not do" bullet above raises and precisely what these three
    variants do not cover. It is skipped because upstream runs it only in the 4-worker /
    2-controller fault-injecting configuration, which this repo cannot reproduce; a stable
    single-controller port of it would be a worthwhile scenario of its own.
  - `async_replica` is skipped: it needs a replicated table with an async replica on a second
    cluster. Its subject is the sink's `require_sync_replica` parameter (`%true` by default, left
    at the default here), which upstream turns off to write through to an async replica.
- **Only the stable single-worker configuration.** Upstream runs `swift` and `transform` with 4
  workers, 2 controllers and `problems=True` — its harness kills and restarts processes mid-run,
  which is what turns "the sums are exact" into an exactly-once claim. This repo has no fault
  injection, so all three variants run 1 controller / 1 worker on the happy path.
- **1000 events, the upstream default** (upstream drops to 200 under sanitizers;
  `prepare_data.py` takes the event count as its optional second argument). 6994 queue rows follow
  from it.
- **The output table is created with `aggregate: sum` only in the aggregate variant**, exactly as
  upstream's `yt_sync.py` does with its `aggregate` flag.
- **The sink's `table_path` is a plain path, not a `<cluster=…>` rich path.** Upstream writes
  `<cluster=primary>` because its harness runs a federation; a bare path uses the pipeline's own
  client, which is what a single-cluster deployment wants. The queue source keeps the rich form,
  since the source resolves its cluster through `proxy_url_aliasing_rules` like every other
  scenario in this repo.
- **`batch_duration = 100` (ms) instead of the 1 s default**, so the demo drains quickly; raise it
  for anything with real throughput.
- **The input queue has no `flow_queue_meta` column.** Upstream's `yt_sync.py` creates one, but
  nothing in either pipeline writes it. Dropping it is safe *not* because the parsing is off —
  `try_parse_flow_queue_meta` defaults to `%true` — but because the source looks the column up by
  name and simply has nothing to parse when it is absent. Add the column back the moment anything
  upstream starts writing event time into the queue.
