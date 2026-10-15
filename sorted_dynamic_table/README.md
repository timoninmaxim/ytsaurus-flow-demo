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
spec — and `aggregate` also needs a different output-table schema. Run them one at a time.

Needs the prerequisites and the sourced env file from the root README (`yt` CLI,
`ytsaurus-flow-yt-sync-mini` and `jinjanate` in one Python environment, podman).

Terminal 1 — from this dir, for the variant `V` (`swift`, `delete` or `aggregate`):

```bash
V=swift   # or delete, or aggregate
python3 yt_sync.py "$V"        # once per variant: pipeline node, queue, table
python3 prepare_data.py "$V"   # 6994 queue rows over 1000 keys
jinjanate "pipeline_$V.yson.j2" > pipeline.yson   # every {{ VAR }} from the env; an unset one fails the render
podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" ghcr.io/ytsaurus/flow-nightly:dev-0.2.1 --config pipeline.yson
```

For `delete`, `prepare_data.py` additionally seeds `output_table` with 1001 rows,
`payload_0 … payload_1000` — one key more than the stream carries, so the survivor proves the sink
deleted the keys it saw and not the table.

Unlike the endless scenarios, this command returns on its own here: the source is finite, so the
runner waits for `completed` and exits. Budget about a minute, plus the image pull the first time a
node runs this release.

Terminal 2 — check the output, then `./stop.sh "sorted_dynamic_table/$V"` from the repo root to
abort the vanilla operation (the pipeline is already `completed`, a final state, so there is
nothing to stop):

```bash
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
./stop.sh "sorted_dynamic_table/$V"
yt unregister-queue-consumer "$YT_DEV_ROOT/sorted_dynamic_table/$V/input_queue" \
                             "$YT_DEV_ROOT/sorted_dynamic_table/$V/consumer"
yt remove -r "$YT_DEV_ROOT/sorted_dynamic_table/$V"
python3 yt_sync.py "$V" && python3 prepare_data.py "$V"
jinjanate "pipeline_$V.yson.j2" > pipeline.yson
podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" ghcr.io/ytsaurus/flow-nightly:dev-0.2.1 --config pipeline.yson
```

Recreating the tables invalidates the proxies' mount cache, so the first `insert-rows` or
`select-rows` afterwards can fail with `Tablet … is not known` / `No such object <id>`; repeat it a
few seconds later.
