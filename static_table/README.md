# static_table

Reading **plain static YT tables** as a stream source. A directory holds two ordinary tables; the
pipeline reads both to their end and stops. Nothing here is custom — no companion, no pipeline
binary of its own, not one line of user code: the whole scenario is a spec over the stock
`flow_server`.

```
reader (stock TSwiftPassthroughOrderedSourceComputation
        over NStaticTableConnector::TSource, tables_path, finite = %true)
   → data → NYT::NFlow::TSyncQueueSink → output_queue
```

The input is `input/2017-07-14T02:40:00` and `input/2020-09-13T12:26:40`, 1000 rows each
(`data = payload_first_00000 …`, `payload_second_00999`). The assertion is that the output queue
holds exactly those 2000 rows, each carrying the event timestamp of the table it came from, and
that the pipeline reaches `completed` on its own.

That also makes it one of the few scenarios here with a plausible chance of running on a
**published** `flow_server` artifact rather than a build from a recent checkout: every class and
spec field it names is long-standing, and `connectors/static_table` is unconditionally linked into
`bin/flow_server`. The companion scenarios in this repo cannot say that — they need a checkout at or
after 2026-08-06 for the C++ companion classes. Not verified against a release artifact, and it is a
claim about the *pipeline* only: this repo's deployment path pins the RPC proxy through
`clients_cache` and may still want a newer runner.

## The subject: a finite source, and event time that comes from a table name

Two things make this connector different from the queue source the other scenarios use.

**It is bounded.** `finite = %true` is what lets the source mark its stream `Completed` once it has
no rows left to hand out; the pipeline then drains and reaches `completed`, a final state. There is
no "keep polling" here; "done" is a real, observable event. The same connector with
`finite = %false` becomes a *continuously monitored directory*: the controller keeps listing
`tables_path` and starts a new partition for every table that appears, which is how a directory of
dated tables is turned into an endless stream.

**Event time comes from the table's name, not from the rows.** The rows have no timestamp column at
all. `event_timestamp_locator` defaults to `{attribute = "key"; format = "iso8601"}` — `key` is the
Cypress attribute holding the node's own name — so every row of `input/2017-07-14T02:40:00` gets
event timestamp `1500000000`. That convention is the whole point of the connector: a directory whose
table names are ISO 8601 instants *is* an event-time-ordered stream, and the source sorts the tables
by that timestamp before reading them. (`system_timestamp_locator` defaults to the node's
`creation_time`, which is what "how late is this table" is measured against.) `format` also accepts
`seconds` / `milli_seconds` against a numeric attribute — note the underscore, that is how the
`MilliSeconds` enumerator is spelled in YSON — and `attribute` can name any attribute at all; a
value the chosen format cannot parse is an error out of the source's table listing, not a skipped
table.

The directory is read strictly, too: a child that is not a table — a symlink, a nested map node —
fails the listing rather than being ignored, unless `skip_non_table_nodes = %true` (or
`ignore_symlinks` for the symlink case alone). This scenario's `input/` holds nothing but tables;
point the source at a directory that is also used for anything else and that is the first thing to
set.

Flink's nearest equivalent is `FileSource`: `forRecordStreamFormat(...).build()` is the bounded
form, `.monitorContinuously(Duration)` the unbounded one, and the switch between them is exactly
Flow's `finite`. Two differences a migrating user should know. First, Flink derives event time from
the *record*, through a `WatermarkStrategy` you attach to the source; deriving it from the file name
means writing a custom `FileEnumerator` or splitting on the path yourself. Flow makes the file-name
route the default and has no per-record option on this connector. Second, Flink's file enumerator
has no notion of ordering between the files it discovers — it hands out splits and the watermark
comes from the records. Flow's source sorts tables by an ordering key,
`(era, event timestamp, system timestamp, path)`, reads them in that order, and drops any table
whose key sorts before the last one it started — so a table dropped into the directory under an
older name is simply never read.

### Seeing the event time and the watermark

The reader is a plain passthrough, so it does not put the event timestamp into the payload — event
time is message *metadata*, not data. The sink flag `write_flow_queue_meta = %true` is what makes it
visible: `TSyncQueueSink` then writes each row's metadata into an extra `any` column,
`flow_queue_meta`, next to the payload.

```json
{"data":"payload_first_00000","flow_queue_meta":{"event_timestamp":1500000000,"event_timestamp_deltas":[]}}
```

That flag has a second effect worth knowing before you switch it on: the sink's **controller** also
writes a heartbeat row into the queue every 10 s, carrying the current event watermark and no
payload at all. Those rows are the other half of a pair: a queue carries no metadata of its own, so
this is the column a downstream `TQueueSource` reads back with `try_parse_flow_queue_meta = %true`
to recover event time and watermarks across the queue. Here nothing consumes the queue, so they are
just noise to filter out (`data` is null on every one of them, and they carry
`pure_heartbeat = %true`) — but they make the watermark of this run readable end to end:

```
{"flow_queue_meta":{"event_watermark":0,"pure_heartbeat":true}}
{"flow_queue_meta":{"event_watermark":1499996400,"pure_heartbeat":true}}
{"flow_queue_meta":{"event_watermark":1600000000,"pure_heartbeat":true}}
{"flow_queue_meta":{"event_watermark":1786433121,"pure_heartbeat":true}}
```

The two ends of that progression are the source's own watermark rule, `watermark_delay` (default one
hour) behind whatever it is sure about: while it is handing out a table it reports that **table's**
event timestamp minus the delay — `1499996400` is `1500000000 - 3600` — and once it has nothing left
to hand out it reports **now** minus the delay, which is what the trailing values are (the last one
here was written at 08:25:21 and reads 07:25:21). That idle jump is what lets downstream windows
close, the same role Flink's source idleness plays.

The middle value does **not** follow that rule, and it is worth being explicit about it: while the
second table was being handed out the rule predicts `1600000000 - 3600 = 1599996400`, and the run
recorded `1600000000`. The rule is the *source's* watermark; what the sink writes is the watermark
of its own input stream, one hop downstream. This scenario does not establish what that hop does, so
treat the source rule as explaining the two ends of the progression and not the middle.

## Run

From the repo root:

```bash
python3 static_table/yt_sync.py        # once: pipeline node + output_queue
python3 static_table/prepare_data.py   # the two input tables, 1000 rows each

FLOW_BIN=~/ytsaurus/yt/yt/flow/bin/flow_server/flow_server.stripped ./run.sh static_table
```

`prepare_data.py` must run **before** `run.sh` — the source is finite, so anything that is not in
the directory by the time it drains is not part of the assertion. It takes an optional row count per
table; the verification snippet below assumes the default 1000.

Set `FLOW_BIN` to the **stripped** server. The runner uploads that exact file on every deploy, and
an unstripped build is gigabytes — the repo README's default path points at the build output, not at
a stripped copy.

`run.sh` returns on its own when the pipeline completes — budget about two minutes. Its first
seconds look alarming and are not: eight `E ... Failed to update pipeline` from the runner
(`leader_controller_address` is not set until the controller publishes itself), one
`E ... Failed to confirm leader_controller_address` and three `W ... Component became broken`
(`/build_cache`, `/collect_feedback`, `/update_metrics`) from the controller, all recovered within
five seconds. `W ... Some computations has partial traverse coverage (Computations: [reader])` then
appears every 5 s **in bursts while a table is being taken up** — seven lines here, in two runs of
08:23:57–08:24:12 and 08:24:42–08:24:52, one per table — and is silent in between and after. (In an
endless pipeline the same warning never stops; that is the shape the other scenarios record.)

Then check the output, and finally `./stop.sh static_table` to abort the vanilla operation (the
pipeline is already `completed`, a final state, so there is nothing to stop):

```bash
yt flow get-pipeline-state "$YT_DEV_ROOT/static_table/pipeline"

# Data rows only — the heartbeat rows have a null payload.
yt select-rows "sum(1) as cnt from [$YT_DEV_ROOT/static_table/output_queue] where not is_null(data) group by 1" --format json

# The full assertion: every expected payload, exactly once, with the event time of its table.
yt select-rows "data, flow_queue_meta from [$YT_DEV_ROOT/static_table/output_queue] where not is_null(data)" --format json | python3 -c '
import json, sys
expected = {"payload_%s_%05d" % (a, i): t
            for a, t in (("first", 1500000000), ("second", 1600000000))
            for i in range(1000)}
actual, dups = {}, 0
for line in sys.stdin:
    r = json.loads(line)
    dups += r["data"] in actual
    actual[r["data"]] = r["flow_queue_meta"]["event_timestamp"]
print("rows:", len(actual), "duplicates:", dups)
print("matches expected (data + event time):", actual == expected)'

# The watermark, as the sink's heartbeats recorded it.
yt select-rows "flow_queue_meta from [$YT_DEV_ROOT/static_table/output_queue] where is_null(data)" --format json
```

## Observed output

Recorded against the server build `run.sh` prints on the way in:

```
flow_server: 26.2.0-local-os~5c69dd1804e43fe5
```

`run.sh` ends with, and exits 0 on (cluster URL and Cypress root elided):

```
I	FlowClient	Pipeline completed (Pipeline: <…>/static_table/pipeline)
```

```
$ yt flow get-pipeline-state "$YT_DEV_ROOT/static_table/pipeline"
completed

$ yt select-rows "sum(1) as cnt from [...output_queue] where not is_null(data) group by 1" --format json
{"cnt":2000}

rows: 2000 duplicates: 0
matches expected (data + event time): True

$ yt select-rows "flow_queue_meta from [...output_queue] where is_null(data)" --format json
{"flow_queue_meta":{"event_timestamp_deltas":[],"event_watermark":0,"pure_heartbeat":true}}
{"flow_queue_meta":{"event_timestamp_deltas":[],"event_watermark":0,"pure_heartbeat":true}}
{"flow_queue_meta":{"event_timestamp_deltas":[],"event_watermark":1499996400,"pure_heartbeat":true}}
{"flow_queue_meta":{"event_timestamp_deltas":[],"event_watermark":1499996400,"pure_heartbeat":true}}
{"flow_queue_meta":{"event_timestamp_deltas":[],"event_watermark":1499996400,"pure_heartbeat":true}}
{"flow_queue_meta":{"event_timestamp_deltas":[],"event_watermark":1499996400,"pure_heartbeat":true}}
{"flow_queue_meta":{"event_timestamp_deltas":[],"event_watermark":1600000000,"pure_heartbeat":true}}
{"flow_queue_meta":{"event_timestamp_deltas":[],"event_watermark":1600000000,"pure_heartbeat":true}}
{"flow_queue_meta":{"event_timestamp_deltas":[],"event_watermark":1786433121,"pure_heartbeat":true}}
…
```

`data` + event time match the upstream test's `expected_output` exactly: 1000 `payload_first_*` rows
at `1500000000` and 1000 `payload_second_*` rows at `1600000000`, each exactly once. The number of
heartbeat rows is not fixed — the controller keeps writing one every 10 s for as long as the vanilla
operation lives, so it grows until `./stop.sh static_table` (16 by the time this run was aborted).

Upstream's two secondary assertions hold too. The pipeline's `states` table is empty, and — the part
that is specific to a *swift* source — the source's partitions were cleaned up on completion, so no
`layout_partitions` row is left with a value:

```
$ yt select-rows "sum(1) as cnt from [$YT_DEV_ROOT/static_table/pipeline/states] group by 1" --format json
(no rows)

$ yt select-rows "sum(1) as cnt from [$YT_DEV_ROOT/static_table/pipeline/flow_state] where state_name = \"layout_partitions\" and not is_null(value) group by 1" --format json
(no rows)
```

Timings for that run (one worker, the binary already in the cluster's file cache): `run.sh` 11:23:38
→ vanilla operation started 11:23:40 → pipeline `working` 11:23:57 → first table read 11:23:58 …
11:24:39 → second table 11:24:39 … 11:25:16 `completed`. Just under two minutes, of which the two
tables take about 40 s each — the rows themselves are nothing (1000 rows, 28 KB), the time goes on
jobs: each table got a partition of its own, and each partition was served by more than one
successive job. (The log `run.sh` streams stops the moment the pipeline completes, so it is not a
complete job census — the controller's own log on the cluster is.)

## Rerunning

`completed` is a final state that refuses both `stop-pipeline` and a spec update, so a repeat run
means recreating the scenario. This is the cheapest scenario in the repo to recreate — there is no
queue consumer to unregister first (see `state_joiner`), because the input is not a queue:

```bash
./stop.sh static_table
yt remove -r "$YT_DEV_ROOT/static_table"
python3 static_table/yt_sync.py && python3 static_table/prepare_data.py
FLOW_BIN=~/ytsaurus/yt/yt/flow/bin/flow_server/flow_server.stripped ./run.sh static_table
```

Recreating the output queue invalidates the proxies' table mount cache, so the first `select-rows`
afterwards can fail with `Tablet … is not known` / `No such object <id>`; repeat it a few seconds
later.

## Differences from the integration test this is ported from

Upstream: `yt/yt/flow/tests/static_table/` (`test.py`, `pipeline/pipeline_swift.yson`,
`pipeline/main.cpp`) in the ytsaurus repo.

- **No user code, where upstream has a pipeline binary.** Upstream's `TReader` is a
  `TSwiftOrderedSourceComputation` that does two things: copy the `data` column into the output
  message, and write `message.EventTimestamp` into an `event_time` payload column. The first is
  exactly what the stock `TSwiftPassthroughOrderedSourceComputation` does; the second is available
  spec-only through the sink's `write_flow_queue_meta`, which is where this scenario's
  `flow_queue_meta/event_timestamp` comes from. So the assertion survives verbatim while the binary
  stays stock — the best possible outcome for an opensource user, who otherwise needs an in-tree
  build for any custom class.
- **`tables_path` *and* `finite = %true`, a combination upstream never runs.** Upstream's default
  variant lists one table explicitly, and every one of its directory tests sets `finite = false` or
  drives the dynamic spec to force completion. Reading a whole directory to its end and stopping on
  its own is therefore exercised here and not by the reference test. The explicit form is a drop-in
  replacement: `"tables" = ["<cluster=…>…/input/2017-07-14T02:40:00"; …]`, and the two are mutually
  exclusive.
- **Both of upstream's tables are used** (`test_two_input_tables`), with upstream's timestamps
  (1.5e9, 1.6e9), aliases and 1000 rows each.
- **The input tables carry a schema.** Upstream writes them schemaless and lets the source infer
  column types from the rows, which it does. A strict `[{data string required}]` schema is what a
  real pipeline would have, and it removes the inference step from the picture.
- **The dynamic spec sets `desired_table_process_time = 1000`** (ms), as upstream's test does. The
  default is **one hour**: the source throttles itself to spread a table's rows over that window, so
  leaving it alone would make this demo take an hour. It is the first parameter to look at whenever
  a static-table pipeline seems to be doing nothing.
- **Only the base variant is ported.** Upstream's file has twelve test functions; this scenario
  merges the first two (`test_one_input_table`, whose two secondary assertions it keeps, and
  `test_two_input_tables`). The other ten split into three groups. (a) Column-type coverage —
  composite, weak-composite, strict/weak optional and YSON `any` `data` columns. Most of what those
  assert is upstream's `TReader` unwrapping the column, i.e. user code; but they also incidentally
  cover the *connector's* read path for weak, optional and `any` columns — type inference from the
  rows, null cells, `EValueType::Any` normalisation — and this port narrows that to one case, a
  strict `string required` column. Real coverage lost, not just user code. (b) Dynamic-spec surgery
  mid-run — `test_throttling`,
  `test_restart_table` (`restart_instant`), `test_removing_table` — each a separate subject worth
  its own scenario. (c) `test_extra_bad_source` and `test_table_directory`, which need a second,
  non-finite source or a table appearing while the pipeline runs.
- **`batch_duration = 100` (ms) instead of the 1 s default**, so the demo finishes quickly; raise it
  for anything with real throughput.
