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
`pure_heartbeat = %true`) — but they make the watermark of this run readable end to end: it starts
at `0`, jumps to each table's own event timestamp while that table is being handed out
(`1500000000`, then `1600000000`), and once the source has nothing left to hand out it jumps to
**now minus `watermark_delay`** (default one hour) — the idle tail that lets downstream windows
close, the same role Flink's source idleness plays.

## Run

Needs the prerequisites and the sourced env file from the root README (`yt` CLI,
`ytsaurus-flow-yt-sync-mini` and `jinjanate` in one Python environment, podman).

Terminal 1 — from this dir: bootstrap the Cypress objects once, fill the input directory, render the
spec, launch:

```bash
python3 yt_sync.py        # once: pipeline node + output_queue
python3 prepare_data.py   # the two input tables, 1000 rows each
jinjanate pipeline.yson.j2 > pipeline.yson   # every {{ VAR }} from the env; an unset one fails the render
podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" ghcr.io/ytsaurus/flow-nightly:dev-0.2.1 --config pipeline.yson
```

`prepare_data.py` must run **before** the launch — the source is finite, so anything that is not in
the directory by the time it drains is not part of the assertion. It takes an optional row count per
table; the verification snippet below assumes the default 1000.

Unlike the endless scenarios, this command returns on its own here: the source is finite, so the
runner waits for `completed` and exits. Budget about a minute, plus the image pull the first time a
node runs this release.

Terminal 2 — check the output, then `./stop.sh static_table` from the repo root to abort the vanilla
operation (the pipeline is already `completed`, a final state, so there is nothing to stop):

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

```
$ yt flow get-pipeline-state "$YT_DEV_ROOT/static_table/pipeline"
completed

$ yt select-rows "sum(1) as cnt from [...output_queue] where not is_null(data) group by 1" --format json
{"cnt":2000}

rows: 2000 duplicates: 0
matches expected (data + event time): True

$ yt select-rows "flow_queue_meta from [...output_queue] where is_null(data)" --format json
{"flow_queue_meta":{"event_timestamp_deltas":[],"event_watermark":0,"pure_heartbeat":true}}
{"flow_queue_meta":{"event_timestamp_deltas":[],"event_watermark":1500000000,"pure_heartbeat":true}}
{"flow_queue_meta":{"event_timestamp_deltas":[],"event_watermark":1600000000,"pure_heartbeat":true}}
{"flow_queue_meta":{"event_timestamp_deltas":[],"event_watermark":1789306370,"pure_heartbeat":true}}
```

Upstream's two secondary assertions hold too. The pipeline's `states` table is empty, and — the part
that is specific to a *swift* source — the source's partitions were cleaned up on completion, so no
`layout_partitions` row is left with a value:

```
$ yt select-rows "sum(1) as cnt from [$YT_DEV_ROOT/static_table/pipeline/states] group by 1" --format json
(no rows)

$ yt select-rows "sum(1) as cnt from [$YT_DEV_ROOT/static_table/pipeline/flow_state] where state_name = \"layout_partitions\" and not is_null(value) group by 1" --format json
(no rows)
```

## Stop

From the repo root:

```bash
./stop.sh static_table
```

The pipeline is already `completed` (a final state), so this only aborts the vanilla operation.

## Rerunning

`completed` is a final state that refuses both `stop-pipeline` and a spec update, so a repeat run
means recreating the scenario. This is the cheapest scenario in the repo to recreate — there is no
queue consumer to unregister first (see `state_joiner`), because the input is not a queue:

```bash
./stop.sh static_table
yt remove -r "$YT_DEV_ROOT/static_table"
python3 yt_sync.py && python3 prepare_data.py
jinjanate pipeline.yson.j2 > pipeline.yson
podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" ghcr.io/ytsaurus/flow-nightly:dev-0.2.1 --config pipeline.yson
```

Recreating the output queue invalidates the proxies' table mount cache, so the first `select-rows`
afterwards can fail with `Tablet … is not known` / `No such object <id>`; repeat it a few seconds
later.
