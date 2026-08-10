# yql_map

The user logic of this pipeline is a single YQL `SELECT` — no user C++ at all:

```sql
SELECT String::AsciiToUpper(user) AS key, CAST(value * 2 AS Int64) AS value
FROM Input WHERE value > 0
```

`reader` (`TSwiftPassthroughOrderedSourceComputation` over `TQueueSource`) → `mapper`
(`TProcessFunctionComputation` hosting `TYqlYsonProcessFunction`, the row-wise YQL process
function) → `TSyncQueueSink`. Input rows carry a YSON payload `{user; value}` in the `data`
column; the query filters out non-positive values, uppercases the user, doubles the value, and
`key_columns = ["key"]` re-keys each output message by the uppercased user. The query is compiled
once at job start via PureCalc (LLVM-free interpreter) and runs in-process in the worker.

## Opensource gap

The stock `flow_server` does not register the YQL process functions; they live in the YQL
computation extension, whose bundled binary is `yql_flow_server`. That extension is **not present
in the opensource [ytsaurus](https://github.com/ytsaurus/ytsaurus) repo** (and one of its
dependencies, `yt/yql/purecalc`, is not exported either), so this scenario currently cannot be
built by an external engineer — `run.sh` requires an explicit `FLOW_BIN` pointing at a
`yql_flow_server` binary obtained elsewhere. Everything else (spec, bootstrap, verification) is
plain opensource Flow.

Because the binary comes from the internal build, the vanilla block sets `"network_project" = #`:
the internal build defaults the operation's network project to a Yandex-internal value that does
not exist on an opensource cluster (the opensource build has no such default), and the entity
value disables it.

## Run

Terminal 1 — bootstrap once, then run the pipeline:

```bash
python3 yt_sync.py                       # once: pipeline node, input_queue + consumer, output_queue
FLOW_BIN=/path/to/yql_flow_server ./run.sh   # deploy + stream the controller log; Ctrl-C detaches
```

Terminal 2 — feed the input queue and watch the output:

```bash
echo '{"data": "{user=alice;value=4}"}
{"data": "{user=bob;value=0}"}
{"data": "{user=carol;value=3}"}' | yt insert-rows --format json "$YT_DEV_ROOT/yql_map/input_queue"

yt select-rows "key, data from [$YT_DEV_ROOT/yql_map/output_queue]" --format json
```

Expected output — `bob` is dropped (`value = 0`), the rest are uppercased and doubled:

```json
{"key": "ALICE", "data": "{\"key\"=\"ALICE\";\"value\"=8}"}
{"key": "CAROL", "data": "{\"key\"=\"CAROL\";\"value\"=6}"}
```

Nothing consumes the output queue, so repeated `select-rows` always shows everything written so
far. `./stop.sh` stops the pipeline and aborts the vanilla operation.
