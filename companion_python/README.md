# companion_python

A pipeline whose transform is written in **Python**, executed by the companion — a separate
Python process the worker spawns inside its own vanilla job and drives over gRPC:

`reader` (native `TSwiftPassthroughOrderedSourceComputation` over `TQueueSource`) → `mapper`
(`NCompanion::TTransformCompanionComputation` hosting the Python function from `main.py`) →
`TSyncQueueSink`.

The mapper mirrors every typed column (string, int64, double, boolean — the companion
wire-protocol type roundtrip) and adds `text_upper`, computed in Python, so the output visibly
proves the row went through the companion. The native `reader` is not registered in `main.py` at
all: native computations run in-process in the worker and never call the companion.

## How the companion gets into the job

In Arcadia the Python pipeline is built as one self-contained PY3_PROGRAM binary that doubles as
the runner: executed as `./my_pipeline --config pipeline.yson --flow-bin flow_server` it enriches
the spec (ships itself via `vanilla/worker/local_files`, points the `CompanionManager` resource's
`entrypoint` at the shipped copy) and hands off to `flow_server`.

Opensource has no PY3_PROGRAM, and the job image carries only a bare python3.8, so this scenario
does the same thing in two explicit pieces:

- `build.sh` packages `companion_bundle.tgz` on the dev host: a self-contained CPython runtime
  (the job image's python3.8 is too old for the SDK), the **`ytsaurus-flow-companion`** package —
  built as a wheel from `yt/yt/flow/tools/python_companion_package` in the checkout, it carries the SDK, the
  generated companion-protocol proto stubs, and the pinned gRPC/protobuf toolchain — with its
  dependencies resolved for the bundled runtime, and `main.py`.
- `pipeline.yson.template` states what the Arcadia runner would have patched in: the two
  `local_files` (the `py_companion` wrapper script + the bundle) and
  `entrypoint = {executable = "./py_companion"}` on the `CompanionManager` resource. The host
  side stays the stock `flow_server` runner. The companion gRPC port is pinned via
  `vanilla/node_config/companion/port`.

## Differences from the original tests

The demo folds two integration tests into one always-on pipeline:

- `tests/companion/passthrough_transform/python` — there the *source* is Python and the transform
  is native; its point is that a native computation completes without ever calling the companion.
  Here the roles are flipped (native source, Python transform); the native-bypass property is the
  same — `main.py` registers only `mapper`.
- `tests/companion/types/python` — the same `TTransformCompanionComputation` identity mapping over
  typed columns; the demo keeps the type roundtrip and adds the visible `text_upper` column.
- The tests run finite (`finite = %true`, wait for `completed`) with the companion binary spawned
  from a local path; the demo streams forever (`finite = %false`) with the companion delivered
  into the vanilla job as described above.

## Run

Terminal 1 — build the bundle, bootstrap, then run the pipeline (from the repo root):

```bash
companion_python/build.sh           # once per SDK/proto change: companion_bundle.tgz
python3 companion_python/yt_sync.py # once: pipeline node, input_queue + consumer, output_queue
./run.sh companion_python           # deploy + stream the log; Ctrl-C detaches,
                                    # ./stop.sh companion_python stops
```

Terminal 2 — feed the input queue and watch the output:

```bash
echo '{"key": "a", "text": "hello flow", "count": 1, "score": 0.5, "flag": true}
{"key": "b", "text": "python companion", "count": -7, "score": 2.25, "flag": false}' \
    | yt insert-rows --format json "$YT_DEV_ROOT/companion_python/input_queue"

yt pull-queue "$YT_DEV_ROOT/companion_python/output_queue" --offset 0 --partition-index 0 --format json
```

Every row comes back with all columns mirrored and `text_upper` filled in by the Python function.
Nothing consumes the output queue, so `--offset 0` always replays everything.

Recorded output (2026-08-10, ytdemo):

```json
{"$$tablet_index":0,"$$row_index":0,"key":"a","text":"hello flow","count":1,"score":0.5,"flag":true,"text_upper":"HELLO FLOW","$$timestamp":1918092881768218632,"$$cumulative_data_weight":55}
{"$$tablet_index":0,"$$row_index":1,"key":"b","text":"python companion","count":-7,"score":2.25,"flag":false,"text_upper":"PYTHON COMPANION","$$timestamp":1918092881768218632,"$$cumulative_data_weight":122}
{"$$tablet_index":0,"$$row_index":3,"key":"pip","text":"from pip package","count":100,"score":3.5,"flag":true,"text_upper":"FROM PIP PACKAGE","$$timestamp":1918123893579579418,"$$cumulative_data_weight":250}
```

(The last row was produced by a later run whose bundle was built from the `ytsaurus-flow-companion`
package.)

The rows had been inserted while an earlier deploy's companion was still crash-looping and were
delivered exactly once by the fixed deploy — nothing lost, nothing duplicated across the restarts.
