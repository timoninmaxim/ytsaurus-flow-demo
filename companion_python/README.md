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

`main.py` is both the launcher and the companion: the Python SDK's `app.run()` picks the mode.

**Restriction:** the launcher ships only `main.py`, so the pipeline's Python code must fit in that
one file. Alternative is ship all required files and dependencies into a docker image used for vanilla
jobs.

The launcher prepares the pipeline for start: it enriches the spec with the user file — `main.py`
goes to the worker as the companion, and the `CompanionManager` resource is pointed at it — then
hands the spec to the image's `flow_server`.

## Run

Needs the prerequisites and the sourced env file from the root README (`yt` CLI,
`ytsaurus-flow-yt-sync-mini` and `jinjanate` in one Python environment, podman).

Terminal 1 — from this dir: bootstrap the Cypress objects once, render the spec, launch through the
Python SDK launcher:

```bash
python3 yt_sync.py   # once: pipeline node, input_queue + consumer, output_queue
jinjanate pipeline.yson.j2 > pipeline.yson   # every {{ VAR }} from the env; an unset one fails the render
podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" \
    ghcr.io/ytsaurus/flow-python-nightly:dev-0.2.1 main.py --config pipeline.yson
```

The launch streams the controller log; Ctrl-C only detaches, the pipeline keeps running.

Terminal 2 — feed the input queue and read the output:

```bash
echo '{"key": "a", "text": "hello flow", "count": 1, "score": 0.5, "flag": true}
{"key": "b", "text": "python companion", "count": -7, "score": 2.25, "flag": false}' \
    | yt insert-rows --format json "$YT_DEV_ROOT/companion_python/input_queue"

yt select-rows "key, text, [count], score, flag, text_upper from [$YT_DEV_ROOT/companion_python/output_queue]" --format json
```

Within a few seconds every row comes back with all columns mirrored and `text_upper` filled in by
the Python function:

```json
{"key":"a","text":"hello flow","count":1,"score":0.5,"flag":true,"text_upper":"HELLO FLOW"}
{"key":"b","text":"python companion","count":-7,"score":2.25,"flag":false,"text_upper":"PYTHON COMPANION"}
```

Nothing consumes the output queue, so it keeps every row: insert more and read again.

## Stop

Stop the pipeline, then abort its vanilla operation — the runner printed its id at launch, in the
`Started vanilla operation (..., OperationId: <id>)` line:

```bash
yt flow stop-pipeline "$YT_DEV_ROOT/companion_python/pipeline"
yt abort-op <OperationId>
```

To drop the scenario's Cypress objects as well: `yt remove -r "$YT_DEV_ROOT/companion_python"`.
