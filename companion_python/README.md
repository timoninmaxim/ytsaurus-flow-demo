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

`main.py` is both the launcher and the companion — the Python SDK's `app.run()` picks the mode.
Everything comes from the Flow 0.2.1 test release (see the repo README's "Released artifacts");
nothing is built from source:

- The launch runs `main.py --config pipeline.yson` in
  `ghcr.io/ytsaurus/flow-python-nightly:dev-0.2.1` (the released server plus
  `ytsaurus-flow-companion` 0.2.1 on `/usr/bin/python3`, the image's entrypoint). The launcher adds
  `main.py` to the worker's `local_files` as `py_companion`, points the `CompanionManager`
  resource's entrypoint at it, reserves the companion's ports, and execs the image's `flow_server`
  (`YT_FLOW_BIN`) with the enriched spec. That is why
  the spec declares `CompanionManager` with empty parameters and ships no files.
- Both vanilla tasks run in the same image, so the worker spawns `./py_companion` (the shebang is
  `/usr/bin/python3`) with the preinstalled SDK. `main.py` must stay a single file: the launcher ships
  only the script itself.

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

The last command enriches the spec with the companion, uploads the released `flow_server`,
launches the controller+worker vanilla operation and streams the controller log; Ctrl-C only
detaches, the pipeline keeps running.

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

From the repo root:

```bash
./stop.sh companion_python
```

It stops the pipeline and aborts its vanilla operation. To drop the scenario's Cypress objects as
well: `yt remove -r "$YT_DEV_ROOT/companion_python"`.
