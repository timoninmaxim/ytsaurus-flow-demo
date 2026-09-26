# docker_vanilla_companion

A Python computation running in a job image. The pipeline:
`reader` (`TSwiftPassthroughOrderedSourceComputation` over `TQueueSource`) → `mapper`
(`TTransformCompanionComputation`, implemented in `main.py`) → `TSyncQueueSink`.

The mapper mirrors every typed input column to the output stream (string, int64, double, boolean —
the companion wire-protocol type roundtrip) and adds `text_upper`, computed in Python.

## Why the job runs in the Python server image

The Python companion imports `yt.wrapper` / `yt.yson` / `yt.type_info` at startup, so it needs an
interpreter with `ytsaurus-client` and the companion SDK (Python >= 3.9). The stock YTsaurus job
environment does not offer one. The released `ghcr.io/ytsaurus/flow-python-nightly:dev-0.2.1`
does: it is the Flow server image plus Python 3.12 at `/usr/bin/python3` with
`ytsaurus-flow-companion` 0.2.1 and `ytsaurus-client` preinstalled, and it pulls anonymously.

The spec names it as the per-task `docker_image` of both vanilla tasks:

```yson
"controller" = {"count" = 1; "docker_image" = "ghcr.io/ytsaurus/flow-python-nightly:dev-0.2.1";};
"worker" = {"count" = 1; "docker_image" = "ghcr.io/ytsaurus/flow-python-nightly:dev-0.2.1"; ...};
```

The companion is then spawned by the image's own interpreter, and the only job file besides the
runner-uploaded `flow_server` is the user's code, shipped as a plain `local_files` entry:

```yson
"entrypoint" = {"executable" = "/usr/bin/python3"; "args" = ["main.py"];};
```

No image of our own is built: `main.py` needs nothing beyond the SDK, so a user image on top of the
released one would only repackage a single file. Build one (`FROM` the released image) only when
the computation needs extra Python packages. No bundle, no registry, no `docker_auth` secret either.

The runner (`run.sh`, the stock `flow-nightly` image) uploads its own `flow_server` to the jobs; it
is byte-identical to the one in `flow-python-nightly` (same release commit).

## Run

From the repo root:

```bash
python3 docker_vanilla_companion/yt_sync.py  # once: pipeline node, input_queue + consumer, output_queue
./run.sh docker_vanilla_companion
```

From a second terminal, feed the input queue and read the output:

```bash
echo '{"key": "a", "text": "hello", "count": 1, "score": 1.5, "flag": true}
{"key": "b", "text": "world", "count": 2, "score": 2.5, "flag": false}' \
    | yt insert-rows --format json "$YT_DEV_ROOT/docker_vanilla_companion/input_queue"

yt select-rows "* from [$YT_DEV_ROOT/docker_vanilla_companion/output_queue]" --format json
```

`select-rows` rather than `pull-queue`: the latter serves flushed rows only, so a row written
seconds ago can be missing from it while plainly present in the table.

When done, `./stop.sh docker_vanilla_companion` stops the pipeline and aborts the vanilla operation.

## Observed output

```
flow_server: 26.3.0-local-os~c27be0f50d0abf1d+distbuild (ghcr.io/ytsaurus/flow-nightly:dev-0.2.1)
```

Every column comes back unchanged plus `text_upper`, so the row went through the Python process
(queue system columns elided):

```json
{"key":"a","text":"hello","count":1,"score":1.5,"flag":true,"text_upper":"HELLO"}
{"key":"b","text":"world","count":2,"score":2.5,"flag":false,"text_upper":"WORLD"}
```

Nothing consumes the output queue, so it keeps everything — insert again and the same rows come back
with higher `$row_index`.

The worker's stderr carries the companion's own logs, which exist only because the SDK started
under the image's interpreter:

```
I  FlowWorker  Spawning companion process (... Executable: /usr/bin/python3, Args: [main.py], CompanionPort: 24582)
INFO:yt.yt.flow.library.python.companion.sizing:Resolved CPU quota from cgroup v1 (Quota: inf, Source: /sys/fs/cgroup/cpu)
INFO:yt.yt.flow.library.python.companion.server:gRPC server started successfully on port 24582
```

And the operation spec carries `docker_image` on both tasks:

```bash
yt get-operation <op-id> --attribute spec --format json | python3 -c '
import json, sys
spec = json.load(sys.stdin)["spec"]
print({name: task.get("docker_image") for name, task in spec["tasks"].items()})'
```
