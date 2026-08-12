# companion_python_docker

A Python computation running in a job image the pipeline chooses:
`reader` (`TSwiftPassthroughOrderedSourceComputation` over `TQueueSource`) → `mapper`
(`TTransformCompanionComputation`, implemented in `main.py`) → `TSyncQueueSink`.

The mapper mirrors every typed input column to the output stream (string, int64, double, boolean —
the companion wire-protocol type roundtrip) and adds `text_upper`, computed in Python.

## Why the job needs its own image

The Python companion imports `yt.wrapper` / `yt.yson` / `yt.type_info` at startup, so it needs an
interpreter with `ytsaurus-client` — and the companion SDK requires Python >= 3.9. The stock
YTsaurus job environment does not offer one, so the interpreter has to come from somewhere.

On an opensource cluster jobs run in a **CRI** job environment, where the porto `layers` mechanism
does not exist — and `layers` additionally pins the operation to porto nodes through
`scheduling_tag_filter = "porto"`, which no node here carries. The CRI counterpart is the per-task
`docker_image`, which this scenario sets on both vanilla tasks:

```yson
"controller" = {"count" = 1; "port_count" = 2; "docker_image" = "docker.io/library/python:3.12-slim";};
"worker" = {"count" = 1; "port_count" = 3; "docker_image" = "docker.io/library/python:3.12-slim"; ...};
```

The image is a stock public one — `flow_server` itself needs nothing from it beyond a glibc
userland, and the job proxy is bind-mounted in by the node. To use another, change it in
`pipeline.yson.template` and pass the same one to `build.sh` as `FLOW_DOCKER_IMAGE`.

### What this scenario needs that the repo README does not list

- **A `flow_server` that passes per-task `docker_image` into the vanilla spec.** Not in ytsaurus
  yet. Without it the field never reaches the operation, the jobs run in the default environment
  where `/usr/local/bin/python3` does not exist, and the companion fails to start.
- **The `ytsaurus-flow-companion` package sources** at
  `<ytsaurus>/yt/yt/flow/tools/python_companion_package`, which `build.sh` compiles. Also not in
  ytsaurus yet.
- **`podman` or `docker`** on the dev host, to build the SDK bundle.

## The SDK travels as a job file

The image supplies the interpreter; the SDK and its runtime dependencies ride into the job sandbox
as a job file. `build.sh` produces `companion_sdk.tgz` (~11 MB) by installing the companion package
and its dependencies **inside the job image**, so the native wheels (grpcio, protobuf) match the
interpreter that will import them. `py_companion` — the entrypoint the worker spawns — unpacks it
on first start, puts it on `PYTHONPATH` and execs `main.py`.

Bundling the interpreter as well, which is what a pipeline must do without `docker_image`, costs
about ten times as much: ~116 MB against ~11 MB, uploaded on every deploy.

Baking the SDK into the image instead would drop the tarball, `py_companion` and `build.sh`
entirely, leaving `entrypoint.executable = /usr/local/bin/python3` with `args = ["main.py"]`. That
needs a registry the cluster can pull from, which this repo does not assume.

## Run

From the repo root:

```bash
companion_python_docker/build.sh          # once: companion_sdk.tgz (YTSAURUS=<checkout>)
python3 companion_python_docker/yt_sync.py  # once: pipeline node, input_queue + consumer, output_queue

./run.sh companion_python_docker
```

From a second terminal, feed the input queue and read the output:

```bash
echo '{"key": "a", "text": "hello", "count": 1, "score": 1.5, "flag": true}
{"key": "b", "text": "world", "count": 2, "score": 2.5, "flag": false}' \
    | yt insert-rows --format json "$YT_DEV_ROOT/companion_python_docker/input_queue"

yt pull-queue "$YT_DEV_ROOT/companion_python_docker/output_queue" \
    --offset 0 --partition-index 0 --format json
```

When done, `./stop.sh companion_python_docker` stops the pipeline and aborts the vanilla operation.

## Observed output

```
flow_server: 26.2.0-local-os~5c69dd1804e43fe5
```

Every column comes back unchanged plus `text_upper`, so the row went through the Python process
(queue system columns elided):

```json
{"key":"a","text":"hello","count":1,"score":1.5,"flag":true,"text_upper":"HELLO"}
{"key":"b","text":"world","count":2,"score":2.5,"flag":false,"text_upper":"WORLD"}
```

Nothing consumes the output queue, so `--offset 0` always replays everything — insert again and the
same rows come back with higher `$row_index`.

The worker's stderr carries the companion's own logs, which exist only because the SDK started
under the image's interpreter:

```
INFO:yt.yt.flow.library.python.companion.sizing:Resolved CPU quota from cgroup v1 (Quota: inf, Source: /sys/fs/cgroup/cpu)
INFO:yt.yt.flow.library.python.companion.server:gRPC server started successfully on port 24582
```

And the operation spec carries `docker_image` on both tasks and no `scheduling_tag_filter`:

```bash
yt get-operation <op-id> --attribute spec --format json | python3 -c '
import json, sys
spec = json.load(sys.stdin)["spec"]
print({name: task.get("docker_image") for name, task in spec["tasks"].items()})
print("scheduling_tag_filter =", spec.get("scheduling_tag_filter"))'
```

```
{'controller': 'docker.io/library/python:3.12-slim', 'worker': 'docker.io/library/python:3.12-slim'}
scheduling_tag_filter = None
```

### The failure path, checked as well

Pointing the template at an image that does not exist fails both tasks in job preparation, which is
what makes the successful run evidence that the field is honored rather than quietly dropped:

```
Job preparation failed
  Failed to pull docker image
    code    1132
```

## What this does not prove

- That the SDK works when baked into the image rather than unpacked from a job file — the import
  path is the same, but nothing here exercises it.
- Anything about porto clusters. `docker_image` and `layers` are mutually exclusive — the node
  rejects layers under CRI and an image under porto — and this scenario only covers the CRI side.
- Anything about private registries. The image is pulled anonymously; a private one needs registry
  credentials in the operation's secure vault, which this scenario does not set up.
