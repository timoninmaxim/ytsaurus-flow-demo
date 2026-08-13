# docker_vanilla_companion

A Python computation running in a job image the pipeline chooses:
`reader` (`TSwiftPassthroughOrderedSourceComputation` over `TQueueSource`) → `mapper`
(`TTransformCompanionComputation`, implemented in `main.py`) → `TSyncQueueSink`.

It shares its name with the spec-only example of the same shape that ytsaurus ships at
`yt/yt/flow/examples/python/docker_vanilla_companion`, and adds what that one cannot carry: the
Cypress bootstrap, a second route that needs no registry, and output from a real cluster.

The mapper mirrors every typed input column to the output stream (string, int64, double, boolean —
the companion wire-protocol type roundtrip) and adds `text_upper`, computed in Python.

## Why the job needs its own image

The Python companion imports `yt.wrapper` / `yt.yson` / `yt.type_info` at startup, so it needs an
interpreter with `ytsaurus-client` — and the companion SDK requires Python >= 3.9. The stock
YTsaurus job environment does not offer one, so the interpreter has to come from somewhere.

A pipeline names the image its jobs run in with the per-task `docker_image`, which this scenario
sets on both vanilla tasks:

```yson
"controller" = {"count" = 1; "port_count" = 2; "docker_image" = "docker.io/library/python:3.12-slim";};
"worker" = {"count" = 1; "port_count" = 3; "docker_image" = "docker.io/library/python:3.12-slim"; ...};
```

The image is a stock public one — `flow_server` itself needs nothing from it beyond a glibc
userland, and the job proxy is bind-mounted in by the node. To use another, change it in
`pipeline.yson.template`.

### What this scenario needs that the repo README does not list

- **`podman` or `docker`** on the dev host, to build the companion image. Both routes below start
  from it.
- **A `flow_server` new enough to pass per-task `docker_image` into the vanilla spec.** Without it
  the field never reaches the operation, the jobs run in the default environment where
  `/usr/local/bin/python3` does not exist, and the companion fails to start. It and the Dockerfile
  come from an ordinary ytsaurus checkout — `git clone https://github.com/ytsaurus/ytsaurus.git` —
  from commit `c6adf1ad176` onwards.

## Two ways to get the SDK into the job

The image supplies the interpreter either way. The SDK can come with it or alongside it, and the
scenario ships a spec for each.

### `pipeline_image.yson.template` — SDK baked into the image

The route to prefer once you have a registry your cluster can pull from. ytsaurus carries a
Dockerfile for exactly this image at
`<ytsaurus>/yt/yt/flow/tools/python_companion_package/Dockerfile`; build it from the checkout root,
so the SDK matches the `flow_server` built from those same sources:

```bash
cd "$YTSAURUS"
docker build -f yt/yt/flow/tools/python_companion_package/Dockerfile \
    -t <registry>/ytflow-python-companion:<tag> .
docker push <registry>/ytflow-python-companion:<tag>
```

The job then needs nothing but the user's own code, and the entrypoint is the image's interpreter:

```yson
"entrypoint" = {"executable" = "/usr/local/bin/python3"; "args" = ["main.py"];};
```

A computation that needs third-party packages inherits from that image (`FROM
<registry>/ytflow-python-companion:<tag>`) and adds them.

The spec assumes a **private** registry, which is what you get by default when you push to one:
`"secret_env" = ["docker_auth"]` in its vanilla block asks the runner to forward an environment
variable of that name into the operation's secure vault, which is where YT looks for registry
credentials. Export it next to the other cluster variables:

```bash
export docker_auth='{username="<user>"; password="<token>"}'
```

If your image allows anonymous pull, drop the `secret_env` line instead — with it present and the
variable unset the runner refuses to launch.

### `pipeline.yson.template` — SDK as a job file

The default here, because it needs no registry at all: the job runs a stock public image and the SDK
arrives as a job file. `build.sh` produces `companion_sdk.tgz` (~9 MB) by copying `site-packages`
straight out of the companion image, which already has everything installed — so the native wheels
(grpcio, protobuf) match the interpreter that will import them, and the script needs no ytsaurus
checkout of its own. `py_companion` — the entrypoint the worker spawns — unpacks it on first start,
puts it on `PYTHONPATH` and execs `main.py`.

You still build the companion image for this, but only locally; nothing is pushed anywhere. Its base
must match the image the spec names, since the wheels inside it are built for that interpreter —
both are `python:3.12-slim` unless you change the Dockerfile's `BASE_IMAGE`.

Note it packs `site-packages`, not a virtualenv: a venv directory is not relocatable, which is the
same reason Spark's `venv-pack` requires the interpreter to be present on every node already.

Bundling the interpreter too, which is what a pipeline must do when it cannot choose its image,
costs about ten times as much: ~116 MB against ~9 MB, uploaded on every deploy.

## Run

From the repo root:

```bash
python3 docker_vanilla_companion/yt_sync.py  # once: pipeline node, input_queue + consumer, output_queue

# SDK in the image:
FLOW_COMPANION_IMAGE=<registry>/ytflow-python-companion:<tag> ./run.sh docker_vanilla_companion image

# or SDK as a job file, from a locally built companion image:
FLOW_COMPANION_IMAGE=ytflow-python-companion:latest docker_vanilla_companion/build.sh
./run.sh docker_vanilla_companion
```

From a second terminal, feed the input queue and read the output:

```bash
echo '{"key": "a", "text": "hello", "count": 1, "score": 1.5, "flag": true}
{"key": "b", "text": "world", "count": 2, "score": 2.5, "flag": false}' \
    | yt insert-rows --format json "$YT_DEV_ROOT/docker_vanilla_companion/input_queue"

yt pull-queue "$YT_DEV_ROOT/docker_vanilla_companion/output_queue" \
    --offset 0 --partition-index 0 --format json
```

When done, `./stop.sh docker_vanilla_companion` stops the pipeline and aborts the vanilla operation.

## Observed output

```
flow_server: 26.2.0-local-os~1bdcb82f3ab63fcb
```

Every column comes back unchanged plus `text_upper`, so the row went through the Python process
(queue system columns elided):

```json
{"key":"a","text":"hello","count":1,"score":1.5,"flag":true,"text_upper":"HELLO"}
{"key":"b","text":"world","count":2,"score":2.5,"flag":false,"text_upper":"WORLD"}
```

Nothing consumes the output queue, so `--offset 0` always replays everything — insert again and the
same rows come back with higher `$row_index`.

Both specs were checked this way against the same cluster; the output is identical, which is the
point — where the SDK comes from is invisible to the pipeline.

The worker's stderr carries the companion's own logs, which exist only because the SDK started
under the image's interpreter:

```
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

```
{'controller': 'docker.io/library/python:3.12-slim', 'worker': 'docker.io/library/python:3.12-slim'}
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

- That `docker_image` combines with anything else that supplies the job root filesystem. It does
  not: a job gets its filesystem from one mechanism or the other, never both.
- That a computation with third-party dependencies works. Both specs here carry only `main.py`,
  which imports nothing beyond the SDK; inheriting from the image is documented but not exercised.
- Anything about registries other than ghcr.io, which is what both routes were checked against.
