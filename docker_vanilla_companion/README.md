# docker_vanilla_companion

A Python computation running in a job image the pipeline chooses:
`reader` (`TSwiftPassthroughOrderedSourceComputation` over `TQueueSource`) → `mapper`
(`TTransformCompanionComputation`, implemented in `main.py`) → `TSyncQueueSink`.

It shares its name with the spec-only example of the same shape that ytsaurus ships at
`yt/yt/flow/examples/python/docker_vanilla_companion`, and adds what that one cannot carry: the
Cypress bootstrap and output from a real cluster.

The mapper mirrors every typed input column to the output stream (string, int64, double, boolean —
the companion wire-protocol type roundtrip) and adds `text_upper`, computed in Python.

## Why the job needs its own image

The Python companion imports `yt.wrapper` / `yt.yson` / `yt.type_info` at startup, so it needs an
interpreter with `ytsaurus-client` — and the companion SDK requires Python >= 3.9. The stock
YTsaurus job environment does not offer one, so the interpreter has to come from somewhere.

A pipeline names the image its jobs run in with the per-task `docker_image`, which this scenario
sets on both vanilla tasks:

```yson
"controller" = {"count" = 1; "port_count" = 2; "docker_image" = "${FLOW_COMPANION_IMAGE}";};
"worker" = {"count" = 1; "port_count" = 3; "docker_image" = "${FLOW_COMPANION_IMAGE}"; ...};
```

The companion is then spawned by the image's own interpreter, and the only job file left is the
user's code:

```yson
"entrypoint" = {"executable" = "/usr/local/bin/python3"; "args" = ["main.py"];};
```

### What this scenario needs that the repo README does not list

- **`podman` or `docker`** on the dev host, and a registry your cluster can pull from.
- **A `flow_server` new enough to pass per-task `docker_image` into the vanilla spec.** Without it
  the field never reaches the operation, the jobs run in the default environment where
  `/usr/local/bin/python3` does not exist, and the companion fails to start. It and the Dockerfile
  below come from an ordinary ytsaurus checkout —
  `git clone https://github.com/ytsaurus/ytsaurus.git` — from commit `c6adf1ad176` onwards.

## Build the image

ytsaurus carries a Dockerfile for exactly this image — an interpreter with the companion SDK and
`ytsaurus-client` installed. Build it from the checkout root, so the SDK is compiled from the same
sources as the `flow_server` you deploy:

```bash
cd "$YTSAURUS"
docker build -f yt/yt/flow/tools/python_companion_package/Dockerfile \
    -t <registry>/ytflow-python-companion:<tag> .
docker push <registry>/ytflow-python-companion:<tag>
```

This example needs nothing beyond the SDK, so it uses that image as it is and ships `main.py` as a
job file. A computation that imports third-party packages inherits from it —
`FROM <registry>/ytflow-python-companion:<tag>` — adds what it needs, and drops the `local_files`
entry.

The spec assumes a **private** registry, which is what you get by default when you push to one:
`"secret_env" = ["docker_auth"]` in its vanilla block asks the runner to forward an environment
variable of that name into the operation's secure vault, which is where YT looks for registry
credentials. Export it next to the other cluster variables:

```bash
export docker_auth='{username="<user>"; password="<token>"}'
```

If your image allows anonymous pull, drop the `secret_env` line instead — with it present and the
variable unset the runner refuses to launch.

## Run

From the repo root:

```bash
python3 docker_vanilla_companion/yt_sync.py  # once: pipeline node, input_queue + consumer, output_queue

FLOW_COMPANION_IMAGE=<registry>/ytflow-python-companion:<tag> ./run.sh docker_vanilla_companion
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
flow_server: 26.2.0-local-os~1bdcb82f3ab63fcb
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

### The failure path, checked as well

Pointing the spec at an image that does not exist fails both tasks in job preparation, which is what
makes the successful run evidence that the field is honored rather than quietly dropped:

```
Job preparation failed
  Failed to pull docker image
    code    1132
```

## What this does not prove

- That `docker_image` combines with anything else that supplies the job root filesystem. It does
  not: a job gets its filesystem from one mechanism or the other, never both.
- That a computation with third-party dependencies works. `main.py` imports nothing beyond the SDK;
  inheriting from the image is documented but not exercised.
- Anything about registries other than ghcr.io, which is what this was checked against.
