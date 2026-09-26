# ytsaurus-flow-demo

Standalone YT Flow demo pipelines deployed to an opensource YTsaurus cluster with **vanilla
operations only**. Each scenario dir holds its pipeline spec and its Cypress bootstrap; a pipeline
is launched with one `podman run` of the released server image, and stopped with `stop.sh`; feeding
the pipeline and reading its output are plain `yt` CLI commands from the scenario README.

## Released artifacts

The scenarios run on the published YT Flow test release **0.2.1** (`flow-test/0.2.1`); nothing is
built from source. This table is the one place that names the version (a bump also changes the
`FLOW_IMAGE` default in `run.sh`).

| Artifact | Coordinate |
|----------|------------|
| Server image (runner + vanilla jobs) | `ghcr.io/ytsaurus/flow-nightly:dev-0.2.1` (`/usr/bin/flow_server`) |
| Server image + JRE 17 (Java companions) | `ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1` |
| Server image + Python SDK (Python companions) | `ghcr.io/ytsaurus/flow-python-nightly:dev-0.2.1` |
| Python packages (TestPyPI) | `ytsaurus-flow-yt-sync-mini==0.2.1.dev10`, `ytsaurus-flow-companion==0.2.1.dev10` |
| Java (Maven snapshots) | `tech.ytsaurus:flow-*:0.2.1-SNAPSHOT` from `https://central.sonatype.com/repository/maven-snapshots/` |
| Go | `go get go.ytsaurus.tech/yt/go/flow@c27be0f50d0a` (pseudo-version of the release commit) |

The images pull anonymously. The Python packages live on TestPyPI, their dependencies on PyPI:

```bash
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ \
    ytsaurus-flow-yt-sync-mini==0.2.1.dev10
```

**Not covered by the artifact run:** scenarios that need a source build — a C++ companion, a custom
C++ binary (`secret_env`), the `yql_*` scenarios — document their own build and local-binary launch
command in their README.

## Prerequisites

- An opensource YTsaurus cluster reachable from your host over **both** proxies: the HTTP proxy
  (`YT_PROXY`) for Cypress and queue work, and an RPC proxy (`YT_PROXY_RPC`) — the runner deploys
  over RPC. Its exec nodes must be able to run jobs in a docker image (`docker_image` in the
  vanilla task spec) pulled from `ghcr.io`.
- [podman](https://podman.io/) — `run.sh` runs the runner from the server image.
- Python 3 with `ytsaurus-client` (`pip install ytsaurus-client`) for the `yt` CLI the scenarios are
  driven with, and `ytsaurus-flow-yt-sync-mini` from the table above for the Cypress bootstrap.

## Configuration — no secrets in this repo

All cluster coordinates and credentials live in a private env file that git never sees — keep it at
`env.sh` in the repo root, which is gitignored. Source it in your shell before running a scenario;
the scripts read these variables from the environment and nothing else. It must export:

| Variable | Meaning |
|----------|---------|
| `YT_TOKEN` | cluster token/password |
| `YT_PROXY` | HTTP proxy URL reachable from your host — what the `yt` CLI and the Python client talk to |
| `YT_PROXY_INTERNAL` | HTTP proxy URL reachable from **inside** the cluster — set it to `YT_PROXY` unless the vanilla jobs cannot resolve the public address (then use the k8s service address). Required: `run.sh` substitutes every `${VAR}` in the spec template and fails on an unset one |
| `YT_CLUSTER_NAME` | cluster name as registered in `//sys/clusters` |
| `YT_DEV_ROOT` | Cypress root for all scenarios, e.g. `//tmp/<login>/ytsaurus_dev` |
| `YT_POOL` | scheduler pool for vanilla operations |
| `YT_PROXY_RPC` | RPC proxy endpoint reachable from your host (`host:port`) |

## Deployment model

The flow runner runs **on the dev host**: the released `flow_server`, in a podman container of the
server image on the host network, with the scenario dir mounted at its own path and the shell
environment passed through. From the scenario dir, with the spec rendered to `pipeline.yson`:

```bash
podman run --rm --network host --env-host -v "$PWD:$PWD" -w "$PWD" \
    ghcr.io/ytsaurus/flow-nightly:dev-0.2.1 /usr/bin/flow_server --config pipeline.yson
```

The runner connects over RPC, uploads its own binary, submits the pipeline spec and launches the
controller+worker vanilla operation, then streams the controller log to the terminal. The jobs run
inside the released image too: a spec sets `"docker_image" = "${FLOW_IMAGE}"` on the `controller`
and `worker` tasks (a companion scenario sets the Java or Python server image instead). Ctrl-C only
detaches — the pipeline keeps running until `./stop.sh <scenario>` stops it and aborts the vanilla
operation (by the alias the runner recorded in `@current_vanilla_operation` on the pipeline node).

`./run.sh <scenario>/pipeline[_<variant>].yson.template` is the shortcut: it renders the template
(every `${VAR}` from the environment; an unset one fails), exporting `FLOW_IMAGE` (default: the
server image above) and `SCENARIO_DIR` (the scenario dir, for files a spec ships through
`local_files` — they must live inside it, the only host path the container sees), then runs the
command above. A pipeline that lives deeper than the scenario dir is stopped by its path:
`./stop.sh <scenario>/<variant>`.

The cluster advertises only a k8s-internal RPC proxy address, which the dev host cannot resolve, so
the runner config pins the reachable one instead of relying on proxy discovery:

```yson
"clients_cache" = {
    "default_connection" = {
        "enable_proxy_discovery" = %false;
        "proxy_addresses" = ["${YT_PROXY_RPC}"];
    };
};
```

Three cluster quirks every spec template accounts for:

- `address_resolver = {enable_ipv4=%true; enable_ipv6=%true}` at the runner level — the external
  RPC endpoint is reached through NAT64, so IPv6 must stay enabled.
- `vanilla/node_config` keeps `{enable_ipv4=%true; enable_ipv6=%false}` for the in-cluster
  controller/worker jobs — k8s DNS serves A-records only, while the YT client defaults to IPv6.
- `vanilla/proxy_url_aliasing_rules = {<cluster_name> = <internal proxy URL>}` — otherwise
  `<cluster=...>` rich paths resolve through the default `*.yt.yandex.net` pattern.

## Running a scenario

```bash
source env.sh                    # your private env file, once per shell

python3 <scenario>/yt_sync.py    # once: Cypress objects (pip-installed yt_sync_mini)
./run.sh <scenario>/pipeline.yson.template   # deploy + stream the controller log; Ctrl-C detaches
```

Then feed the pipeline and watch its output from a second terminal with the `yt` CLI — each
scenario's README shows the exact commands. When done, `./stop.sh <scenario>` shuts the pipeline
down.

## Layout

- `run.sh` (takes the spec template), `stop.sh` (takes the scenario name) — shared by every scenario.
- `<scenario>/` — one dir per scenario: `pipeline.yson.template`, `yt_sync.py`; a scenario that
  builds a binary of its own from source adds `pipeline/` (C++ sources) and `build.sh`. Files a
  spec ships to the jobs (`local_files`) must live inside the scenario dir, the only host path the
  runner container sees.
