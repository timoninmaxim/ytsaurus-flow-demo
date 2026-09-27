# ytsaurus-flow-demo

Standalone YT Flow demo pipelines deployed to an opensource YTsaurus cluster with **vanilla
operations only**.

## Released artifacts

The scenarios run on the published YT Flow test release **0.2.1** (`flow-test/0.2.1`); nothing is
built from source. This table is the one place that lists them; the commands and specs name the
images literally, so a version bump is a search-and-replace of `dev-0.2.1`.

| Artifact | Coordinate |
|----------|------------|
| Server image (runner + vanilla jobs) | `ghcr.io/ytsaurus/flow-nightly:dev-0.2.1` (entrypoint `/usr/bin/flow_server`) |
| Server image + JRE 17 (Java companions) | `ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1` (entrypoint `java`) |
| Server image + Python SDK (Python companions) | `ghcr.io/ytsaurus/flow-python-nightly:dev-0.2.1` (entrypoint `python3`) |
| Python packages (TestPyPI) | `ytsaurus-flow-yt-sync-mini==0.2.1.dev11`, `ytsaurus-flow-companion==0.2.1.dev11` |
| Java (Maven snapshots) | `tech.ytsaurus:flow-*:0.2.1-SNAPSHOT` from `https://central.sonatype.com/repository/maven-snapshots/` |
| Go | `go get go.ytsaurus.tech/yt/go/flow@6f22b54593c1` (pseudo-version of the release commit) |

The images pull anonymously. Each starts in `/app/pipeline`, where a scenario dir is mounted, and
names its `flow_server` in `YT_FLOW_BIN` for the SDK launchers. The Python packages live on
TestPyPI, their dependencies on PyPI:

```bash
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ \
    ytsaurus-flow-yt-sync-mini==0.2.1.dev11
```

**Not covered by the artifact run:** scenarios that need a source build — a C++ companion, a custom
C++ binary (`secret_env`), the `yql_*` scenarios — document their own build and local-binary launch
command in their README.

## Prerequisites

- An opensource YTsaurus cluster reachable from your host over **both** proxies: the HTTP proxy
  (`YT_PROXY`) for Cypress and queue work, and an RPC proxy (`YT_PROXY_RPC`) — the runner deploys
  over RPC. Its exec nodes must be able to run jobs in a docker image (`docker_image` in the
  vanilla task spec) pulled from `ghcr.io`.
- [podman](https://podman.io/) — the launch runs in a released image.
- Python 3 with, in one environment: `ytsaurus-client` (`pip install ytsaurus-client`) for the `yt`
  CLI the scenarios are driven with, `ytsaurus-flow-yt-sync-mini` from the table above for the
  Cypress bootstrap, and [jinjanator](https://pypi.org/project/jinjanator/) (`pip install jinjanator`)
  for `jinjanate`, which renders the spec templates.

## Configuration — no secrets in this repo

All cluster coordinates and credentials live in a private env file that git never sees — keep it at
`env.sh` in the repo root, which is gitignored. Source it in your shell before running a scenario;
the scripts read these variables from the environment and nothing else. It must export:

| Variable | Meaning |
|----------|---------|
| `YT_TOKEN` | cluster token/password |
| `YT_PROXY` | HTTP proxy URL reachable from your host — what the `yt` CLI and the Python client talk to |
| `YT_PROXY_INTERNAL` | HTTP proxy URL reachable from **inside** the cluster — set it to `YT_PROXY` unless the vanilla jobs cannot resolve the public address (then use the k8s service address). Required: rendering substitutes every `{{ VAR }}` in the spec template and fails on an unset one |
| `YT_CLUSTER_NAME` | cluster name as registered in `//sys/clusters` |
| `YT_DEV_ROOT` | Cypress root for all scenarios, e.g. `//tmp/<login>/ytsaurus_dev` |
| `YT_POOL` | scheduler pool for vanilla operations |
| `YT_PROXY_RPC` | RPC proxy endpoint reachable from your host (`host:port`) |

## Deployment model

The flow runner runs **on the dev host**, in a podman container of a released image, with the
scenario dir mounted at `/app/pipeline` and only `YT_TOKEN` passed in from the environment.
It connects over RPC, uploads the released `flow_server`, submits the pipeline spec and launches
the controller+worker vanilla operation, then streams the controller log to the terminal. The jobs
run in a released image too: a spec sets `docker_image` on the `controller` and `worker` tasks.
Ctrl-C only detaches — the pipeline keeps running until `./stop.sh <scenario>` stops it and aborts
the vanilla operation (by the alias the runner recorded in `@current_vanilla_operation` on the
pipeline node).

The cluster advertises only a k8s-internal RPC proxy address, which the dev host cannot resolve, so
the runner config pins the reachable one instead of relying on proxy discovery:

```yson
"clients_cache" = {
    "default_connection" = {
        "enable_proxy_discovery" = %false;
        "proxy_addresses" = ["{{ YT_PROXY_RPC }}"];
    };
};
```

Three cluster quirks every spec template (`pipeline.yson.j2`) accounts for:

- `address_resolver = {enable_ipv4=%true; enable_ipv6=%true}` at the runner level — the external
  RPC endpoint is reached through NAT64, so IPv6 must stay enabled.
- `vanilla/node_config` keeps `{enable_ipv4=%true; enable_ipv6=%false}` for the in-cluster
  controller/worker jobs — k8s DNS serves A-records only, while the YT client defaults to IPv6.
- `vanilla/proxy_url_aliasing_rules = {<cluster_name> = <internal proxy URL>}` — otherwise
  `<cluster=...>` rich paths resolve through the default `*.yt.yandex.net` pattern.

### SDK pipelines

A pipeline built from stock classes is launched with the released `flow_server` itself. A Java,
Python or Go pipeline launches through its SDK launcher instead — never bare `flow_server`. Every
SDK has the same command line, `<pipeline program> --config pipeline.yson`: the launcher enriches
the spec (ships the pipeline program as the companion, fills stream schemas and companion resources)
and hands the launch to the image's `flow_server` (`YT_FLOW_BIN`). It runs in the matching released
image, whose entrypoint is the program's interpreter:

| SDK | `podman run` arguments after the image | Image |
|-----|-----------------------------------|-------|
| Python | `main.py --config pipeline.yson` — the script ends in `app.run()` | `ghcr.io/ytsaurus/flow-python-nightly:dev-0.2.1` |
| Java | `-cp 'lib/*' <MainClass> --config pipeline.yson` — `main` calls `FlowApplication.run(args, context)` | `ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1` |
| Go | `--entrypoint ./pipeline` before the image, `--config pipeline.yson` after it — a static binary whose `main` calls `runner.Launch` | `ghcr.io/ytsaurus/flow-nightly:dev-0.2.1` |

Anything the program or the spec ships (`local_files`, jars) must live inside the scenario dir,
the only host path the container sees; refer to it by a path relative to the scenario dir. The
runner reads only `YT_TOKEN` from the environment: a scenario that forwards more variables to its
jobs lists each with `-e NAME`.

## Layout

- `stop.sh` — shared by every scenario: `./stop.sh <scenario>` from the repo root; a pipeline that
  lives deeper than the scenario dir is stopped by its path, `./stop.sh <scenario>/<variant>`.
- `<scenario>/` — one dir per scenario: `README.md` with the full instruction (bootstrap, render the
  spec, launch, feed and read, stop), `pipeline.yson.j2` (Jinja spec template), `yt_sync.py`; a
  scenario that builds a binary of its own from source adds `pipeline/` (C++ sources) and
  `build.sh`. Files a spec ships to the jobs (`local_files`) must live inside the scenario dir, the
  only host path the runner container sees.
