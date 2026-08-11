# ytsaurus-flow-demo

Standalone YT Flow demo pipelines deployed to an opensource YTsaurus cluster with **vanilla
operations only**. Each scenario dir holds its pipeline spec and its Cypress bootstrap; the shared
`run.sh`/`stop.sh` in the repo root deploy and stop any of them by name; feeding the pipeline and
reading its output are plain `yt` CLI commands from the scenario README.

## Prerequisites

- An opensource YTsaurus cluster reachable from your host over **both** proxies: the HTTP proxy
  (`YT_PROXY`) for Cypress and queue work, and an RPC proxy (`YT_PROXY_RPC`) — the runner deploys
  over RPC.
- The `flow_server` binary built from the [ytsaurus](https://github.com/ytsaurus/ytsaurus) repo:
  `./ya make --build=release yt/yt/flow/bin/flow_server` from the checkout root. **Strip it**
  (`strip -o flow_server.stripped flow_server`) — the runner uploads the executable on every deploy,
  and the unstripped build is gigabytes. `run.sh` takes the path from the `FLOW_BIN` env var
  (default: `~/ytsaurus/yt/yt/flow/bin/flow_server/flow_server`).

  Scenarios are written to run on that stock binary. The single deliberate exception is
  `secret_env`, whose subject is the job process itself: it ships its own C++ and its own
  `build.sh`, which builds and strips a binary of its own (see its README). `run.sh` deploys a
  `*.stripped` binary found in the scenario dir in preference to the stock one.
- **Run the scenarios on the host that built the binary.** Deployment ships that local executable to
  the cluster's vanilla jobs, so it must be a Linux build from this machine.
- Python 3 with the `ytsaurus-client` package (`pip install ytsaurus-client`) — it provides the
  `yt` CLI the scenarios are driven with.
- The Flow Cypress-bootstrap library `ytsaurus-flow-yt-sync-mini`, installed from a checkout of the
  [ytsaurus](https://github.com/ytsaurus/ytsaurus) repo:

  ```bash
  git clone https://github.com/ytsaurus/ytsaurus.git
  pip install ./ytsaurus/yt/yt/flow/tools/yt_sync_mini
  ```

  Or install straight from GitHub without a manual checkout:

  ```bash
  pip install "ytsaurus-flow-yt-sync-mini @ git+https://github.com/ytsaurus/ytsaurus.git#subdirectory=yt/yt/flow/tools/yt_sync_mini"
  ```

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

`./run.sh <scenario>` renders that scenario's `pipeline.yson.template` (substituting `${VAR}`s from
the environment) and runs the flow runner **on the dev host**: it connects over RPC, uploads the
binary, submits the pipeline spec and launches the controller+worker vanilla operation, then
streams the controller log to the terminal. Ctrl-C only detaches — the pipeline keeps running on
the cluster until `./stop.sh <scenario>` stops it and aborts the vanilla operation (by the alias the
runner recorded in `@current_vanilla_operation` on the pipeline node).

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
./run.sh <scenario>              # deploy + stream the controller log; Ctrl-C detaches
```

Then feed the pipeline and watch its output from a second terminal with the `yt` CLI — each
scenario's README shows the exact commands. When done, `./stop.sh <scenario>` shuts the pipeline
down.

## Layout

- `run.sh`, `stop.sh` — shared by every scenario, taking the scenario name as their argument.
- `<scenario>/` — one dir per scenario: `pipeline.yson.template`, `yt_sync.py`; a scenario that
  builds a binary of its own adds `pipeline/` (C++ sources) and `build.sh`.
