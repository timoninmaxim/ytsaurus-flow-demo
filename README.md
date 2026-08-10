# ytsaurus-flow-demo

Re-implementations of YT Flow integration-test scenarios as standalone pipelines deployed to an
opensource YTsaurus cluster with **vanilla operations only**. Each scenario dir is self-contained:
its pipeline spec, its Cypress bootstrap, and a `run.sh`/`stop.sh` pair; feeding the pipeline and
reading its output are plain `yt` CLI commands from the scenario README.

## Prerequisites

- An opensource YTsaurus cluster reachable from your host over **both** proxies: the HTTP proxy for
  Cypress and queue work, and an RPC proxy — the runner deploys over RPC (`YT_PROXY_RPC`).
- The `flow_server` binary built from the [ytsaurus](https://github.com/ytsaurus/ytsaurus) repo:
  `./ya make --build=release yt/yt/flow/bin/flow_server` from the checkout root; scenarios with
  custom C++ code build their own binary the same way. **Strip it** (`strip -o flow_server.stripped
  flow_server`) — the runner uploads the executable on every deploy, and the unstripped build is
  gigabytes. `run.sh` takes the path from the `FLOW_BIN` env var
  (default: `~/ytsaurus/yt/yt/flow/bin/flow_server/flow_server`).
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
| `YT_PROXY` | HTTP proxy URL reachable from your host — what the `yt` CLI and the Python client talk to. Use the **https** URL if the balancer redirects http to https: clients following the redirect resend writes without the body, so every write silently writes nothing |
| `YT_PROXY_INTERNAL` | *optional* — HTTP proxy URL reachable from **inside** the cluster; defaults to `YT_PROXY`, and only a cluster whose public address the vanilla jobs cannot resolve needs its own value (k8s service address) |
| `YT_CLUSTER_NAME` | cluster name as registered in `//sys/clusters` |
| `YT_DEV_ROOT` | Cypress root for all scenarios, e.g. `//tmp/<login>/ytsaurus_dev` |
| `YT_POOL` | scheduler pool for vanilla operations |
| `YT_PROXY_RPC` | RPC proxy endpoint reachable from your host (`host:port`) |

## Deployment model

A scenario's `run.sh` renders `pipeline.yson.template` (substituting `${VAR}`s from the
environment) and runs the flow runner **on the dev host**: it connects over RPC, uploads the
binary, submits the pipeline spec and launches the controller+worker vanilla operation, then
streams the controller log to the terminal. Ctrl-C only detaches — the pipeline keeps running on
the cluster until `stop.sh` stops it and aborts the vanilla operation (by the alias the runner
recorded in `@current_vanilla_operation` on the pipeline node).

The controller must run **in-cluster**: every Flow client command (the runner's spec push,
`yt flow get-pipeline-state`, …) is relayed by the cluster's proxies to the pipeline controller,
so the cluster must be able to dial the controller's advertised address. A NAT'd dev host has no
such reverse route — running the controller locally (`YT_FLOW_MODE=controller+worker`) only works
when the cluster can connect back to the host.

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
source env.sh              # your private env file, once per shell
cd <scenario>

python3 yt_sync.py         # once: Cypress objects (pip-installed yt_sync_mini)
./run.sh                   # deploy + stream the controller log; Ctrl-C detaches
```

Then feed the pipeline and watch its output from a second terminal with the `yt` CLI — each
scenario's README shows the exact commands. When done, `./stop.sh` shuts the pipeline down.

## Layout

- `<scenario>/` — one dir per scenario: `pipeline.yson.template`, `yt_sync.py`, `run.sh`,
  `stop.sh`.
