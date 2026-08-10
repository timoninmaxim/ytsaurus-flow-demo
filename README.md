# ytsaurus-flow-demo

Re-implementations of YT Flow integration-test scenarios as standalone pipelines deployed to an
opensource YTsaurus cluster with **vanilla operations only**. Each scenario dir is self-contained:
its pipeline spec, its Cypress bootstrap, and one `run_<scenario>.py` that deploys the pipeline,
feeds it, and tails its output.

## Prerequisites

- An opensource YTsaurus cluster reachable from your host over **both** proxies: the HTTP proxy for
  Cypress and queue work, and an RPC proxy — the runner deploys over RPC (`YT_PROXY_RPC`).
- The `flow_server` binary built from the [ytsaurus](https://github.com/ytsaurus/ytsaurus) repo:
  `./ya make --build=release yt/yt/flow/bin/flow_server` from the checkout root; scenarios with
  custom C++ code build their own binary the same way. Keep it free of debug info — a debug or
  profile build is gigabytes, and the runner uploads the executable on every deploy.
- **Run the scenarios on the host that built the binary.** Deployment ships that local executable to
  the cluster's vanilla jobs, so it must be a Linux build from this machine; `--flow-bin` points at
  it (default: `~/ytsaurus/yt/yt/flow/bin/flow_server/flow_server`).
- Python 3 with the `ytsaurus-client` package (`pip install ytsaurus-client`) — the scripts drive
  the cluster through its client, and it also provides the `yt` CLI for poking around by hand.
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
| `YT_PROXY_INTERNAL` | *optional* — HTTP proxy URL reachable from **inside** the cluster; defaults to `YT_PROXY`, and only a cluster whose public address the vanilla jobs cannot resolve needs its own value (k8s service address) |
| `YT_CLUSTER_NAME` | cluster name as registered in `//sys/clusters` |
| `YT_DEV_ROOT` | Cypress root for all scenarios, e.g. `//tmp/<login>/ytsaurus_dev` |
| `YT_POOL` | scheduler pool for vanilla operations |
| `YT_PROXY_RPC` | RPC proxy endpoint reachable from your host (`host:port`) |

## Deployment model

A scenario's `deploy` step runs the flow runner **on the dev host**: it connects over RPC, uploads
the binary, submits the pipeline spec and launches the controller+worker vanilla operation, then
exits (`YT_FLOW_WAIT=0`) instead of tailing the pipeline. The runner config is the scenario's
`pipeline.yson.template` with its `${VAR}` placeholders filled in from the environment.

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

`cluster_url` is the proxy the *in-cluster* components use: with discovery disabled the runner never
dials it, but it is what ends up in Cypress (the vanilla operation manifest). On this cluster the
public address is a NAT64-only name the vanilla jobs cannot resolve, which is why
`YT_PROXY_INTERNAL` exists at all; anywhere else it stays unset and `YT_PROXY` serves both roles.

## Running a scenario

```bash
source env.sh              # your private env file, once per shell
cd <scenario>

python3 yt_sync.py         # Cypress objects (pip-installed yt_sync_mini)
python3 run_<scenario>.py  # deploy → write input data → tail the output
```

`run_<scenario>.py` also takes a single step name to run one step on its own — `deploy`, `prepare`,
`tail`, or `stop`. Shutting the pipeline down is always explicit: `stop` is never part of a plain
run. It aborts the vanilla operation by the alias the runner recorded on the pipeline node
(`@current_vanilla_operation`), so there is no operation id to pass around.

## Layout

- `<scenario>/` — one dir per scenario: `pipeline.yson.template`, `yt_sync.py`,
  `run_<scenario>.py`.
