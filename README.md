# ytsaurus-flow-demo

Re-implementations of YT Flow integration-test scenarios as standalone pipelines deployed to an
opensource YTsaurus cluster with **vanilla operations only**. Each scenario dir is self-contained:
its pipeline spec, its Cypress bootstrap, and one `scenario.py` that deploys, feeds, checks and
shuts the pipeline down.

## Prerequisites

- An opensource YTsaurus cluster reachable over its HTTP proxy.
- The `flow_server` binary built from the [ytsaurus](https://github.com/ytsaurus/ytsaurus) repo
  (`ya make yt/yt/flow/bin/flow_server`); scenarios with custom C++ code build their own binary the
  same way. Build it **stripped** — the runner uploads its own executable to the cluster, and an
  unstripped profile build carries gigabytes of debug info.
- **Run the scenarios on the host that built the binary.** Deployment ships that local executable to
  the cluster's vanilla jobs, so it must be a Linux build from this machine; `scenario.py --flow-bin`
  points at it (default: `~/ytsaurus/yt/yt/flow/bin/flow_server/flow_server`).
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
python3 scenario.py        # deploy → write input data → assert the expected result → stop
```

`scenario.py` also takes a single step name (`deploy`, `prepare`, `verify`, `stop`) to run one of
them on its own — handy for re-checking or shutting down a pipeline that is already deployed. The
`stop` step aborts the vanilla operation by the alias the runner recorded on the pipeline node
(`@current_vanilla_operation`), so there is no operation id to pass around.

## Layout

- `<scenario>/` — one dir per scenario: `pipeline.yson.template`, `yt_sync.py`, `scenario.py`.
