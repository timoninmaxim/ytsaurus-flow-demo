# ytsaurus-flow-demo

Re-implementations of YT Flow integration-test scenarios as standalone pipelines deployed to an
opensource YTsaurus cluster with **vanilla operations only**. Each scenario dir is self-contained:
pipeline spec, Cypress bootstrap, data preparation, deployment, and verification scripts.

## Prerequisites

- An opensource YTsaurus cluster reachable over its HTTP proxy.
- The `flow_server` binary built from the [ytsaurus](https://github.com/ytsaurus/ytsaurus) repo
  (`ya make --build=profile yt/yt/flow/bin/flow_server`); scenarios with custom C++ code build their
  own binary the same way.
- Python 3 with the `ytsaurus-client` package (`pip install ytsaurus-client`).
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
- `curl`.

## Configuration — no secrets in this repo

All cluster coordinates and credentials live in a private env file that git never sees: either
`env.sh` at the repo root (gitignored, the default) or any path pointed to by `YT_FLOW_DEMO_ENV`.
It must export:

| Variable | Meaning |
|----------|---------|
| `YT_TOKEN` | cluster token/password |
| `YT_PROXY_EXTERNAL` | HTTP proxy URL reachable from your host |
| `YT_PROXY_INTERNAL` | HTTP proxy URL reachable from inside the cluster (k8s service address) |
| `YT_CLUSTER_NAME` | cluster name as registered in `//sys/clusters` |
| `YT_DEV_ROOT` | Cypress root for all scenarios, e.g. `//tmp/<login>/ytsaurus_dev` |
| `YT_POOL` | scheduler pool for vanilla operations |
| `YT_PROXY_RPC` | RPC proxy endpoint reachable from your host (`host:port`) |

## Deployment model

`common/deploy.sh` runs the flow runner **on the dev host**: it connects over RPC, uploads the
(stripped) binary, submits the pipeline spec and launches the controller+worker vanilla operation,
then exits (`YT_FLOW_WAIT=0`) instead of tailing the pipeline.

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

`cluster_url` stays the *internal* HTTP proxy: with discovery disabled it is never dialled, and
everything the runner records in Cypress (the vanilla operation manifest) then stays valid for the
in-cluster components that read it.

## Running a scenario

```bash
export YT_FLOW_DEMO_ENV=~/path/to/your/env.sh
cd <scenario>
source ../common/env.sh   # once: exports cluster vars + ytcurl/ytget/... helpers

python3 yt_sync.py        # Cypress objects (pip-installed yt_sync_mini)
./prepare_data.sh         # write input data (if the scenario needs any)
../common/deploy.sh <scenario>
./verify.sh               # assert the original test's expected result
../common/stop.sh <scenario> <operation_id>   # for non-finite pipelines
```

## Layout

- `common/` — env loading, curl API helpers, deploy/stop scripts shared by all scenarios.
- `<scenario>/` — one dir per scenario.
