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
| `YT_PROXY_RPC` | *(optional)* external RPC proxy endpoint (`host:port`), once the cluster exposes one |

## Deployment model

The cluster's RPC proxies advertise k8s-internal addresses, so a runner on the dev host cannot
connect natively. `common/deploy.sh` therefore uploads the (stripped) binary and the rendered
runner config over the HTTP API and starts a 1-job **bootstrap vanilla operation** that executes
the runner inside the cluster with `YT_FLOW_WAIT=0`: the runner launches the real
controller+worker vanilla operation, submits the pipeline spec, and exits.

Two cluster quirks every spec template accounts for:

- `address_resolver = {enable_ipv4=%true; enable_ipv6=%false}` — k8s DNS serves A-records only,
  while the YT client defaults to IPv6-only resolution.
- `vanilla/proxy_url_aliasing_rules = {<cluster_name> = <internal proxy URL>}` — otherwise
  `<cluster=...>` rich paths resolve through the default `*.yt.yandex.net` pattern.

### RPC proxy access (planned)

An external RPC endpoint (`$YT_PROXY_RPC`) will replace the bootstrap-operation workaround: the
runner will run on the dev host and talk RPC directly, and data-prep/verify scripts will move to
the `ytsaurus-client` RPC backend (`pip install ytsaurus-rpc-driver`). As of 2026-08-02 the
endpoint is not usable — see PLAN.md ("Planned switch to the external RPC proxy") for the two
blockers: the balancer routes it as L7 HTTP (YT's binary bus protocol needs an L4 TCP passthrough),
and `discover_proxies` still advertises only k8s-internal addresses while the flow runner cannot
take static `proxy_addresses`.

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
