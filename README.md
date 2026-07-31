# ytsaurus-flow-demo

Re-implementations of YT Flow integration-test scenarios as standalone pipelines deployed to an
opensource YTsaurus cluster with **vanilla operations only**. Each scenario dir is self-contained:
pipeline spec, Cypress bootstrap, data preparation, deployment, and verification scripts.

See `PLAN.md` for the full scenario list and status.

## Prerequisites

- An opensource YTsaurus cluster reachable over its HTTP proxy.
- The `flow_server` binary built from the [ytsaurus](https://github.com/ytsaurus/ytsaurus) repo
  (`ya make --build=profile yt/yt/flow/bin/flow_server`); scenarios with custom C++ code build their
  own binary the same way.
- Python 3 with the `ytsaurus-client` package (`pip install ytsaurus-client`).
- The Flow Cypress-bootstrap library `ytsaurus-flow-yt-sync-mini`, installed from a checkout of the
  [ytsaurus](https://github.com/ytsaurus/ytsaurus) repo. It is a single wheel that ships both
  `yt_sync_mini` and its `pipeline_tables` dependency under their real import path
  (`yt.yt.flow.library.python.{yt_sync_mini,pipeline_tables}` — the same names the in-repo build
  uses, layered onto the `yt` package from `ytsaurus-client` via PEP 420 namespaces). Its `setup.py`
  lives beside the `yt_sync_mini` sources under `yt/yt/flow/tools/yt_sync_mini`:

  ```bash
  # from a ytsaurus checkout:
  git clone https://github.com/ytsaurus/ytsaurus.git
  pip install ./ytsaurus/yt/yt/flow/tools/yt_sync_mini
  ```

  Or install straight from GitHub without a manual checkout:

  ```bash
  pip install "ytsaurus-flow-yt-sync-mini @ git+https://github.com/ytsaurus/ytsaurus.git#subdirectory=yt/yt/flow/tools/yt_sync_mini"
  ```
- `curl`.

## Configuration — no secrets in this repo

All cluster coordinates and credentials live in a private env file **outside** the repo. Point
`YT_FLOW_DEMO_ENV` at it; it must export:

| Variable | Meaning |
|----------|---------|
| `YT_TOKEN` | cluster token/password |
| `YT_PROXY_EXTERNAL` | HTTP proxy URL reachable from your host |
| `YT_PROXY_INTERNAL` | HTTP proxy URL reachable from inside the cluster (k8s service address) |
| `YT_CLUSTER_NAME` | cluster name as registered in `//sys/clusters` |
| `YT_DEV_ROOT` | Cypress root for all scenarios, e.g. `//tmp/<login>/ytsaurus_dev` |
| `YT_POOL` | scheduler pool for vanilla operations |

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

The demo cluster runs a real queue agent (`//sys/@cluster_connection/queue_agent` points at the
`qa-*` instances and `//sys/queue_agents/consumer_registrations` is provisioned by the queue-agent
state migration), so `register_queue_consumer` works through the normal path — no manual Cypress
fixup is required.

## Running a scenario

```bash
export YT_FLOW_DEMO_ENV=~/path/to/your/env.sh
cd <scenario>
./bootstrap.sh          # create Cypress objects (pip-installed yt_sync_mini)
./prepare_data.sh       # write input data (if the scenario needs any)
../common/deploy.sh <scenario>
./verify.sh             # assert the original test's expected result
../common/stop.sh <scenario> <operation_id>   # for non-finite pipelines
```

## Layout

- `common/` — env loading, curl API helpers, deploy/stop scripts shared by all scenarios.
- `<scenario>/` — one dir per scenario: `README.md`, `pipeline.yson.template`, `yt_sync.py`,
  `bootstrap.sh`, `prepare_data.sh`, `verify.sh`, plus C++ pipeline code where the stock
  `flow_server` binary is not enough.
