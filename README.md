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

One-time cluster fixup (already applied to the demo cluster): the cluster runs no queue agents,
but consumer-registration checks are enforced; `//sys/queue_agents/consumer_registrations` was
created and mounted by hand with the queue-agent state v7+ schema (five ascending key columns
`queue_cluster, queue_path, consumer_cluster, consumer_path, consumer_name`, value columns
`vital` boolean and `partitions` any).

## Running a scenario

```bash
export YT_FLOW_DEMO_ENV=~/path/to/your/env.sh
cd <scenario>
./bootstrap.sh          # create Cypress objects (yt_sync_mini, vendored in lib/)
./prepare_data.sh       # write input data (if the scenario needs any)
../common/deploy.sh <scenario>
./verify.sh             # assert the original test's expected result
../common/stop.sh <scenario> <operation_id>   # for non-finite pipelines
```

## Layout

- `common/` — env loading, curl API helpers, deploy/stop scripts shared by all scenarios.
- `lib/` — vendored `yt_sync_mini` + `pipeline_tables` from the ytsaurus repo (Cypress bootstrap
  without internal tooling).
- `<scenario>/` — one dir per scenario: `README.md`, `pipeline.yson.template`, `yt_sync.py`,
  `bootstrap.sh`, `prepare_data.sh`, `verify.sh`, plus C++ pipeline code where the stock
  `flow_server` binary is not enough.
