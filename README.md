# ytsaurus-flow-demo

Re-implementations of YT Flow integration-test scenarios as standalone pipelines deployed to an
opensource YTsaurus cluster with **vanilla operations only**. Each scenario dir is self-contained:
pipeline spec, Cypress bootstrap, data preparation, deployment, and verification scripts.

## Prerequisites

- An opensource YTsaurus cluster reachable over its HTTP proxy.
- The `flow_server` binary built from the [ytsaurus](https://github.com/ytsaurus/ytsaurus) repo
  (`ya make yt/yt/flow/bin/flow_server`); scenarios with custom C++ code build their own binary the
  same way. Build it **stripped** — the runner uploads its own executable to the cluster, and an
  unstripped profile build carries gigabytes of debug info.
- **Run the scenarios on the host that built the binary.** Deployment ships that local executable to
  the cluster's vanilla jobs, so it must be a Linux build from this machine; `FLOW_BINARY` points at
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
| `YT_PROXY_INTERNAL` | HTTP proxy URL reachable from inside the cluster (k8s service address) |
| `YT_CLUSTER_NAME` | cluster name as registered in `//sys/clusters` |
| `YT_DEV_ROOT` | Cypress root for all scenarios, e.g. `//tmp/<login>/ytsaurus_dev` |
| `YT_POOL` | scheduler pool for vanilla operations |
| `YT_PROXY_RPC` | RPC proxy endpoint reachable from your host (`host:port`) |

## Deployment model

`common/deploy.py` runs the flow runner **on the dev host**: it connects over RPC, uploads the
binary, submits the pipeline spec and launches the controller+worker vanilla operation, then exits
(`YT_FLOW_WAIT=0`) instead of tailing the pipeline.

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
source env.sh              # your private env file, once per shell
cd <scenario>

python3 yt_sync.py         # Cypress objects (pip-installed yt_sync_mini)
python3 scenario.py        # write input data, deploy the pipeline, assert the expected result
```

`scenario.py` also takes a single step name (`prepare`, `deploy`, `verify`) to re-run one of them
on its own. Data is written **before** the pipeline starts: a finite source reports itself empty as
soon as it reaches the end of its input, so a pipeline deployed against an empty queue completes
before any row arrives.

Non-finite scenarios are shut down with `python3 ../common/stop.py <scenario> <operation_id>`.

## Layout

- `common/` — the deploy/stop helpers shared by all scenarios.
- `<scenario>/` — one dir per scenario.
