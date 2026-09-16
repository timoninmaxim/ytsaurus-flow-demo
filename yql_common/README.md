# YQL over Flow — shared harness

The `yql_*` scenarios run **YQL streaming queries** on the demo cluster: YQL
text goes in, a Flow pipeline comes out. The YQL ytflow provider (released
into opensource in September 2026) compiles `pragma Engine = "ytflow"` queries
into a Flow pipeline spec, bootstraps every Cypress object the pipeline needs
(no `yt_sync_mini` here), uploads the `ytflow_worker` binary, and launches the
controller and workers as one vanilla operation — the same deployment model as
every other scenario in this repo.

In the product this provider lives inside the **YQL agent** (Query Tracker):
you `yt start-query yql '...'` and the cluster does the rest. Every released
Query Tracker image predates the feature, so these scenarios drive the same
gateway from the dev host through **`ytflowrun`** — the ytflow-flavoured YQL
CLI, released alongside the provider.

## Binaries

Both are built from the [ytsaurus](https://github.com/ytsaurus/ytsaurus) repo:

```bash
./ya make --build=release -DUSE_ICONV=static -DUSE_IDN=static \
    yt/yql/tools/ytflowrun yt/yql/tools/ytflow_worker
strip -o yt/yql/tools/ytflow_worker/ytflow_worker.stripped yt/yql/tools/ytflow_worker/ytflow_worker
```

`run.sh` finds them at `~/ytsaurus/yt/yql/tools/ytflowrun/ytflowrun` and
`~/ytsaurus/yt/yql/tools/ytflow_worker/ytflow_worker.stripped`, overridable
with `YTFLOWRUN_BIN` / `YTFLOW_WORKER_BIN`. Strip the worker — it is uploaded to
the cluster on every run. The static `iconv`/`idn` flags matter: the worker
travels into the vanilla jobs as a single file, and a stock release build links
those two libraries dynamically.

Host-side RPC clients still cannot reach a cluster whose RPC proxies advertise
in-cluster addresses, and the flow node config shipped into the jobs cannot be
overridden, so the build needs two small patches (`YT_RPC_PROXY_ADDRESSES` in
the RPC-proxy connection, and the node-config env patches). See the gap list
below.

## How a scenario runs

```bash
source env.sh
./yql_common/run.sh yql_select
./yql_common/stop.sh yql_select
```

1. `gateways.conf` is rendered from `yql_common/gateways.conf.template`: the
   `Yt` section points the compile-time source/sink resolution at the external
   HTTP proxy; the `Ytflow` section carries the worker binary path, the
   in-cluster proxy URL (what the vanilla jobs dial), and the default settings
   (finite streams, vanilla mode, logs to YT).
2. The scenario's `query.yql` is rendered with the scenario's Cypress root
   `$YT_DEV_ROOT/<scenario>` substituted into the pragmas and table paths.
3. `setup.sh` creates the input/output queues (ordered dynamic tables with the
   `$timestamp`/`$cumulative_data_weight` system columns) and writes the input
   rows.
4. `ytflowrun` executes the query: the gateway prepares the pipeline, starts the
   vanilla operation, and returns.
5. `run.sh` polls `yt flow get-pipeline-state` until `completed` — with finite
   streams the pipeline drains the inputs and completes on its own — then
   `verify.sh` compares the output queue against `expected.json`.

## Scenario layout

```
yql_<name>/
  query.yql.template   # the query, pragmas included; $VARS rendered by run.sh
  setup.sh             # input/output queues + input rows
  verify.sh            # output assertions
  expected.json        # expected output rows, order-insensitive
  README.md            # what the query demonstrates
```

Each scenario uses its own controller/worker port quadruple (27100 + 10·N) so
leftovers of one scenario cannot collide with the next on the exec nodes.

## Known limitations

- Reading a **static table** as a streaming source is not supported by the
  provider (the upstream test suite marks it expected-to-fail); Flow's own
  `static_table` connector is not reachable from YQL yet.
- Writing into a **sorted dynamic table** with `insert` semantics is likewise
  marked expected-to-fail upstream; sorted-table outputs work through the
  dedicated write modes only.
