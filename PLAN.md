# Plan: re-implement integration-test scenarios on the opensource YTsaurus cluster

Goal: for each feasible scenario from `yt/yt/flow/tests/`, build a standalone, vanilla-deployed
pipeline against the opensource demo cluster (coordinates come from the private env file, see README)
and verify that the pipeline reaches `Working` and produces the result the original test asserts.

## Common infrastructure (shared by all scenarios)

- **Cluster access.** The runner runs on the dev host and talks RPC directly (spec submission,
  binary upload, vanilla launch) — no bootstrap operation. Two things make that work:
  1. The RPC proxy port must be exposed through a raw TCP (L4) passthrough on an address the dev
     host can reach — YT RPC is a binary bus protocol, so an HTTP/L7 route cannot carry it. Put
     that endpoint in `$YT_PROXY_RPC`. If the host has no external IPv4 route, dial the address
     through NAT64 and leave IPv6 enabled in the runner's `address_resolver`.
  2. `discover_proxies` may advertise only cluster-internal addresses, so the runner config pins
     the reachable one instead: `clients_cache = {default_connection = {enable_proxy_discovery =
     %false; proxy_addresses = [...]}}`. Honouring `clients_cache` in the vanilla launcher needed
     a small upstream change to the flow runner; the rest of the runner already routed its clients
     through the root clients cache.
- **DNS.** The demo k8s DNS serves IPv4 only, so `vanilla/node_config` must set
  `address_resolver = {enable_ipv4 = %true; enable_ipv6 = %false}` for controller/worker jobs. At
  the runner level IPv6 must stay **enabled** instead — the RPC endpoint is reached over NAT64.
- **Bootstrap of Cypress objects** — `yt_sync_mini` (`yt/yt/flow/library/python/yt_sync_mini`) only;
  no internal yt_sync. It is `pip install`-ed from the ytsaurus repo via the single wheel
  `ytsaurus-flow-yt-sync-mini` (`yt/python/packages/ytsaurus-flow-yt-sync-mini`, alongside the other
  ytsaurus Python packages; it bundles `pipeline_tables` too), not vendored. Each scenario ships its
  own `yt_sync/` script (PIPELINES/STAGES dicts + `__main__.py`, as in `examples/cpp/noop/yt_sync`)
  that creates the pipeline node and the scenario's queues/tables/consumers/producers.
- **Cluster-name aliasing.** `<cluster=...>` rich-path references resolve to
  `<cluster_name>.yt.yandex.net` by default; every vanilla block must carry
  `proxy_url_aliasing_rules = {<cluster_name> = <internal proxy URL>}`.
- **Queue consumer registrations.** The demo cluster runs a real queue agent (registered in
  `//sys/@cluster_connection/queue_agent`, with `//sys/queue_agents/consumer_registrations`
  provisioned by the queue-agent state migration), so `register_queue_consumer` works through the
  normal path with no manual fixup.
- **Pool**: `$YT_POOL`. Worker/controller job defaults (6 CPU / 18 GiB) fit the demo exec nodes
  (16 CPU / 65 GiB × 5).
- **Binaries** are stripped before use (2.6 GB profile → ~190 MB); `deploy.sh` runs the stripped
  copy, because the runner uploads its own executable for the vanilla jobs.
- **Verification** runs from the dev host over the HTTP API / `yt` CLI: `get-pipeline-state`,
  `select_rows`/`read_table` on outputs, flow-view reads — mirroring the original test's asserts.
- **Layout per scenario** (`yandex/ytsaurus_dev/<scenario>/`):
  - `README.md` — scenario description, expected result, full command sequence (build → bootstrap →
    prepare data → deploy → verify → stop).
  - `pipeline.yson` — spec adapted from the test (single cluster, vanilla block).
  - `pipeline/` — C++ runner program (`main.cpp` + `ya.make`), unless the stock
    `bin/flow_server` binary suffices.
  - `yt_sync/` — yt_sync_mini bootstrap script.
  - Data-prep and verify snippets inside README (or a small `prepare_data.py` / `verify.py` where
    the original test writes non-trivial input).

## Scenarios — implementation order

Ordered simplest-first; complexity S/M/L ≈ new code + verification effort.

| # | Scenario | Source test | Pipeline | Verify (from the original assert) | Cx |
|---|----------|------------|----------|----------------------------------|----|
| 1 | `message_filter` | tests/message_filter | stock binary; TQueueSource → TPassthroughComputation → TSyncQueueSink, `skip_if_expression` filters `key = "bad"` | output queue contains only `good_*` keys | S |
| 2 | `secret_env` | tests/secret_env | TSecretChecker over finite TRandomSource; secret delivered via vanilla secure vault (`secret_env`) | pipeline reaches `Completed` (job asserts `YT_MY_SECRET == "5"`) | S |
| 3 | `shuffle` | tests/shuffle | stock binary; reader → 4 chained TPassthroughComputation with different `group_by_schema` → TSyncQueueSink | `sum(1)` over output queue == events written (1500) | S |
| 4 | `word_count_sync` | tests/word_count_sync | TProcessFunctionSourceComputation + TProcessFunctionComputation, external state `/state`, custom TStopWordsResource, sync side-write of skipped words | `word_counts` == {hello:1, world:1}; `skipped_words` == {a:1, is:2, on:2, it:2} | M |
| 5 | `computation_cycles_and_buffers` | tests/computation_cycles_and_buffers | reader → transform/swift-map cycle (stream closes back into transform_a) → reducer with TSimpleExternalStateManager | state table has 1 row, `count == 1000` (exactly-once through a cycle) | M |
| 6 | `state_joiner` | tests/state_joiner | reader → accumulator + joiner (TProcessFunctionComputation, `state_joiners /user_total`) → sorted-dyntable sink | output table `(UserId, Total)` equals input amounts | M |
| 7 | `external_joiner` | tests/external_joiner | reader → TEnricher with two `external_state_joiners` over prefilled sorted dyntables → sorted-dyntable sink | output rows carry joined `(BankName, UserNickname)`; `read-states` works | M |
| 8 | `static_table` | tests/static_table | TStaticTableConnector::TSource reader (swift variant) → TSyncQueueSink | output queue == rows of the prefilled static tables | M |
| 9 | `sorted_dynamic_table` | tests/sorted_dynamic_table (single-cluster subset) | reader → NSortedDynamicTable::TSyncSink; swift + delete + aggregate spec variants | output table matches expected rows / deletions / aggregates; `states` empty | M |
| 10 | `swift_map_batching` | tests/swift_map_batching | reader → TBatcher → TWriter → TSyncQueueSink | output `event_id` set == `range(2000)`, no loss/dups | M |
| 11 | `test_distributed_throttler` | tests/test_distributed_throttler | reader → TThrottledPassthrough (controller-served throttler `api`) → TSyncQueueSink | `Completed`, output count == 200 | M |
| 12 | `at_most_once_sink` | tests/at_most_once_sink | TReader → two TAsyncQueueSink with `at_most_once_strategy` | control queue complete; data queue loses rows while output was unmounted (at-most-once semantics) | M |
| 13 | `keep_order_mode` | tests/keep_order_mode (happy path) | reader → 2× TRacyPassthroughComputation → TQueueReducer, `relaxed_ordering=false`, event-time ordering | per-`reduce_id` event sequences exactly ordered, no dups | M |
| 14 | `servicelog` | tests/servicelog/merge_profiles | TServiceLogSource with in-source table_joiner over 2 profile tables → TStateKeeper | every key present in state with expected counts | M |
| 15 | `table_injector` | tests/table_injector (primary subset) | TTableReader (finite, watermarks) → TTableWriter → TAsyncQueueSink | output queue == input; `input_messages` table structure as asserted | M |
| 16 | `working_pipeline_telemetry` | tests/working_pipeline_telemetry | TReader over TRandomSource with injected failure → TProcessor | failure comment visible in `describe-pipeline`; flow-view exposes buffer/epoch stats | M |
| 17 | `test_resource_control_channel` | tests/test_resource_control_channel | reader → swift map, custom TCounterResource on controller+workers (2 workers) | flow-view shows all workers + controller decoded the published resource revision | M |
| 18 | `key_visitor` | tests/key_visitor/cpp | swift source → TProcessFunctionComputation with visit-tester function; key-visitor stream | latest visit per key carries v2 payload | L |
| 19 | `companion_python` | tests/companion/passthrough_transform + types/python | python companion (gRPC) inside the worker vanilla job; TTransformCompanionComputation | output mirrors input; native passthrough bypasses companion / type roundtrip | L |
| 20 | `pipeline_alter` | tests/pipeline_alter | reader queue→queue; stop, rename computation in static spec, restart | data intact after rename; source-path change erases old state | L |
| 21 | `transform_high_throughput` | yandex/benchmarks (moved) | TRandomSource → TProcessFunctionComputation (per-key state) → TAsyncQueueSink | throughput reported; `states` table non-empty | L |

## Excluded (with reason)

| Scenario | Reason |
|----------|--------|
| `multi_cluster` | needs 3 YT clusters (cross-cluster sink failover is the point) |
| `read_chaos_tables` | needs chaos cells + 2 clusters |
| `static_table_v2` multi-cluster tests | need a second cluster (single-cluster subset is covered by #8 semantics; may add later) |
| `sorted_dynamic_table` async-replica variant | needs replicated tables across clusters |
| `test_compact_output_store`, `test_input_store`, `test_timer_store` | store-level gtests, not deployable pipelines |
| `recipes` | test infra, not a test |
| `flow_execute` | value is the Arcadia prebuilt-package version matrix; pipeline itself is trivial |
| `buffer_memory_usage`, `process_function_overhead`, `pure_swift_high_throughput` | measure RSS/CPU via `/proc` of co-located worker processes — impossible with vanilla jobs |
| Java companion variants (`companion/*` java, `key_visitor/java*`, `reanimate_vanilla/java`) | Java Flow SDK is Arcadia-internal (`IF (NOT OPENSOURCE)`) |
| `reanimate_vanilla` | its subject (vanilla reanimation tool) partially covered implicitly; cpp variant needs 2 clusters; revisit if wanted |
| `diagnostic_tools`, `start_stop_pipeline_stress`, `ipv4_support` | reuse other pipelines; their asserts are about local tooling/process control; partially covered by our deployment procedure itself |
| `add_message_distribute_flag` | python-companion watermark corner case; fold into #19 if wanted |

## Workflow per scenario

1. Write scenario dir (code, spec, yt_sync script, README).
2. Build + strip binary; bootstrap Cypress objects; prepare input data.
3. Deploy via the bootstrap vanilla operation; wait for `Working` (or `Completed` for finite).
4. Run the verification queries; record actual output in README.
5. Stop pipeline, abort operation (leave Cypress objects for inspection).
6. Commit to the repo; review; next scenario.
