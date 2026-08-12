# resource_control_channel

A pipeline whose subject is the engine's **resource control channel**: a custom resource hosted on
the controller and on every worker, whose controller side keeps publishing new target-revision
spec payloads — and the flow view proving that every worker and the controller-side instance
decoded the delivered payload.

The data path is deliberately trivial: `reader` is the stock
`NYT::NFlow::TSwiftPassthroughOrderedSourceComputation` over the built-in `TRandomSource`, and
`consumer` is a `TProcessFunctionSwiftMapComputation` hosting `NYT::NFlow::NDemo::TNullFunction`,
which discards every message. The pipeline exists only to keep two workers busy while the resource
is exercised.

The resource is the point (`pipeline/main.cpp`):

- `TCounterResourceController` runs inside the pipeline controller. Every `generation_period`
  (3 s) it bumps a counter and publishes `{value = N}` as the Counter resource's target-revision
  spec. It also aggregates the statuses reported back by every instance into its flow view:
  a `workers_per_value` histogram of the values the workers decoded, plus `controller_value` for
  the controller-side instance.
- `TCounterResource` runs on each worker — and once on the controller, since the `consumer`
  computation requests the resource with both `worker = %true` and `controller = %true`. As its
  applied revision id it reports the value it decodes **from the delivered payload**, so the ids
  the controller aggregates prove the payload content crossed the wire, not just a revision stamp.

## This scenario ships its own binary

The resource classes are user C++ registered via `YT_FLOW_DEFINE_RESOURCE`, and they must exist in
**both** processes: the controller instantiates `TCounterResourceController` (and one worker-side
`TCounterResource`), each worker instantiates `TCounterResource`. Companions cannot host this:
`library/cpp/companion` offers only a worker-side proxy for companion-hosted resources
(`TCompanionResource`); there is no companion-hosted resource *controller*, which is the very side
that publishes the payloads. So the scenario builds its own binary, exactly like
`working_pipeline_telemetry`: `build.sh` stages `pipeline/` into your ytsaurus checkout, builds
with `ya make` and strips the result back here (see `secret_env/README.md` for why staging is
needed).

## Deviation from the upstream test, deliberate

The upstream test (`tests/test_resource_control_channel`) places the random-source knobs
(`partition_count`, `message_size_mean`, …) directly under the dynamic spec's
`source_streams/random` instead of nesting them under `parameters`, so the source silently runs on
defaults (3 partitions, λ = 1 M messages, 1 KB each) — the same misplacement the
`working_pipeline_telemetry` README documents for its upstream test. It does not affect the
upstream asserts (the resource machinery is load-independent), it just generates far more traffic
than intended. This scenario nests the knobs correctly, so the source really runs 1 partition ×
~10 × 100-byte messages and the demo cluster stays quiet. Class names moved from the test's
`NExample` to `NYT::NFlow::NDemo`; everything else — spec shape, resource parameters, worker
count (2), the verified conditions — mirrors the test.

## Run

From the repo root:

```bash
resource_control_channel/build.sh             # builds + strips the binary (YTSAURUS=<checkout>)
python3 resource_control_channel/yt_sync.py   # once: the pipeline node (no queues or tables)
FLOW_BIN=resource_control_channel/resource_control_channel_pipeline.stripped \
    ./run.sh resource_control_channel         # deploy + stream the controller log; Ctrl-C detaches
```

Then, from a second terminal, run the checks (each mirrors one upstream assert, with upstream's
one-minute waits):

```bash
python3 resource_control_channel/verify.py
```

What it checks, in order (`get_flow_view` with `cache=False` throughout):

1. `/state/execution_spec/resource_target_revisions/value/Counter/spec/value` ≥ 1 — the
   controller published a Counter payload; remember the value as `first`.
2. `/ephemeral_state/resource_controller_views/Counter` — at least 2 workers sit in
   `workers_per_value` buckets with a decoded value ≥ `first`, and `controller_value` ≥ `first`.
3. The published value grows past `first` — the counter keeps publishing; remember `second`.
4. The same instance check for `second` — the workers and the controller keep catching up.

`TRandomSource` never completes, so stop the pipeline once verified:

```bash
./stop.sh resource_control_channel
```

## Observed output

Recorded from the live run on the demo cluster. The pipeline reached `working` in about
20 seconds; `verify.py` passed on the first run with no retries beyond its own polling:

```
$ python3 resource_control_channel/verify.py
ok: controller published a Counter payload
ok: all 2 workers + controller decoded a payload value >= 2
ok: a newer payload (> 2) was published
ok: all 2 workers + controller decoded a payload value >= 4
final resource controller view: {'controller_value': 4, 'value': 5, 'worker_count': 2, 'workers_per_value': {'4': 2}}
OK: the published payloads reach every worker and the controller, and keep catching up
```

A raw flow-view sample taken a few publications later, while the pipeline was `working`:

```
published target revision: {'revision_id': 1918323797463662699, 'spec': {'value': 7}}
controller view: {'controller_value': 6, 'value': 7, 'worker_count': 2, 'workers_per_value': {'6': 2}}
```

Note the instances trail the freshly published value by one generation — the controller bumps the
counter at publish time, delivery and status feedback take one round trip; the verify conditions
are monotone in time, so the lag never makes them flap (upstream's own design).
