# standalone_direct

The same queue-to-queue pipeline as `message_filter` (stock `flow_server`, `reader` →
`writer` → `TSyncQueueSink`, `skip_if_expression = 'key = "bad"'` drops blacklisted rows at the
source), but launched a different way: no vanilla operation at all. The controller, the worker and
the runner are three plain `flow_server` processes on the dev host, each in its own podman
container of the released image, talking to each other directly instead of through a YT vanilla
operation and the cluster's RPC proxy.

## Why direct mode

A vanilla pipeline's controller and worker run inside the cluster, where the RPC proxy can dial
them to relay the runner's commands (`get-pipeline-state`, `set-pipeline-specs`,
`set-target-pipeline-state`, ...). Here the controller runs on the dev host instead, which the
cluster has no route into — the host is behind NAT, and the RPC proxy's callback would need to
reach a host address it cannot resolve or dial. The runner config's
`direct_controller_commands = {enabled = %true;}` block (see
[docs](https://github.com/ytsaurus/ytsaurus/blob/main/yt/docs/ru/_includes/flow/tools/cli.md#direct-controller-commands))
makes the runner send those commands straight to the controller's RPC port instead of through the
proxy — the one connection this setup actually has, since the runner itself also runs on the host
and can reach the controller. `direct_controller_commands` is a runner-only feature: neither the
`yt` CLI nor the UI has it, so anything that goes through the proxy (`yt flow ...`) still cannot
reach this controller — see Stop below.

The controller also gets `YT_FLOW_SKIP_LEADER_PROXY_CONFIRMATION=1`: without it, it keeps retrying
a proxy round-trip to confirm its own leadership that can never succeed here, and logs it as a
plain failure. With it set, the controller recognizes the situation and logs it as the (benign)
reason `yt flow` and the UI won't work against it.

Because there is no proxy relay, the controller, the worker and the runner must reach each other
directly by address:port, so all three run with `--network host` (unlike a vanilla pipeline, whose
tasks are reached through the cluster's proxy, not dialed by the runner). Each node config also
sets `address_resolver.localhost_name_override = "127.0.0.1"`, so the controller and worker
advertise a loopback address the other host processes can actually dial, instead of their
container's own unresolvable hostname. A node config may only enable one of `enable_ipv4` /
`enable_ipv6` (the released binary rejects both at once with "Exactly one of ... must be set" —
unlike the runner's own `address_resolver`, which keeps both on for the NAT64 route to the
cluster's RPC endpoint); `enable_ipv4 = %true` here matches the IPv4 loopback override, and the
node still reaches the cluster's literal NAT64 address fine since that is a literal address, not
something it resolves.

Security note from the same docs page: with the controller's default `require_proxy_signature =
%false`, any host that can reach its RPC port can issue commands unauthenticated. Fine for this
demo (the port is only exposed on the dev host); do not do this against an RPC port reachable by
anyone untrusted.

## Run

Needs the prerequisites and the sourced env file from the root README (`yt` CLI,
`ytsaurus-flow-yt-sync-mini` and `jinjanate` in one Python environment, podman).

Bootstrap the Cypress objects once, from this dir:

```bash
python3 yt_sync.py   # once: pipeline node, input_queue + consumer, output_queue
```

Render the runner spec and, from the same template, one node config per role — the ports must
differ since all three processes share the host network:

```bash
jinjanate pipeline.yson.j2 > pipeline.yson
FLOW_RPC_PORT=19101 FLOW_MONITORING_PORT=19111 jinjanate node_config.yson.j2 > node_config_controller.yson
FLOW_RPC_PORT=19102 FLOW_MONITORING_PORT=19112 jinjanate node_config.yson.j2 > node_config_worker.yson
```

Three terminals, from this dir (`--network host` so the three processes can dial each other by
port; see "Why direct mode" above):

Terminal 1 — controller:

```bash
podman run --rm --network host -e YT_TOKEN -e YT_FLOW_MODE=Controller \
    -e YT_FLOW_SKIP_LEADER_PROXY_CONFIRMATION=1 -v "$PWD:/app/pipeline" \
    ghcr.io/ytsaurus/flow-nightly:dev-0.2.1 --config node_config_controller.yson
```

Wait for the leadership-confirmation line — logged as a Warning because of the env var above,
mentioning direct mode explicitly:

```
Leadership confirmation through the RPC proxy is skipped; the cluster cannot connect to this
controller, so only the runner reaches it, in direct mode (runner config
direct_controller_commands/enabled); yt flow and the UI will not work
```

Terminal 2 — worker:

```bash
podman run --rm --network host -e YT_TOKEN -e YT_FLOW_MODE=Worker \
    -v "$PWD:/app/pipeline" ghcr.io/ytsaurus/flow-nightly:dev-0.2.1 --config node_config_worker.yson
```

Terminal 3 — runner (no `YT_FLOW_MODE`: bare `flow_server` is the runner):

```bash
podman run --rm --network host -e YT_TOKEN -v "$PWD:/app/pipeline" \
    ghcr.io/ytsaurus/flow-nightly:dev-0.2.1 --config pipeline.yson
```

The runner logs the line that shows it went direct, then submits the spec, starts the pipeline and
streams the controller log; Ctrl-C only detaches, the pipeline keeps running:

```
Pipeline commands go directly to the leader controller
```

Check there is no vanilla operation behind this pipeline — `yt list-operations` must not show one
for this scenario. The pipeline's own state (`working`) is visible in the runner's log
(`Wait finished (CurrentState: Working, TargetState: Working)`) or the controller's
(`Jobs status (PipelineState: Working, ...)`); `yt flow get-pipeline-state` itself does **not**
work here — see Stop below, same cause:

```bash
yt list-operations --state running   # empty, or only unrelated pipelines' operations
```

## Feed and read

Terminal 4:

```bash
echo '{"key": "good_0", "data": "0"}
{"key": "bad",    "data": "1"}
{"key": "good_1", "data": "2"}' | yt insert-rows --format json "$YT_DEV_ROOT/standalone_direct/input_queue"

yt pull-queue "$YT_DEV_ROOT/standalone_direct/output_queue" --offset 0 --partition-index 0 --format json
```

Only the `good_*` rows come back — the `bad` row is dropped by the filter.

## One more direct command

Re-running the runner exercises the same direct-mode round trip again
(`get-pipeline-state` / `set-flow-core-target` / `set-pipeline-specs` / `set-target-pipeline-state`,
all sent straight to the controller): it logs `Pipeline commands go directly to the leader
controller` again. `YT_FLOW_WAIT=0` skips the log-tailing step that otherwise blocks until the
pipeline completes, so the command returns as soon as it confirms `Working` again — a few seconds,
not instant, since the runner still polls the controller for the state:

```bash
podman run --rm --network host -e YT_TOKEN -e YT_FLOW_WAIT=0 -v "$PWD:/app/pipeline" \
    ghcr.io/ytsaurus/flow-nightly:dev-0.2.1 --config pipeline.yson
```

## Stop

The released runner's CLI (`TSimpleRunnerProgram`) only ever drives a pipeline towards `Working` —
this release has no runner flag to submit a `Stopped`/`Paused` target instead, and
`direct_controller_commands` is a runner-only feature, so `yt flow stop-pipeline` /
`pause-pipeline` (through the CLI, hence through the proxy) cannot reach this controller either:

```bash
yt flow get-pipeline-state --pipeline-path "$YT_DEV_ROOT/standalone_direct/pipeline"
# and the same for stop-pipeline / pause-pipeline: the CLI always goes through the proxy, which
# tries to dial the controller's published (loopback) address and fails immediately:
#   Cannot connect to pipeline controller leader. Probably controller is stopped or it is failing
#     Channel terminated
#       Error connecting to [127.0.0.1]:19101
#         Connect error
#           Connection refused
```

What actually works here is stopping the processes themselves: Ctrl-C the runner (terminal 3, just
detaches — the controller and worker keep running), then stop the other two:

```bash
podman stop <controller container> <worker container>
```

or Ctrl-C them in their terminals. Drop the scenario's Cypress objects once stopped:
`yt remove -r "$YT_DEV_ROOT/standalone_direct"`.

## Not covered

The C++ companion, Java/Python/Go SDK launchers and YQL scenarios are out of scope for this
scenario; it only exercises the stock `flow_server` in the three roles above.
