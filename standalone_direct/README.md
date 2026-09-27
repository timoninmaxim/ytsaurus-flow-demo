# standalone_direct

The `message_filter` pipeline (`reader` → `writer` → `TSyncQueueSink`), but launched a different
way — no vanilla operation at all — and with the filtering itself moved into a **Java** companion:

```
reader (TSwiftPassthroughOrderedSourceComputation over TQueueSource)
   → event_in
writer (NCompanion::TTransformCompanionComputation, the Java function MessageFilter)
   → event_out → TSyncQueueSink → output_queue
```

`writer` drops rows whose `key` is blacklisted (`"bad"`) and, for the rest, adds `data_upper`
(`data` uppercased) — proof the row went through the Java code rather than the stock C++
passthrough this scenario used to run. The controller, the worker and the runner are three plain
`flow_server` processes on the dev host, each in its own podman container of the released image,
talking to each other directly instead of through a YT vanilla operation and the cluster's RPC
proxy.

## How the companion gets into the job

`TJavaCompanionManager` resource whose `classpath` names jars the job environment already holds is
taken from there rather than shipped. The scenario dir is mounted at `/app/pipeline` in every
container (controller, worker and runner alike), so the pipeline spec's `resources.CompanionManager`
sets `classpath = "/app/pipeline/lib/*"` (the jars built by `installLib`, see Build) and
`jdk_bin_path = "/opt/java/openjdk/bin/java"` (the `flow-java-nightly` image's own `java`, at the
`eclipse-temurin` path) — both exactly as the worker's own container sees them, since the worker is
the one that spawns the companion process. `main_class` names the entry point,
`StandaloneDirectMain`, registered in the same class as the runner's own `main`.

The worker container therefore needs the same two things the runner container needs: the `lib/`
jars mounted in, and a JVM on its `PATH` — which is part of `flow-java-nightly`.

## Build

From this dir, in the official Gradle container — resolves the SDK from the Sonatype snapshot
repository, runs the offline tests (`MessageFilterTest`) and collects the pipeline jar with its
dependencies into `lib/`:

```bash
podman run --rm -v "$PWD:/src" -w /src docker.io/library/gradle:8-jdk17 \
    gradle -q --refresh-dependencies test installLib
```

## Direct mode

Here the controller runs on the dev host, which the YT cluster has no route into. 
The runner config's `direct_controller_commands = {enabled = %true;}` block (see
[docs](https://github.com/ytsaurus/ytsaurus/blob/main/yt/docs/ru/_includes/flow/tools/cli.md#direct-controller-commands))
makes the runner send commands straight to the controller's RPC port instead of through the
YT RPC proxy — the one connection this setup actually has, since the runner also runs on the host.
`direct_controller_commands` is a runner-only feature: neither the `yt` CLI nor the UI has it, so
anything that goes through the proxy (`yt flow ...`) still cannot reach this controller — see Stop
below.

The controller also gets `YT_FLOW_SKIP_LEADER_PROXY_CONFIRMATION=1`: without it, it keeps retrying
a proxy round-trip to confirm its own leadership that can never succeed here, and logs it as a
plain failure; with it set, it logs the same fact as the benign reason `yt flow` and the UI won't
work against it.

## Run

Needs the prerequisites and the sourced env file from the root README (`yt` CLI,
`ytsaurus-flow-yt-sync-mini` and `jinjanate` in one Python environment, podman).

Bootstrap the Cypress objects once, from this dir:

```bash
python3 yt_sync.py   # once: pipeline node, input_queue + consumer, output_queue
```

Render the runner spec and, from the same template, one node config per role — the ports must
differ since all three processes share the host network; only the worker's companion actually
listens on its two (the controller never spawns one, see "How the companion gets into the job"):

```bash
jinjanate pipeline.yson.j2 > pipeline.yson
jinjanate node_config.yson.j2 > node_config_controller.yson
jinjanate node_config.yson.j2 > node_config_worker.yson
```

Three terminals, from this dir (`--network host` so the three processes can dial each other by
port; see "Why direct mode" above):

Terminal 1 — controller:

```bash
podman run --rm --network host -e YT_TOKEN -e YT_FLOW_MODE=Controller \
    -e YT_FLOW_SKIP_LEADER_PROXY_CONFIRMATION=1 -v "$PWD:/app/pipeline" \
    --entrypoint /usr/bin/flow_server ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1 \
    --config node_config_controller.yson
```

Wait for the leadership-confirmation line, logged as a Warning because of the env var above:

```
Leadership confirmation through the RPC proxy is skipped; the cluster cannot connect to this
controller, so only the runner reaches it, in direct mode (runner config
direct_controller_commands/enabled); yt flow and the UI will not work
```

Terminal 2 — worker (this is the process that spawns the Java companion; the companion's own log
lines appear here too):

```bash
podman run --rm --network host -e YT_TOKEN -e YT_FLOW_MODE=Worker -v "$PWD:/app/pipeline" \
    --entrypoint /usr/bin/flow_server ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1 \
    --config node_config_worker.yson
```

Terminal 3 — runner, the Java SDK launcher (`FlowApplication.run`; no `YT_FLOW_MODE`, so it takes
the runner path and execs `flow_server` itself):

```bash
podman run --rm --network host -e YT_TOKEN -v "$PWD:/app/pipeline" \
    ghcr.io/ytsaurus/flow-java-nightly:dev-0.2.1 -cp 'lib/*' \
    tech.ytsaurus.flow.demo.standalonedirect.StandaloneDirectMain --config pipeline.yson
```

The runner logs the line that shows it went direct, then submits the spec, starts the pipeline and
streams the controller log; Ctrl-C only detaches, the pipeline keeps running.

## Feed and read

Terminal 4:

```bash
echo '{"key": "good_0", "data": "hello"}
{"key": "bad",    "data": "dropped"}
{"key": "good_1", "data": "flow"}' | yt insert-rows --format json "$YT_DEV_ROOT/standalone_direct/input_queue"

yt pull-queue "$YT_DEV_ROOT/standalone_direct/output_queue" --offset 0 --partition-index 0 --format json
```

Only the `good_*` rows come back, each with `data_upper` filled in by the Java companion — the
`bad` row is dropped by the filter:

```json
{"key":"good_0","data":"hello","data_upper":"HELLO"}
{"key":"good_1","data":"flow","data_upper":"FLOW"}
```

## Stop

Re-submitting new pipeline spec is done with the same runner command. It first stops pipeline, updates spec,
and starts pipeline.

For stopping the processes: Ctrl-C the runner (terminal 3, just
detaches — the controller and worker keep running), then stop the other two:

```bash
podman stop <controller container> <worker container>
```

or Ctrl-C them in their terminals. Drop the scenario's Cypress objects once stopped:
`yt remove -r "$YT_DEV_ROOT/standalone_direct"`.
