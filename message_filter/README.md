# message_filter

A queue-to-queue pipeline built entirely from stock classes (the pipeline binary is the stock
`flow_server`):
`reader` (`TSwiftPassthroughOrderedSourceComputation` over `TQueueSource`) → `writer`
(`TPassthroughComputation`) → `TSyncQueueSink`. The dynamic spec sets
`skip_if_expression = 'key = "bad"'` on the reader, so blacklisted rows are dropped at the source.

## Run

Needs the prerequisites and the sourced env file from the root README (`yt` CLI,
`ytsaurus-flow-yt-sync-mini` and `jinjanate` in one Python environment, podman).

Terminal 1 — from this dir: bootstrap the Cypress objects once, render the spec, deploy:

```bash
python3 yt_sync.py   # once: pipeline node, input_queue + consumer, output_queue
jinjanate pipeline.yson.j2 > pipeline.yson   # every {{ VAR }} from the env; an unset one fails the render
podman run --rm -e YT_TOKEN -v "$PWD:/app/pipeline" ghcr.io/ytsaurus/flow-nightly:dev-0.2.1 --config pipeline.yson
```

The last command uploads the released `flow_server`, launches the controller+worker vanilla
operation and streams the controller log; Ctrl-C only detaches, the pipeline keeps running.
The image's entrypoint is `flow_server`, and it starts in `/app/pipeline`.

Terminal 2 — feed the input queue and watch the output:

```bash
echo '{"key": "good_0", "data": "0"}
{"key": "bad",    "data": "1"}
{"key": "good_1", "data": "2"}' | yt insert-rows --format json "$YT_DEV_ROOT/message_filter/input_queue"

yt pull-queue "$YT_DEV_ROOT/message_filter/output_queue" --offset 0 --partition-index 0 --format json
```

Only the `good_*` rows come back — the `bad` row is dropped by the filter. Insert more rows and
pull again: nothing consumes the output queue, so `--offset` is just a row count and `0` always
shows everything.

## Stop

From the repo root:

```bash
./stop.sh message_filter
```

It stops the pipeline and aborts its vanilla operation. To drop the scenario's Cypress objects as
well: `yt remove -r "$YT_DEV_ROOT/message_filter"`.
