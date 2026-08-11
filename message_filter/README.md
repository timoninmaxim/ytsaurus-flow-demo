# message_filter

A queue-to-queue pipeline built entirely from stock classes (the pipeline binary is the stock
`flow_server`):
`reader` (`TSwiftPassthroughOrderedSourceComputation` over `TQueueSource`) → `writer`
(`TPassthroughComputation`) → `TSyncQueueSink`. The dynamic spec sets
`skip_if_expression = 'key = "bad"'` on the reader, so blacklisted rows are dropped at the source.

## Run

Terminal 1 — bootstrap once, then run the pipeline (from the repo root):

```bash
python3 message_filter/yt_sync.py   # once: pipeline node, input_queue + consumer, output_queue
./run.sh message_filter             # deploy + stream the controller log; Ctrl-C detaches,
                                    # ./stop.sh message_filter stops
```

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
