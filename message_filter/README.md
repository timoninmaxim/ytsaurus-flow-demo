# message_filter

Re-implementation of the `yt/yt/flow/tests/message_filter` integration test as a standalone
vanilla-deployed pipeline.

**Scenario.** A queue-to-queue pipeline built entirely from stock classes (the pipeline binary is
the stock `flow_server`): `reader` (`TSwiftPassthroughOrderedSourceComputation` over
`TQueueSource`) → `writer` (`TPassthroughComputation`) → `TSyncQueueSink`. The dynamic spec sets
`skip_if_expression = 'key = "bad"'` on the reader, so blacklisted rows are dropped at the source.

**Expected result.** The output queue receives only the keys `good_0`, `good_1`, `good_2` — the two
`bad` rows are filtered out.

## Run

```bash
python3 yt_sync.py               # Cypress objects (pipeline node, input_queue + consumer, output_queue)
python3 run_message_filter.py    # deploy → 5 rows (good_0, bad, good_1, bad, good_2) → tail the output
python3 run_message_filter.py stop
```

The tail prints every row reaching the output queue, so the `bad` rows are visible by their absence.
The source is not finite: the pipeline keeps running until the explicit `stop` step shuts it down
and aborts its vanilla operation.
