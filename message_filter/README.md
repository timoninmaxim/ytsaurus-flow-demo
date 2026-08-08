# message_filter

Re-implementation of the `yt/yt/flow/tests/message_filter` integration test as a standalone
vanilla-deployed pipeline.

**Scenario.** A queue-to-queue pipeline built entirely from stock classes (the pipeline binary is
the stock `flow_server`): `reader` (`TSwiftPassthroughOrderedSourceComputation` over
`TQueueSource`) → `writer` (`TPassthroughComputation`) → `TSyncQueueSink`. The dynamic spec sets
`skip_if_expression = 'key = "bad"'` on the reader, so blacklisted rows are dropped at the source.

**Expected result.** The output queue receives only the keys `good_0`, `good_1`, `good_2` — the two
`bad` rows are filtered out. This mirrors the original test's assertion
`keys == ["good_0", "good_1", "good_2"]`.

## Run

```bash
source ../env.sh           # your private env file, once per shell

python3 yt_sync.py         # Cypress objects (pipeline node, input_queue + consumer, output_queue)
                           # idempotent; re-run if it hits transient master lag
python3 scenario.py        # deploy → 5 rows (good_0, bad, good_1, bad, good_2) → check → stop
```

The source is not finite, so the pipeline keeps running until the `stop` step shuts it down and
aborts its vanilla operation.
