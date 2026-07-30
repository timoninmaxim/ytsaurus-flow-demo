# message_filter

Re-implementation of the `yt/yt/flow/tests/message_filter` integration test as a standalone
vanilla-deployed pipeline.

**Scenario.** A queue-to-queue pipeline built entirely from stock classes (the pipeline binary is
the stock `flow_server`): `reader` (`TSwiftPassthroughOrderedSourceComputation` over
`TQueueSource`, finite) → `writer` (`TPassthroughComputation`) → `TSyncQueueSink`. The dynamic
spec sets `skip_if_expression = 'key = "bad"'` on the reader, so blacklisted rows are dropped at
the source.

**Expected result.** The pipeline drains the input queue and reaches `Completed`; the output
queue contains only the keys `good_0`, `good_1`, `good_2` (the two `bad` rows are filtered out).
This mirrors the original test's assertion `keys == ["good_0", "good_1", "good_2"]`.

## Run

```bash
export YT_FLOW_DEMO_ENV=~/path/to/your/env.sh

./bootstrap.sh              # pipeline node, input_queue + consumer, output_queue
./prepare_data.sh           # 5 rows: good_0, bad, good_1, bad, good_2
../common/deploy.sh message_filter
./verify.sh                 # waits for Completed, asserts output keys
```

The pipeline is finite — no `stop.sh` needed; abort the controller/worker vanilla operation after
verification if you do not plan to inspect it.

## Run record (2026-07-30)

Executed end-to-end with the scripts in this dir (`bootstrap.sh` → `prepare_data.sh` →
`deploy.sh` → `verify.sh`): pipeline reached `Working`, drained the input, finished `Completed`;
`verify.sh` printed `output keys: good_0,good_1,good_2` / `PASS: bad rows were filtered out`.
