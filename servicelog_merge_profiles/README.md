# servicelog_merge_profiles

A two-stage pipeline built entirely from stock classes (the pipeline binary is the stock
`flow_server`) whose subject is the **servicelog connector**: `TServiceLogSource` traverses a
sorted dynamic table by hash ranges and its **in-source `table_joiner`** merges a second profile
table into every row on the fly, prefixing the joined columns and synthesizing a
`<prefix>ispresent` marker for keys the secondary table does not have.

```
reader ────────────> key_reducer ──> output_queue
(TServiceLogSource    (group by hash, key,
 over profiles         7 partitions)
 ⨝ another_profiles,
 5 partitions)
```

- `reader` is `TSwiftPassthroughOrderedSourceComputation` over `NYT::NFlow::TServiceLogSource`
  with `finite = %true` — the source completes once every hash range has been traversed, and the
  pipeline completes with it. The `table_joiner` has two fetchers: `profiles` (empty prefix — the
  primary table that drives the traversal) and `another_profiles` under the prefix `merged.`, so
  each emitted event carries `key`, `value`, `second_value` plus `merged.value`,
  `merged.second_value` and the synthesized `merged.ispresent`.
- `key_reducer` is a `TPassthroughComputation` grouped by the upstream test's
  `(hash, key)` schema — every event repartitions from the source's 5 hash-range partitions into
  7 key partitions, exactly as the upstream `state_keeper` did — and materializes each delivery
  into the output queue through a `TSyncQueueSink`.

The data set is the upstream test's: `profiles` holds 1500 rows (`key` 0..1499,
`value = key + 1`, `second_value = key + 2`); `another_profiles` holds the same keys **except
every tenth** (`key % 10 == 0`), with `value = key + 3`, `second_value = key + 4`. The 150 holes
are the point: the joiner must mark them `merged.ispresent = false` with null merged columns
rather than dropping or mangling the row.

**What it proves** — after the finite traversal completes, the output queue holds exactly 1500
rows, one per key (the upstream per-key state counts: `len(rows) == 1500`, `count == 1` — no key
lost, none delivered twice), and every row carries the correct merge: primary columns intact,
`merged.* == (key+3, key+4, true)` for joined keys, `(null, null, false)` for the holes.

## Run

From the repo root, with your env file sourced and the stock stripped `flow_server` built (see the
repo README):

```bash
python3 servicelog_merge_profiles/yt_sync.py         # once: pipeline node, 2 profile tables, output queue
python3 servicelog_merge_profiles/prepare_data.py    # 1500 + 1350 profile rows
./run.sh servicelog_merge_profiles                   # deploys and waits; finite pipeline -> the runner
                                                     # exits by itself on completion (~2 min)
```

The runner streams the controller log and returns once it prints `Pipeline completed`. Then:

```bash
yt flow get-pipeline-state "$YT_DEV_ROOT/servicelog_merge_profiles/pipeline"
python3 servicelog_merge_profiles/verify.py          # the upstream asserts: 1500 keys, once each, correct merge
./stop.sh servicelog_merge_profiles                  # aborts the vanilla operation (pipeline is completed)
```

## Observed output

Recorded against the demo cluster, `flow_server: 26.2.0-local-os~5c69dd1804e43fe5`:

```
$ python3 servicelog_merge_profiles/prepare_data.py
inserted 1500 rows into //tmp/timoninmaxim/ytsaurus_dev/servicelog_merge_profiles/profiles
inserted 1350 rows into //tmp/timoninmaxim/ytsaurus_dev/servicelog_merge_profiles/another_profiles

$ yt flow get-pipeline-state "$YT_DEV_ROOT/servicelog_merge_profiles/pipeline"
completed

$ python3 servicelog_merge_profiles/verify.py
output rows: 1500 (expected 1500)
OK: every key delivered exactly once with the correct merge (1350 keys joined, 150 marked absent)

$ yt select-rows "key, value, second_value, [merged.ispresent] as p, [merged.value] as mv, \
    [merged.second_value] as ms from [$YT_DEV_ROOT/servicelog_merge_profiles/output_queue] \
    where key in (0, 1, 10, 11)" --format json
{"key":10,"value":11,"second_value":12,"p":false,"mv":null,"ms":null}
{"key":1,"value":2,"second_value":3,"p":true,"mv":4,"ms":5}
{"key":11,"value":12,"second_value":13,"p":true,"mv":14,"ms":15}
{"key":0,"value":1,"second_value":2,"p":false,"mv":null,"ms":null}

$ ./stop.sh servicelog_merge_profiles
pipeline is completed (final state, nothing to stop)
operation 478638fb-3f7b3ab3-103e8-b7bba35e (...) aborted
```

Deploy to `Pipeline completed` took about 105 seconds (working after ~45 s, the 10-second
traversal cycle plus draining after that); the only errors in the log were the usual E-level
`leader_controller_address is not set` flood while the controller was still starting.

## Rerunning

The finite source remembers its completed traversal in the pipeline state, so a repeat run means
recreating the scenario from scratch:

```bash
yt remove -r "$YT_DEV_ROOT/servicelog_merge_profiles"
python3 servicelog_merge_profiles/yt_sync.py
python3 servicelog_merge_profiles/prepare_data.py
```

Recreating the tables invalidates the proxies' table mount cache, so the first `insert-rows` or
`select-rows` afterwards can fail with `No such tablet`; the Python client retries writes by
itself, for a read just repeat the command a few seconds later.

## Differences from the integration test this is ported from

Upstream: `yt/yt/flow/tests/servicelog/merge_profiles/` (`test_servicelog.py`,
`pipeline/main.cpp`, `pipeline/pipeline.yson`) in the ytsaurus repo. This ports `test_finite` —
the variant whose asserts are about the data (every key in state exactly once, merge columns
verified per row); the other variants layer throttling timing windows, non-existing-table
resilience and injected worker restarts on the same topology, and their asserts are about the
local test harness's timing and process control.

- **Both custom computations are replaced by stock classes**, which is why the stock binary
  suffices:
  - Upstream's `TReader` is a verbatim passthrough over the source — literally what
    `TSwiftPassthroughOrderedSourceComputation` is.
  - Upstream's `TStateKeeper` is a per-key delivery counter: it `YT_VERIFY`s the primary and
    merged columns of every message and increments a `count` in per-key external state, which the
    test then reads back (`count == 1` per key for the finite run). Here the same keyed
    computation is the stock passthrough and each delivery is materialized into the output queue
    instead, so the per-key count is the number of queue rows per key and the column `YT_VERIFY`s
    become explicit checks in `verify.py` — the observable asserts are unchanged, and a
    duplicate delivery or a bad merge fails the verifier just as it would have crashed the job.
- **The join itself is untouched**: the same two fetchers (`prefix = ""` and `prefix = "merged."`),
  the same profile schemas with the computed `farm_hash(key)` hash column, the same event-stream
  schema with the dotted `merged.*` columns.
- **Numbers are upstream's non-sanitizer finite set:** 1500 rows, `max_rows_per_batch = 150`,
  `desired_partition_count = 5` for the source and 7 for the keyed stage,
  `desired_cycle_time = "10s"`, `throttler_period = "10s"`, `job_tracker/job_threads = 4`.
- **Two workers, one controller** instead of upstream's four workers and two controllers with the
  bullied-process federation (`ProblemsConfig` soft restarts) — that harness supervises local
  processes and cannot run inside vanilla jobs.
- **`enable_dynamic_store_read` is set on both profile tables** as upstream does; the fetchers'
  default `fetch_type = "table_reader"` path does not see freshly inserted, unflushed rows
  without it.
