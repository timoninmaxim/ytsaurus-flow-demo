# Verifies the engine telemetry against the upstream test's asserts
# (tests/working_pipeline_telemetry, Test.test_telemetry):
#   - the injected failure's comment is visible in `describe-pipeline` under the
#     reader computation's messages — and, stronger than upstream, inside an
#     actual job-failure message rather than the "Spec" info message that echoes
#     the static spec (which contains fail_comment verbatim and would satisfy
#     the upstream `in str(messages)` check even if the failure never fired);
#   - the flow view's per-partition job statuses expose epoch statistics
#     (epoch_part_times on the reader) and buffer/store statistics
#     (input_buffer_bytes on the processor; output_buffer_bytes,
#     output_store_bytes, output_store_count on the reader);
#   - `describe-workers` lists the worker and `get-worker-backtraces` returns a
#     non-empty text for it.
#
# This is the reference ../verify.py with only the pipeline path changed to the
# Java-companion variant's root; every check is identical.
#
# Run after sourcing your env file (see the repo README), once the pipeline is
# `working` and feed.py is running in another terminal. Each check is retried
# for up to three minutes, like upstream:
#   python3 verify.py

import os
import sys
import time

import yt.wrapper as yt

FAIL_COMMENT = "TELEMETRY_DEMO_INTENTIONAL_FAIL"
TIMEOUT = 180


def wait_for(name, check):
    deadline = time.time() + TIMEOUT
    while True:
        try:
            if check():
                print(f"ok: {name}")
                return True
        except Exception as ex:
            if time.time() > deadline:
                raise
        if time.time() > deadline:
            print(f"FAIL: {name} (not satisfied within {TIMEOUT}s)")
            return False
        time.sleep(3)


def main():
    pipeline = os.environ["YT_DEV_ROOT"] + "/working_pipeline_telemetry_java/pipeline"
    client = yt.YtClient(proxy=os.environ["YT_PROXY"], token=os.environ["YT_TOKEN"])

    failed = False

    # Upstream: FAIL_COMMENT in str(description["computations"]["reader"]["messages"]).
    # Here the comment must come from a reported job failure, so the "Spec" info
    # message (which echoes the static spec, fail_comment included) is excluded.
    def check_job_fail_error():
        description = client.flow_execute(pipeline, "describe-pipeline")
        for message in description["computations"]["reader"]["messages"]:
            if message.get("text") != "Spec" and FAIL_COMMENT in str(message):
                print(f"    job-failure message: {message.get('text')}")
                return True
        return False

    failed |= not wait_for("fail comment in a describe-pipeline reader job-failure message", check_job_fail_error)

    def find_job_status(computation_id, filter_func):
        flow_view = client.get_flow_view(pipeline, cache=False)
        partitions = flow_view["state"]["execution_spec"]["layout"]["partitions"]
        partition_job_statuses = flow_view["feedback"]["partition_job_statuses"]
        for partition_id, partition in partitions.items():
            if partition["computation_id"] != computation_id:
                continue
            job_status = partition_job_statuses.get(partition_id, {}).get("current_job_status")
            if not job_status:
                continue
            if filter_func(job_status):
                return job_status
        return None

    # Upstream: sum(epoch_part_times) > 0 on a reader job.
    def check_epoch_part_times(job_status):
        return sum(job_status.get("epoch_part_times", {}).values()) > 0

    failed |= not wait_for(
        "reader epoch_part_times in flow view",
        lambda: find_job_status("reader", check_epoch_part_times) is not None)

    # Upstream: used input_buffer_bytes > 0 on a processor job.
    def check_input_limits(job_status):
        input_buffer = job_status.get("input_limits", {}).get("input_buffer_bytes", {})
        return sum(v.get("used", 0) for v in input_buffer.values()) > 0

    failed |= not wait_for(
        "processor input_buffer_bytes in flow view",
        lambda: find_job_status("processor", check_input_limits) is not None)

    # Upstream: used output_buffer_bytes / output_store_bytes / output_store_count > 0 on a reader job.
    def get_output_limits_checker(name):
        def checker(job_status):
            return sum(v.get("used", 0) for v in job_status.get("output_limits", {}).get(name, {}).values()) > 0
        return checker

    for name in ("output_buffer_bytes", "output_store_bytes", "output_store_count"):
        failed |= not wait_for(
            f"reader {name} in flow view",
            lambda name=name: find_job_status("reader", get_output_limits_checker(name)) is not None)

    # Upstream: describe-workers lists a worker and get-worker-backtraces returns text for it.
    workers = client.flow_execute(pipeline, "describe-workers")
    if len(workers["workers"]) > 0:
        print(f"ok: describe-workers lists {len(workers['workers'])} worker(s)")
    else:
        failed = True
        print("FAIL: describe-workers lists no workers")

    if workers["workers"]:
        worker_address = workers["workers"][0]["address"]
        res = client.flow_execute(pipeline, "get-worker-backtraces", {"worker": worker_address})
        text = res["text"]
        if isinstance(text, (str, bytes)) and len(text) > 0:
            print(f"ok: get-worker-backtraces returned {len(text)} bytes for {worker_address}")
        else:
            failed = True
            print("FAIL: get-worker-backtraces returned no text")

    if failed:
        return 1
    print("OK: failure comment reported, buffer/epoch telemetry exposed, worker backtraces work")
    return 0


if __name__ == "__main__":
    sys.exit(main())
