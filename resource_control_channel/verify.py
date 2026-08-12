# Verifies the resource control channel against the upstream test's asserts
# (tests/test_resource_control_channel, Test.test_target_revision_reaches_workers):
#   - the controller publishes a Counter target-revision payload: the flow view exposes its
#     `value` under /state/execution_spec/resource_target_revisions/value/Counter/spec/value;
#   - every worker (2 of them) and the controller-side resource instance decode a payload value
#     at least that high: the resource controller's own view under
#     /ephemeral_state/resource_controller_views/Counter histograms the per-worker decoded
#     values (workers_per_value) and reports the controller instance's (controller_value).
#     The reported number is read out of the delivered payload, so this asserts the payload
#     itself — not just its revision id — reached the instances;
#   - the counter keeps publishing new payloads and the instances keep catching up: the same
#     two checks again for a strictly newer published value.
#
# Run after sourcing your env file (see the repo README), once the pipeline is
# `working`. Each condition is waited for up to a minute, like upstream:
#   python3 verify.py

import os
import sys
import time

import yt.wrapper as yt

WORKERS_COUNT = 2
TIMEOUT = 60


def wait_for(name, check):
    deadline = time.time() + TIMEOUT
    while True:
        try:
            if check():
                print(f"ok: {name}")
                return True
        except Exception:
            if time.time() > deadline:
                raise
        if time.time() > deadline:
            print(f"FAIL: {name} (not satisfied within {TIMEOUT}s)")
            return False
        time.sleep(2)


def main():
    pipeline = os.environ["YT_DEV_ROOT"] + "/resource_control_channel/pipeline"
    client = yt.YtClient(proxy=os.environ["YT_PROXY"], token=os.environ["YT_TOKEN"])

    # The `value` field of the spec payload the controller published for Counter.
    def get_published_value():
        return client.get_flow_view(
            pipeline,
            view_path="/state/execution_spec/resource_target_revisions/value/Counter/spec/value",
            cache=False,
        )

    # Every worker and the controller-side instance decoded a spec-payload value at least
    # `value`. Monotone in time, so frequently published revisions cannot make it flap.
    def all_instances_applied(value):
        view = client.get_flow_view(
            pipeline,
            view_path="/ephemeral_state/resource_controller_views/Counter",
            cache=False,
        )
        applied = sum(
            count
            for decoded, count in view["workers_per_value"].items()
            if int(decoded) >= value
        )
        return applied >= WORKERS_COUNT and view.get("controller_value", 0) >= value

    failed = False

    failed |= not wait_for("controller published a Counter payload", lambda: get_published_value() >= 1)

    first = get_published_value()
    failed |= not wait_for(
        f"all {WORKERS_COUNT} workers + controller decoded a payload value >= {first}",
        lambda: all_instances_applied(first))

    # The counter keeps publishing new payloads and the workers keep catching up.
    failed |= not wait_for(
        f"a newer payload (> {first}) was published",
        lambda: get_published_value() > first)

    second = get_published_value()
    failed |= not wait_for(
        f"all {WORKERS_COUNT} workers + controller decoded a payload value >= {second}",
        lambda: all_instances_applied(second))

    view = client.get_flow_view(
        pipeline, view_path="/ephemeral_state/resource_controller_views/Counter", cache=False)
    print(f"final resource controller view: {view}")

    if failed:
        return 1
    print("OK: the published payloads reach every worker and the controller, and keep catching up")
    return 0


if __name__ == "__main__":
    sys.exit(main())
