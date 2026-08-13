# Feeds the input queue of the Go-companion variant — the stand-in for the C++
# variant's TRandomSource, which the stock flow_server does not link. Writes batches of
# rows with random keys drawn from range(1000) (so an ordinary key can never equal the
# fail or panic key), plus, every --fail-every seconds, one row with key = fail_key and a
# unique "data" value: each such row makes the Go reader return an error fail_attempts
# times and then pass (see main.go), i.e. one genuine job failure per fail row. A row
# with key = panic_key (1101) — the panic-shaped counterpart — is inserted manually once
# from the README, not by this feeder.
#
# Run after sourcing your env file (see the repo README), alongside verify.py:
#   python3 feed.py --duration 900

import argparse
import json
import os
import random
import string
import time
import uuid

import yt.wrapper as yt

FAIL_KEY = "1100"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=900, help="seconds to keep feeding")
    parser.add_argument("--rate", type=int, default=800, help="rows per second")
    parser.add_argument("--fail-every", type=int, default=45, help="seconds between fail-key rows")
    args = parser.parse_args()

    queue = os.environ["YT_DEV_ROOT"] + "/working_pipeline_telemetry_go/input_queue"
    client = yt.YtClient(proxy=os.environ["YT_PROXY"], token=os.environ["YT_TOKEN"])

    filler = "".join(random.choices(string.ascii_lowercase, k=120))
    batch_period = 1.0
    batch_size = int(args.rate * batch_period)

    deadline = time.time() + args.duration
    next_fail = time.time()
    total, fails = 0, 0
    while time.time() < deadline:
        started = time.time()
        # The JSON format needs no ytsaurus-yson bindings; a literal "$" in a column
        # name is doubled there ("$$tablet_index"), as everywhere in this repo.
        rows = [
            {"key": str(random.randrange(1000)), "data": filler, "$$tablet_index": 0}
            for _ in range(batch_size)
        ]
        if started >= next_fail:
            rows.append({"key": FAIL_KEY, "data": f"fail-{uuid.uuid4()}", "$$tablet_index": 0})
            next_fail = started + args.fail_every
            fails += 1
        client.insert_rows(
            queue,
            "\n".join(json.dumps(row) for row in rows).encode(),
            format=yt.JsonFormat(),
            raw=True,
        )
        total += len(rows)
        print(f"fed {total} rows ({fails} fail rows)", flush=True)
        time.sleep(max(0.0, batch_period - (time.time() - started)))


if __name__ == "__main__":
    main()
