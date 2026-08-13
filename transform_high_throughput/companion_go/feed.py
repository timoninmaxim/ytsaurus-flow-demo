# Feeds the input queue of the Go-companion variant — the stand-in for the C++
# variant's TRandomSource, which the stock flow_server does not link. Mirrors the
# generator's distributions: keys are drawn from the normal approximation of
# Poisson(1000000) (the C++ spec's message_key_range is a Poisson *mean*, giving
# ~6-8 thousand distinct keys around one million), payloads are 100-byte strings
# (message_size_mean), rows alternate between the queue's two tablets (the two
# source partitions).
#
# The feed must outrun the pipeline so that the *pipeline* is the bottleneck being
# measured: keep the total feed rate well above the expected throughput and check the
# backlog line of companion_go/measure.py. One process tops out around 15K rows/s
# (the insert path is a single HTTP writer); to feed faster, run several processes in
# parallel — appends to an ordered table interleave safely:
#   python3 companion_go/feed.py --duration 900 &
#   python3 companion_go/feed.py --duration 900 &
# The per-second report prints the achieved cumulative rate; a "behind" warning means
# this process cannot keep the pace and its --rate is effectively lower.
#
# Run after sourcing your env file (see the repo README), alongside measure.py:
#   python3 companion_go/feed.py --duration 600

import argparse
import json
import os
import random
import string
import time

import yt.wrapper as yt

TABLET_COUNT = 2
KEY_MEAN = 1000000
KEY_SIGMA = 1000  # sqrt(KEY_MEAN): normal approximation of Poisson(KEY_MEAN).
PAYLOAD_SIZE = 100
PAYLOAD_POOL_SIZE = 64


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=600, help="seconds to keep feeding")
    parser.add_argument("--rate", type=int, default=15000, help="rows per second")
    args = parser.parse_args()

    queue = os.environ["YT_DEV_ROOT"] + "/transform_high_throughput_go/input_queue"
    client = yt.YtClient(proxy=os.environ["YT_PROXY"], token=os.environ["YT_TOKEN"])

    pool = [
        "".join(random.choices(string.ascii_lowercase, k=PAYLOAD_SIZE))
        for _ in range(PAYLOAD_POOL_SIZE)
    ]

    batch_period = 1.0
    batch_size = int(args.rate * batch_period)

    started_at = time.time()
    deadline = started_at + args.duration
    total = 0
    while time.time() < deadline:
        started = time.time()
        # The JSON format needs no ytsaurus-yson bindings; a literal "$" in a column
        # name is doubled there ("$$tablet_index"), as everywhere in this repo.
        rows = [
            {
                "key": str(int(random.gauss(KEY_MEAN, KEY_SIGMA))),
                "data": random.choice(pool),
                "$$tablet_index": i % TABLET_COUNT,
            }
            for i in range(batch_size)
        ]
        client.insert_rows(
            queue,
            "\n".join(json.dumps(row) for row in rows).encode(),
            format=yt.JsonFormat(),
            raw=True,
        )
        total += len(rows)
        elapsed_total = time.time() - started_at
        achieved = total / elapsed_total if elapsed_total > 0 else 0.0
        behind = " [behind: insert took longer than the batch period]" if time.time() - started > batch_period else ""
        print(f"fed {total} rows, {achieved:.0f} rows/s achieved{behind}", flush=True)
        time.sleep(max(0.0, batch_period - (time.time() - started)))


if __name__ == "__main__":
    main()
