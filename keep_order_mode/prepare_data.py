# Fills the input queue with the upstream test's data set: 2500 events across
# ten profiles (reduce_id 0..9), spread over the queue's seven tablets by
# reduce_id % 7. Within each profile event_id counts up 0, 1, 2, ... in write
# order — that sequence is exactly what the pipeline must reproduce — with
# "skip me" events (event_id = -1) interleaved by a fixed rule and random
# event_time noise on the rest, so event-time order deliberately disagrees
# with write order.
#
# Run after sourcing your env file (see the repo README):
#   python3 prepare_data.py

import os
import random
import sys

from collections import defaultdict

import yt.wrapper as yt

EVENT_COUNT = 2500
PROFILE_COUNT = 10
INPUT_TABLET_COUNT = 7
BATCH_SIZE = 500


def generate_events():
    """The upstream generator: (reduce_id, event_id) pairs in write order.

    Deterministic — verify.py replays it to compute the expected output.
    Returns (events, expected) where expected maps reduce_id to the exact
    event_id sequence the output queue must carry.
    """
    events = []
    expected = defaultdict(list)
    for i in range(1000000000):
        for p in range(PROFILE_COUNT):
            if i % (p + 10) == 0:
                if len(events) >= EVENT_COUNT:
                    return events, expected
                if i * 147 % (p + 11) == 0:
                    events.append((p, -1))
                else:
                    event_id = len(expected[p])
                    expected[p].append(event_id)
                    events.append((p, event_id))


def main():
    queue = os.environ["YT_DEV_ROOT"] + "/keep_order_mode/input_queue"
    client = yt.YtClient(proxy=os.environ["YT_PROXY"], token=os.environ["YT_TOKEN"])

    events, expected = generate_events()

    # JSON doubles a literal "$" in a column name; the plain "$tablet_index"
    # spelling belongs to the YSON path, which needs the ytsaurus-yson bindings.
    rows = [
        {
            "reduce_id": p,
            "event_id": event_id,
            # The -1 events carry a fixed event_time, the rest a random one —
            # so ordering by event time would NOT reproduce the write order.
            "event_time": 1000000 if event_id == -1 else 1000000 + random.randint(0, 1000),
            "$$tablet_index": p % INPUT_TABLET_COUNT,
        }
        for p, event_id in events
    ]

    # The client's default row format is YSON, which raises "YSON bindings required" unless
    # the separate ytsaurus-yson package is installed; JSON works with ytsaurus-client alone.
    for start in range(0, len(rows), BATCH_SIZE):
        client.insert_rows(queue, rows[start:start + BATCH_SIZE], format="json")

    skipped = sum(1 for _, event_id in events if event_id == -1)
    print(f"inserted {len(rows)} rows into {queue} "
          f"({skipped} of them event_id=-1, expected output rows: {len(rows) - skipped})")


if __name__ == "__main__":
    sys.exit(main())
