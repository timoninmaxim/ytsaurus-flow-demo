# Creates the Cypress objects for the Java-companion variant of the swift_map_batching
# scenario, under its own root ($YT_DEV_ROOT/swift_map_batching_java) so the C++ and Python
# variants' objects stay inspectable. Same objects as ../yt_sync.py: the pipeline node, the
# five-tablet input queue with its consumer, and the output queue.
#
# Run after sourcing your env file (see the repo README):
#   python3 yt_sync.py
# Requires the pip-installed yt_sync_mini + pipeline_tables packages
# (see the repo README). The ensure flow is idempotent.

import os

from yt.yt.flow.library.python.yt_sync_mini import StagesSpec, run_yt_sync_easy_mode

CLUSTER = os.environ["YT_PROXY"]
FOLDER = os.environ["YT_DEV_ROOT"] + "/swift_map_batching_java"

# Five tablets, as upstream: the reader partitions by tablet, so the events reach the batcher
# spread over five source partitions.
INPUT_QUEUE_TABLET_COUNT = 5

INPUT_QUEUE_SCHEMA = [
    {"name": "event_id", "type": "int64"},
    {"name": "group_key", "type": "uint64"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]

# batch_size is this port's addition: the size of the merged batch each event came out of.
OUTPUT_QUEUE_SCHEMA = [
    {"name": "event_id", "type": "int64"},
    {"name": "batch_size", "type": "int64"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]


def main():
    stages = {
        "default": {},
        "demo": {
            "folder": FOLDER,
            "presets": {
                "builtin:storage_preset": {"clusters": {CLUSTER: {"attributes": {"primary_medium": "default"}}}},
                "builtin:table_preset": {"clusters": {CLUSTER: {"attributes": {"tablet_cell_bundle": "default"}}}},
            },
        },
    }

    pipelines = {
        "pipeline": {
            "default": {
                "$merge_presets": ["builtin:pipeline_preset"],
                "monitoring_project": "",
                "monitoring_cluster": "",
            },
        },
    }

    tables = {
        name: {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": schema,
                "clusters": {"_all_data_clusters": {"attributes": {"tablet_count": tablet_count}}},
            },
        }
        for name, schema, tablet_count in (
            ("input_queue", INPUT_QUEUE_SCHEMA, INPUT_QUEUE_TABLET_COUNT),
            ("output_queue", OUTPUT_QUEUE_SCHEMA, 1),
        )
    }

    consumers = {
        "consumer": {
            "default": {
                "$merge_presets": ["builtin:consumer_preset"],
                "in_stage_queues": {"input_queue": {"vital": True}},
            },
        },
    }

    run_yt_sync_easy_mode(
        "swift_map_batching_java",
        StagesSpec(stages=stages, pipelines=pipelines, tables=tables, consumers=consumers),
        args=["--stage", "demo", "--scenario", "ensure", "--parallel-factor", "0", "--commit"],
    )


if __name__ == "__main__":
    main()
