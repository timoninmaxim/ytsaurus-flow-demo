# Creates the Cypress objects for the keep_order_mode scenario: the pipeline
# node, the seven-tablet input queue with its consumer, and the single-tablet
# output queue.
#
# Run after sourcing your env file (see the repo README):
#   python3 yt_sync.py
# Requires the pip-installed yt_sync_mini + pipeline_tables packages
# (see the repo README). The ensure flow is idempotent.

import os

from yt.yt.flow.library.python.yt_sync_mini import StagesSpec, run_yt_sync_easy_mode

INPUT_TABLET_COUNT = 7

QUEUE_META_COLUMNS = [
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]

INPUT_QUEUE_SCHEMA = [
    {"name": "reduce_id", "type": "uint64"},
    {"name": "event_id", "type": "int64"},
    {"name": "event_time", "type": "uint64"},
    *QUEUE_META_COLUMNS,
]

OUTPUT_QUEUE_SCHEMA = [
    {"name": "reduce_id", "type": "uint64"},
    {"name": "event_id", "type": "int64"},
    *QUEUE_META_COLUMNS,
]

CLUSTER = os.environ["YT_PROXY"]
FOLDER = os.environ["YT_DEV_ROOT"] + "/keep_order_mode"


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
        "input_queue": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": INPUT_QUEUE_SCHEMA,
                "clusters": {
                    "_all_data_clusters": {
                        "attributes": {
                            "tablet_count": INPUT_TABLET_COUNT,
                            "commit_ordering": "strong",
                        },
                    },
                },
            },
        },
        "output_queue": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": OUTPUT_QUEUE_SCHEMA,
                "clusters": {
                    "_all_data_clusters": {
                        "attributes": {
                            "tablet_count": 1,
                            "commit_ordering": "strong",
                        },
                    },
                },
            },
        },
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
        "keep_order_mode",
        StagesSpec(stages=stages, pipelines=pipelines, tables=tables, consumers=consumers),
        args=["--stage", "demo", "--scenario", "ensure", "--parallel-factor", "0", "--commit"],
    )


if __name__ == "__main__":
    main()
