# Creates the Cypress objects for the at_most_once_sink scenario: the pipeline
# node, the input queue with its consumer, the two output queues (data +
# control), and the producer node the async sinks write through.
#
# Run after sourcing your env file (see the repo README):
#   python3 yt_sync.py
# Requires the pip-installed yt_sync_mini + pipeline_tables packages
# (see the repo README). The ensure flow is idempotent.

import os

from yt.yt.flow.library.python.yt_sync_mini import StagesSpec, run_yt_sync_easy_mode

TABLET_COUNT = 5

QUEUE_SCHEMA = [
    {"name": "data", "type": "string"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]

CLUSTER = os.environ["YT_PROXY"]
FOLDER = os.environ["YT_DEV_ROOT"] + "/at_most_once_sink"


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
                "schema": QUEUE_SCHEMA,
                "clusters": {
                    "_all_data_clusters": {
                        "attributes": {
                            "tablet_count": TABLET_COUNT,
                            "commit_ordering": "strong",
                        },
                    },
                },
            },
        }
        for name in ("input_queue", "output_queue", "control_output_queue")
    }

    consumers = {
        "consumer": {
            "default": {
                "$merge_presets": ["builtin:consumer_preset"],
                "in_stage_queues": {"input_queue": {"vital": True}},
            },
        },
    }

    producers = {
        "producer": {"default": {"$merge_presets": ["builtin:producer_preset"]}},
    }

    run_yt_sync_easy_mode(
        "at_most_once_sink",
        StagesSpec(stages=stages, pipelines=pipelines, tables=tables, consumers=consumers, producers=producers),
        args=["--stage", "demo", "--scenario", "ensure", "--parallel-factor", "0", "--commit"],
    )


if __name__ == "__main__":
    main()
