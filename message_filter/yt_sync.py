# Creates the Cypress objects for the message_filter scenario: the pipeline
# node, the input queue with its consumer, and the output queue.
#
# Run: bootstrap.sh (loads the cluster env). Requires the pip-installed
# yt_sync_mini + pipeline_tables packages (see the repo README).

import os

from yt.yt.flow.library.python.yt_sync_mini import StagesSpec, run_yt_sync_easy_mode

QUEUE_SCHEMA = [
    {"name": "key", "type": "string"},
    {"name": "data", "type": "string"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]

CLUSTER = os.environ["YT_PROXY_EXTERNAL"]
FOLDER = os.environ["YT_DEV_ROOT"] + "/message_filter"


def main():
    stages = {
        "default": {},
        "test": {
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
                "clusters": {"_all_data_clusters": {"attributes": {"tablet_count": 1}}},
            },
        }
        for name in ("input_queue", "output_queue")
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
        "message_filter",
        StagesSpec(stages=stages, pipelines=pipelines, tables=tables, consumers=consumers),
        args=["--stage", "test", "--scenario", "ensure", "--parallel-factor", "0", "--commit"],
    )


if __name__ == "__main__":
    main()
