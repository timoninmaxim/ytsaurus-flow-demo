# Creates the Cypress objects for the companion_python scenario: the pipeline
# node, the input queue with its consumer, and the output queue.
#
# Run after sourcing your env file (see the repo README):
#   python3 yt_sync.py
# Requires the pip-installed yt_sync_mini + pipeline_tables packages
# (see the repo README). The ensure flow is idempotent.

import os

from yt.yt.flow.library.python.yt_sync_mini import StagesSpec, run_yt_sync_easy_mode

DATA_COLUMNS = [
    {"name": "key", "type": "string"},
    {"name": "text", "type": "string"},
    {"name": "count", "type": "int64"},
    {"name": "score", "type": "double"},
    {"name": "flag", "type": "boolean"},
]

SYSTEM_COLUMNS = [
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]

INPUT_QUEUE_SCHEMA = DATA_COLUMNS + SYSTEM_COLUMNS
OUTPUT_QUEUE_SCHEMA = DATA_COLUMNS + [{"name": "text_upper", "type": "string"}] + SYSTEM_COLUMNS

CLUSTER = os.environ["YT_PROXY"]
FOLDER = os.environ["YT_DEV_ROOT"] + "/companion_python"


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
                "clusters": {"_all_data_clusters": {"attributes": {"tablet_count": 1}}},
            },
        }
        for name, schema in (("input_queue", INPUT_QUEUE_SCHEMA), ("output_queue", OUTPUT_QUEUE_SCHEMA))
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
        "companion_python",
        StagesSpec(stages=stages, pipelines=pipelines, tables=tables, consumers=consumers),
        args=["--stage", "demo", "--scenario", "ensure", "--parallel-factor", "0", "--commit"],
    )


if __name__ == "__main__":
    main()
