# Creates the Cypress objects for the transform_high_throughput scenario: the
# pipeline node, the output queue the async sink writes to, and the producer
# node it writes through. There is no input queue — the source is the built-in
# random generator.
#
# Run after sourcing your env file (see the repo README):
#   python3 yt_sync.py
# Requires the pip-installed yt_sync_mini + pipeline_tables packages
# (see the repo README). The ensure flow is idempotent.

import os

from yt.yt.flow.library.python.yt_sync_mini import StagesSpec, run_yt_sync_easy_mode

TABLET_COUNT = 2

# The two `$`-columns are the queue system columns required by ordered (queue) tables.
OUTPUT_QUEUE_SCHEMA = [
    {"name": "key", "type": "string"},
    {"name": "data", "type": "string"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]

CLUSTER = os.environ["YT_PROXY"]
FOLDER = os.environ["YT_DEV_ROOT"] + "/transform_high_throughput"


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
        "output_queue": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": OUTPUT_QUEUE_SCHEMA,
                "clusters": {
                    "_all_data_clusters": {
                        "attributes": {
                            "tablet_count": TABLET_COUNT,
                            "commit_ordering": "strong",
                        },
                    },
                },
            },
        },
    }

    producers = {
        "output_producer": {"default": {"$merge_presets": ["builtin:producer_preset"]}},
    }

    run_yt_sync_easy_mode(
        "transform_high_throughput",
        StagesSpec(stages=stages, pipelines=pipelines, tables=tables, producers=producers),
        args=["--stage", "demo", "--scenario", "ensure", "--parallel-factor", "0", "--commit"],
    )


if __name__ == "__main__":
    main()
