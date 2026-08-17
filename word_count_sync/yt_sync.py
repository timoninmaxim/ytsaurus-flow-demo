# Creates the Cypress objects for the word_count_sync scenario: the pipeline node, the input
# queue with its consumer, the word_counts external-state table and the skipped_words table.
#
# Run after sourcing your env file (see the repo README):
#   python3 yt_sync.py
# Requires the pip-installed yt_sync_mini + pipeline_tables packages
# (see the repo README). The ensure flow is idempotent.

import os

from yt.yt.flow.library.python.yt_sync_mini import StagesSpec, run_yt_sync_easy_mode

CLUSTER = os.environ["YT_PROXY"]
FOLDER = os.environ["YT_DEV_ROOT"] + "/word_count_sync"

INPUT_QUEUE_SCHEMA = [
    {"name": "text", "type": "string"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]

# The external state of the "counter" computation: keyed exactly by its group_by_schema.
WORD_COUNTS_SCHEMA = [
    {"name": "hash", "expression": "farm_hash(word)", "type": "uint64", "sort_order": "ascending"},
    {"name": "word", "type": "string", "sort_order": "ascending"},
    {"name": "count", "type": "int64"},
]

SKIPPED_WORDS_SCHEMA = [
    {"name": "word", "type": "string", "sort_order": "ascending"},
    {"name": "length", "type": "int64"},
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
                "clusters": {"_all_data_clusters": {"attributes": {"tablet_count": 1}}},
            },
        }
        for name, schema in (
            ("input_queue", INPUT_QUEUE_SCHEMA),
            ("word_counts", WORD_COUNTS_SCHEMA),
            ("skipped_words", SKIPPED_WORDS_SCHEMA),
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
        "word_count_sync",
        StagesSpec(stages=stages, pipelines=pipelines, tables=tables, consumers=consumers),
        args=["--stage", "demo", "--scenario", "ensure", "--parallel-factor", "0", "--commit"],
    )


if __name__ == "__main__":
    main()
