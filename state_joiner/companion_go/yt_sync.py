# Creates the Cypress objects for the Go-companion variant of the state_joiner scenario,
# under its own root ($YT_DEV_ROOT/state_joiner_go) so the C++ variant's objects stay
# inspectable. Same objects as ../yt_sync.py: the pipeline node, the input queue with its
# consumer, the user_totals table the accumulator keeps its per-user totals in, and the
# output_table the joiner's sink writes to.
#
# Run after sourcing your env file (see the repo README):
#   python3 yt_sync.py
# Requires the pip-installed yt_sync_mini + pipeline_tables packages
# (see the repo README). The ensure flow is idempotent.

import os

from yt.yt.flow.library.python.yt_sync_mini import StagesSpec, run_yt_sync_easy_mode

CLUSTER = os.environ["YT_PROXY"]
FOLDER = os.environ["YT_DEV_ROOT"] + "/state_joiner_go"

INPUT_QUEUE_SCHEMA = [
    {"name": "UserId", "type": "string"},
    {"name": "Amount", "type": "int64"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]

# The accumulator's external state: keyed exactly by its group_by_schema, one value column.
USER_TOTALS_SCHEMA = [
    {"name": "Hash", "expression": "farm_hash(UserId)", "type": "uint64", "sort_order": "ascending"},
    {"name": "UserId", "type": "string", "required": True, "sort_order": "ascending"},
    {"name": "Total", "type": "int64"},
]

OUTPUT_TABLE_SCHEMA = [
    {"name": "Hash", "expression": "farm_hash(UserId)", "type": "uint64", "sort_order": "ascending"},
    {"name": "UserId", "type": "string", "required": True, "sort_order": "ascending"},
    {"name": "Total", "type": "int64", "required": True},
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
            ("user_totals", USER_TOTALS_SCHEMA),
            ("output_table", OUTPUT_TABLE_SCHEMA),
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
        "state_joiner_go",
        StagesSpec(stages=stages, pipelines=pipelines, tables=tables, consumers=consumers),
        args=["--stage", "demo", "--scenario", "ensure", "--parallel-factor", "0", "--commit"],
    )


if __name__ == "__main__":
    main()
