# Creates the Cypress objects for one variant of the sorted_dynamic_table scenario: the pipeline
# node, the input queue with its consumer, and the sorted output table the sink writes to.
#
# Every variant lives in its own Cypress subtree, because they differ in the *static* part of the
# pipeline spec (and the aggregate variant in the output table's schema), and a static-spec change
# needs a fresh pipeline.
#
# Run after sourcing your env file (see the repo README):
#   python3 yt_sync.py {swift|delete|aggregate}
# Requires the pip-installed yt_sync_mini + pipeline_tables packages
# (see the repo README). The ensure flow is idempotent.

import os
import sys

from yt.yt.flow.library.python.yt_sync_mini import StagesSpec, run_yt_sync_easy_mode

VARIANTS = ("swift", "delete", "aggregate")

CLUSTER = os.environ["YT_PROXY"]

INPUT_QUEUE_SCHEMA = [
    {"name": "data", "type": "string"},
    {"name": "i", "type": "int64"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]


def output_table_schema(variant):
    # "data" alone is the key: the sink writes whatever the message payload holds, so the payload
    # schema and the table schema are the same two columns. The aggregate variant marks "i" with
    # the "sum" aggregate function, which is what turns the sink's writes into read-modify-writes.
    value_column = {"name": "i", "type": "int64"}
    if variant == "aggregate":
        value_column["aggregate"] = "sum"
    return [
        {"name": "data", "type": "string", "required": True, "sort_order": "ascending"},
        value_column,
    ]


def main():
    variant = sys.argv[1] if len(sys.argv) > 1 else ""
    if variant not in VARIANTS:
        sys.exit(f"usage: yt_sync.py {{{'|'.join(VARIANTS)}}}")

    folder = f"{os.environ['YT_DEV_ROOT']}/sorted_dynamic_table/{variant}"

    stages = {
        "default": {},
        "demo": {
            "folder": folder,
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
            ("output_table", output_table_schema(variant)),
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
        f"sorted_dynamic_table_{variant}",
        StagesSpec(stages=stages, pipelines=pipelines, tables=tables, consumers=consumers),
        args=["--stage", "demo", "--scenario", "ensure", "--parallel-factor", "0", "--commit"],
    )


if __name__ == "__main__":
    main()
