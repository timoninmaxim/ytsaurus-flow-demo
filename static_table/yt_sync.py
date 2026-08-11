# Creates the Cypress objects for the static_table scenario: the pipeline node and the output
# queue. The scenario's input is a directory of plain static tables, which is not a Flow entity —
# prepare_data.py creates it with the ordinary yt client.
#
# Run after sourcing your env file (see the repo README):
#   python3 yt_sync.py
# Requires the pip-installed yt_sync_mini + pipeline_tables packages
# (see the repo README). The ensure flow is idempotent.

import os

from yt.yt.flow.library.python.yt_sync_mini import StagesSpec, run_yt_sync_easy_mode

CLUSTER = os.environ["YT_PROXY"]
FOLDER = os.environ["YT_DEV_ROOT"] + "/static_table"

# "flow_queue_meta" carries the per-row event timestamp the sink writes when
# write_flow_queue_meta is on; without the column the sink's write is rejected.
OUTPUT_QUEUE_SCHEMA = [
    {"name": "data", "type": "string"},
    {"name": "flow_queue_meta", "type": "any"},
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
        "output_queue": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": OUTPUT_QUEUE_SCHEMA,
                "clusters": {"_all_data_clusters": {"attributes": {"tablet_count": 1}}},
            },
        },
    }

    run_yt_sync_easy_mode(
        "static_table",
        StagesSpec(stages=stages, pipelines=pipelines, tables=tables),
        args=["--stage", "demo", "--scenario", "ensure", "--parallel-factor", "0", "--commit"],
    )


if __name__ == "__main__":
    main()
