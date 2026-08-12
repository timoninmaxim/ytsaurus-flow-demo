# Creates the Cypress objects for the servicelog_merge_profiles scenario: the
# pipeline node, the two profile tables the in-source table_joiner reads, and
# the output queue the per-key deliveries are materialized into.
#
# Run after sourcing your env file (see the repo README):
#   python3 yt_sync.py
# Requires the pip-installed yt_sync_mini + pipeline_tables packages
# (see the repo README). The ensure flow is idempotent.

import os

from yt.yt.flow.library.python.yt_sync_mini import StagesSpec, run_yt_sync_easy_mode

# The upstream test's profile schema: hash is a computed key column, so
# prepare_data.py never writes it — the cluster evaluates the expression.
PROFILE_SCHEMA = [
    {"name": "hash", "expression": "farm_hash(key)", "type": "uint64", "sort_order": "ascending"},
    {"name": "key", "type": "int64", "sort_order": "ascending"},
    {"name": "value", "type": "int64"},
    {"name": "second_value", "type": "int64"},
]

# Everything the joined event stream carries, so verify.py can check the merge
# itself, not just the delivery counts.
OUTPUT_QUEUE_SCHEMA = [
    {"name": "hash", "type": "uint64"},
    {"name": "key", "type": "int64"},
    {"name": "value", "type": "int64"},
    {"name": "second_value", "type": "int64"},
    {"name": "merged.ispresent", "type": "boolean"},
    {"name": "merged.value", "type": "int64"},
    {"name": "merged.second_value", "type": "int64"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]

CLUSTER = os.environ["YT_PROXY"]
FOLDER = os.environ["YT_DEV_ROOT"] + "/servicelog_merge_profiles"


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
        # The source's fetchers read the profile tables through a table reader,
        # which sees freshly inserted rows only with dynamic store read on
        # (the upstream test sets the same attribute).
        name: {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": PROFILE_SCHEMA,
                "clusters": {
                    "_all_data_clusters": {
                        "attributes": {"enable_dynamic_store_read": True},
                    },
                },
            },
        }
        for name in ("profiles", "another_profiles")
    }

    tables["output_queue"] = {
        "default": {
            "$merge_presets": ["builtin:table_preset"],
            "schema": OUTPUT_QUEUE_SCHEMA,
            "clusters": {"_all_data_clusters": {"attributes": {"tablet_count": 1}}},
        },
    }

    run_yt_sync_easy_mode(
        "servicelog_merge_profiles",
        StagesSpec(stages=stages, pipelines=pipelines, tables=tables),
        args=["--stage", "demo", "--scenario", "ensure", "--parallel-factor", "0", "--commit"],
    )


if __name__ == "__main__":
    main()
