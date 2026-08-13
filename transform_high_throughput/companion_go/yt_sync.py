# Creates the Cypress objects for the Go-companion variant of the
# transform_high_throughput scenario, under its own root
# ($YT_DEV_ROOT/transform_high_throughput_go) so the C++ and Python variants' objects stay
# inspectable. Unlike the C++ variant (whose input is the in-binary TRandomSource, so it
# needs nothing but the pipeline node and the output queue), this variant reads a queue:
# the stock flow_server does not link the random connector, so the input is a queue fed
# from the dev host (see feed.py). Two tablets on the input queue give the reader the C++
# variant's two source partitions.
#
# Run after sourcing your env file (see the repo README):
#   python3 companion_go/yt_sync.py
# Requires the pip-installed yt_sync_mini + pipeline_tables packages
# (see the repo README). The ensure flow is idempotent.

import os

from yt.yt.flow.library.python.yt_sync_mini import StagesSpec, run_yt_sync_easy_mode

TABLET_COUNT = 2

# The two `$`-columns are the queue system columns required by ordered (queue) tables.
QUEUE_SCHEMA = [
    {"name": "key", "type": "string"},
    {"name": "data", "type": "string"},
    {"name": "$timestamp", "type": "uint64"},
    {"name": "$cumulative_data_weight", "type": "int64"},
]

CLUSTER = os.environ["YT_PROXY"]
FOLDER = os.environ["YT_DEV_ROOT"] + "/transform_high_throughput_go"


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
                "schema": QUEUE_SCHEMA,
                "clusters": {"_all_data_clusters": {"attributes": {"tablet_count": TABLET_COUNT}}},
            },
        },
        "output_queue": {
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

    producers = {
        "output_producer": {"default": {"$merge_presets": ["builtin:producer_preset"]}},
    }

    run_yt_sync_easy_mode(
        "transform_high_throughput_go",
        StagesSpec(
            stages=stages,
            pipelines=pipelines,
            tables=tables,
            consumers=consumers,
            producers=producers,
        ),
        args=["--stage", "demo", "--scenario", "ensure", "--parallel-factor", "0", "--commit"],
    )


if __name__ == "__main__":
    main()
