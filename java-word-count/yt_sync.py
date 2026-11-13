import argparse
import os

from yt.yt.flow.library.python.yt_sync_mini import StagesSpec, run_yt_sync_easy_mode


def run_yt_sync(cluster, folder, queue_tablet_count=1):
    stages = {
        "default": {},
        "demo": {
            "folder": folder,
            "presets": {"builtin:storage_preset": {"clusters": {cluster: {}}}},
        },
    }
    tables = {
        "input_queue": {
            "default": {
                "$merge_presets": ["builtin:table_preset"],
                "schema": [
                    {"name": "word", "type": "string"},
                    {"name": "$timestamp", "type": "uint64"},
                    {"name": "$cumulative_data_weight", "type": "int64"},
                ],
                "clusters": {
                    "_all_data_clusters": {"attributes": {"tablet_count": queue_tablet_count}},
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
        "producer": {"default": {"$merge_presets": ["builtin:producer_preset"]}},
    }
    pipelines = {
        "pipeline": {"default": {"$merge_presets": ["builtin:pipeline_preset"]}},
    }
    return run_yt_sync_easy_mode(
        name="wordcount",
        stages_spec=StagesSpec(
            stages=stages,
            tables=tables,
            consumers=consumers,
            producers=producers,
            pipelines=pipelines,
        ),
        args=["--stage", "demo", "--scenario", "ensure", "--parallel-factor", "0", "--commit"],
    )


def main():
    parser = argparse.ArgumentParser(description="Create the WordCount pipeline, queue, consumer and producer.")
    parser.add_argument("--cluster", default=os.environ.get("YT_PROXY"), help="HTTP proxy; defaults to YT_PROXY")
    parser.add_argument("--folder", default=os.environ.get("YT_FLOW_ROOT"), help="Cypress root; defaults to YT_FLOW_ROOT")
    parser.add_argument("--queue-tablet-count", type=int, default=1, help="Number of input queue tablets (default: 1)")
    args = parser.parse_args()
    if not args.cluster:
        parser.error("set --cluster or YT_PROXY")
    if not args.folder or not args.folder.startswith("//") or args.folder.rstrip("/") == "":
        parser.error("set --folder or YT_FLOW_ROOT to a Cypress directory below //")
    if args.queue_tablet_count <= 0:
        parser.error("--queue-tablet-count must be positive")
    return run_yt_sync(args.cluster, args.folder.rstrip("/"), args.queue_tablet_count)


if __name__ == "__main__":
    main()
