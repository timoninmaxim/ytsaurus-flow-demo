# The rename step of the alter choreography: with the pipeline stopped, rename
# the computation "reader" to "reader_renamed" in both the static and the
# dynamic spec — exactly what the upstream test does between stop and restart.
#
# Run after `yt flow stop-pipeline` has reported the pipeline stopped:
#   python3 alter_rename.py

import os

import yt.wrapper as yt


def main():
    pipeline = os.environ["YT_DEV_ROOT"] + "/pipeline_alter/pipeline"
    client = yt.YtClient(proxy=os.environ["YT_PROXY"], token=os.environ["YT_TOKEN"])

    spec = client.get_pipeline_spec(pipeline)["spec"]
    spec["computations"]["reader_renamed"] = spec["computations"].pop("reader")
    client.set_pipeline_spec(pipeline, spec)
    print("static spec: computation 'reader' renamed to 'reader_renamed'")

    dynamic_spec = client.get_pipeline_dynamic_spec(pipeline)["spec"]
    dynamic_spec["computations"]["reader_renamed"] = dynamic_spec["computations"].pop("reader")
    client.set_pipeline_dynamic_spec(pipeline, dynamic_spec)
    print("dynamic spec: computation 'reader' renamed to 'reader_renamed'")


if __name__ == "__main__":
    main()
