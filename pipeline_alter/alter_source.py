# The source-path-change step of the alter choreography: with the pipeline
# stopped, point the reader's queue source at the alternate input queue and
# its consumer — exactly what the upstream test does between stop and restart
# (YTFLOW-525: a changed source identity must retire the old partitions and
# erase their state).
#
# Run after `yt flow stop-pipeline` has reported the pipeline stopped:
#   python3 alter_source.py

import os

import yt.wrapper as yt


def main():
    folder = os.environ["YT_DEV_ROOT"] + "/pipeline_alter"
    cluster = os.environ["YT_CLUSTER_NAME"]
    client = yt.YtClient(proxy=os.environ["YT_PROXY"], token=os.environ["YT_TOKEN"])

    spec = client.get_pipeline_spec(folder + "/pipeline")["spec"]
    parameters = spec["computations"]["reader"]["source_streams"]["queue"]["parameters"]
    parameters["queue_path"] = f"<cluster={cluster}>{folder}/input_queue_alt"
    parameters["consumer_path"] = f"<cluster={cluster}>{folder}/consumer_alt"
    client.set_pipeline_spec(folder + "/pipeline", spec)
    print("static spec: reader source switched to input_queue_alt / consumer_alt")


if __name__ == "__main__":
    main()
