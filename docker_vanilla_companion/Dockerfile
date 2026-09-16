# An interpreter with the Python companion SDK and ytsaurus-client, named as the jobs' docker_image.
# Keep COMPANION_VERSION equal to the version of the flow_server you deploy.
FROM docker.io/library/python:3.12-slim

ARG COMPANION_VERSION=0.1.0
RUN pip install --no-cache-dir "ytsaurus-flow-companion==${COMPANION_VERSION}" && \
    python -c 'from yt.yt.flow.library.python.companion import Pipeline, server'
