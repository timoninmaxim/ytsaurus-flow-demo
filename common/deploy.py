# Deploys a scenario pipeline to the cluster by running the flow runner on this host.
#
# Usage: python3 common/deploy.py <scenario_name>, or deploy(<scenario_name>) from a scenario script.
#
# Expects in the scenario dir:
#   pipeline.yson.template — runner config with ${YT_PROXY_INTERNAL}, ${YT_PROXY_RPC},
#                            ${YT_CLUSTER_NAME}, ${YT_DEV_ROOT}, ${YT_POOL} placeholders.
#
# The runner connects over RPC (proxy_addresses pinned in the config's clients_cache, because the
# cluster advertises an address that does not resolve outside k8s), uploads its own binary, submits
# the pipeline spec and launches the controller+worker vanilla operation. YT_FLOW_WAIT=0 makes it
# exit once the pipeline is Working instead of tailing it.

import os
import subprocess
import sys
import tempfile

from yt.wrapper.flow_commands import get_pipeline_state

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_BINARY = "~/ytsaurus/yt/yt/flow/bin/flow_server/flow_server"

TEMPLATE_VARS = ("YT_PROXY_INTERNAL", "YT_PROXY_RPC", "YT_CLUSTER_NAME", "YT_DEV_ROOT", "YT_POOL")
REQUIRED_VARS = ("YT_TOKEN", "YT_PROXY") + TEMPLATE_VARS


def deploy(scenario):
    for var in REQUIRED_VARS:
        if not os.environ.get(var):
            sys.exit("error: {} is not set; source your env file first".format(var))

    binary = os.path.expanduser(os.environ.get("FLOW_BINARY", DEFAULT_BINARY))
    if not os.path.exists(binary):
        sys.exit("error: flow_server binary not found: {} (set FLOW_BINARY)".format(binary))

    with open(os.path.join(REPO_ROOT, scenario, "pipeline.yson.template")) as template:
        config = template.read()
    for var in TEMPLATE_VARS:
        config = config.replace("${%s}" % var, os.environ[var])

    with tempfile.NamedTemporaryFile("w", suffix=".yson") as config_file:
        config_file.write(config)
        config_file.flush()
        subprocess.check_call([binary, "--config", config_file.name], env=dict(os.environ, YT_FLOW_WAIT="0"))

    pipeline = "{}/{}/pipeline".format(os.environ["YT_DEV_ROOT"], scenario)
    print("pipeline state: {}".format(get_pipeline_state(pipeline)))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("usage: deploy.py <scenario_name>")
    deploy(sys.argv[1])
