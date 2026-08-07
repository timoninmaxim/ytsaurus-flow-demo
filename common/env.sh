# Loads cluster coordinates and credentials from a file NOT tracked by git.
# Nothing sensitive (tokens, logins, addresses) is stored in the repo itself.
#
# Point YT_FLOW_DEMO_ENV at your secrets file (default: <repo root>/env.sh, which is
# gitignored); it must export:
#   YT_TOKEN            — cluster token/password
#   YT_PROXY_EXTERNAL   — HTTP proxy URL reachable from the dev host
#   YT_PROXY_INTERNAL   — HTTP proxy URL reachable from inside the cluster (k8s service)
#   YT_CLUSTER_NAME     — cluster name as registered in //sys/clusters
#   YT_DEV_ROOT         — Cypress root for the scenarios, e.g. //tmp/<login>/ytsaurus_dev
#   YT_POOL             — scheduler pool for vanilla operations
# Optional:
#   YT_PROXY_RPC        — external RPC proxy endpoint (host:port), once the cluster exposes one

set -euo pipefail

YT_FLOW_DEMO_ENV=${YT_FLOW_DEMO_ENV:-"$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/env.sh"}
[ -f "$YT_FLOW_DEMO_ENV" ] || { echo "error: secrets env file not found: $YT_FLOW_DEMO_ENV (set YT_FLOW_DEMO_ENV)" >&2; exit 1; }
# shellcheck disable=SC1090
source "$YT_FLOW_DEMO_ENV"

for var in YT_TOKEN YT_PROXY_EXTERNAL YT_PROXY_INTERNAL YT_CLUSTER_NAME YT_DEV_ROOT YT_POOL; do
    [ -n "${!var:-}" ] || { echo "error: $var is not set by $YT_FLOW_DEMO_ENV" >&2; exit 1; }
done

# The scenarios drive the cluster through the public `yt` CLI, which reads YT_PROXY and YT_TOKEN
# from the environment. Exporting them here is all a child script needs to talk to the cluster.
export YT_PROXY="$YT_PROXY_EXTERNAL"

command -v yt > /dev/null || { echo "error: the yt CLI is not on PATH (pip install ytsaurus-client)" >&2; exit 1; }
