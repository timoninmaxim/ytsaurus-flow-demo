# Loads cluster coordinates and credentials from a file OUTSIDE this repo.
# Nothing sensitive (tokens, logins, addresses) is stored in the repo itself.
#
# Point YT_FLOW_DEMO_ENV at your secrets file; it must export:
#   YT_TOKEN            — cluster token/password
#   YT_PROXY_EXTERNAL   — HTTP proxy URL reachable from the dev host
#   YT_PROXY_INTERNAL   — HTTP proxy URL reachable from inside the cluster (k8s service)
#   YT_CLUSTER_NAME     — cluster name as registered in //sys/clusters
#   YT_DEV_ROOT         — Cypress root for the scenarios, e.g. //tmp/<login>/ytsaurus_dev
#   YT_POOL             — scheduler pool for vanilla operations

set -euo pipefail

: "${YT_FLOW_DEMO_ENV:?Set YT_FLOW_DEMO_ENV to the path of your secrets env file}"
# shellcheck disable=SC1090
source "$YT_FLOW_DEMO_ENV"

for var in YT_TOKEN YT_PROXY_EXTERNAL YT_PROXY_INTERNAL YT_CLUSTER_NAME YT_DEV_ROOT YT_POOL; do
    [ -n "${!var:-}" ] || { echo "error: $var is not set by $YT_FLOW_DEMO_ENV" >&2; exit 1; }
done

export YT_API="$YT_PROXY_EXTERNAL/api/v4"
export YT_AUTH="Authorization: OAuth $YT_TOKEN"

# curl wrappers for the YT HTTP API.
ytcurl() { curl -sS --max-time "${YT_CURL_TIMEOUT:-60}" -H "$YT_AUTH" "$@"; }
ytpost() { local cmd=$1 params=$2; ytcurl -X POST -H 'X-YT-Header-Format: <format=text>yson' -H "X-YT-Parameters: $params" "$YT_API/$cmd"; }
ytput()  { local cmd=$1 params=$2; shift 2; ytcurl -X PUT -H 'X-YT-Header-Format: <format=text>yson' -H "X-YT-Parameters: $params" "$@" "$YT_API/$cmd"; }
ytget()  { local cmd=$1; shift; ytcurl "$YT_API/$cmd" "$@"; }
