#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

usage() {
  echo "usage: $0 [dashboard.json]" >&2
}

fail() {
  echo "Grafana Cloud deploy failed: $1" >&2
  exit 1
}

need_command() {
  if ! command -v "$1" >/dev/null 2>&1; then
    fail "$1 command is required"
  fi
}

require_env() {
  local name="$1"
  local value="${!name:-}"

  if [[ -z "$value" ]]; then
    fail "$name is required"
  fi
}

body_summary() {
  local body="$1"

  if jq -er '.message // .error // empty' "$body" >/dev/null 2>&1; then
    jq -r '.message // .error' "$body"
  else
    head -c 500 "$body"
  fi
}

api_request() {
  local method="$1"
  local path="$2"
  local output="$3"
  local data_file="${4:-}"
  local status

  local curl_args=(
    curl
    --config "$CURL_CONFIG"
    -sS
    -o "$output"
    -w "%{http_code}"
    -X "$method"
    "$GRAFANA_STACK_URL_NORMALIZED$path"
  )

  if [[ -n "$data_file" ]]; then
    curl_args+=(
      -H "Content-Type: application/json"
      --data-binary "@$data_file"
    )
  fi

  status="$("${curl_args[@]}")" || fail "request failed: $method $path"
  printf '%s' "$status"
}

expect_success() {
  local status="$1"
  local path="$2"
  local body="$3"

  case "$status" in
    2*) ;;
    *) fail "$path returned HTTP $status: $(body_summary "$body")" ;;
  esac
}

check_plugin() {
  local plugin_id="$1"
  local plugin_name="$2"
  local plugin_body="$TMP_DIR/plugin-$plugin_id.json"
  local plugin_status

  plugin_status="$(api_request GET "/api/plugins/$plugin_id/settings" "$plugin_body")"
  case "$plugin_status" in
    2*) echo "$plugin_name plugin is installed." ;;
    404) fail "$plugin_name plugin is not installed. Install $plugin_id in Grafana Cloud, or set GRAFANA_SKIP_PLUGIN_CHECK=1 to skip plugin checks." ;;
    *) fail "$plugin_name plugin check failed with HTTP $plugin_status: $(body_summary "$plugin_body")" ;;
  esac
}

if [[ $# -gt 1 ]]; then
  usage
  exit 2
fi

if [[ $# -eq 1 ]]; then
  if [[ "$1" = /* ]]; then
    DASHBOARD="$1"
  else
    DASHBOARD="$ROOT/$1"
  fi
else
  DASHBOARD="$ROOT/grafana/dashboards/microcms-content-ops-analytics.json"
fi

if [[ ! -f "$DASHBOARD" ]]; then
  fail "dashboard file not found: $DASHBOARD"
fi

need_command curl
need_command jq
require_env GRAFANA_STACK_URL
require_env GRAFANA_STACK_SERVICE_ACCOUNT_TOKEN

GRAFANA_STACK_URL_NORMALIZED="${GRAFANA_STACK_URL%/}"
if [[ -z "$GRAFANA_STACK_URL_NORMALIZED" ]]; then
  fail "GRAFANA_STACK_URL is required"
fi

GRAFANA_DASHBOARD_UID="${GRAFANA_DASHBOARD_UID:-microcms-content-ops}"
if [[ -z "$GRAFANA_DASHBOARD_UID" ]]; then
  fail "GRAFANA_DASHBOARD_UID must not be empty"
fi

GRAFANA_DASHBOARD_NAMESPACE="${GRAFANA_DASHBOARD_NAMESPACE:-default}"
if [[ -z "$GRAFANA_DASHBOARD_NAMESPACE" ]]; then
  fail "GRAFANA_DASHBOARD_NAMESPACE must not be empty"
fi

QUERY_API_URL="${QUERY_API_URL:-}"
if [[ -z "$QUERY_API_URL" ]]; then
  need_command tofu
  QUERY_API_URL="$(tofu -chdir="$ROOT/infra/aws" output -raw query_api_url)"
fi
if [[ -z "$QUERY_API_URL" ]]; then
  fail "QUERY_API_URL is required or infra/aws query_api_url output must be available"
fi

"$ROOT/scripts/validate-grafana-dashboard.sh" "$DASHBOARD"

TMP_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

CURL_CONFIG="$TMP_DIR/curl.conf"
{
  printf 'header = "Authorization: Bearer %s"\n' "$GRAFANA_STACK_SERVICE_ACCOUNT_TOKEN"
  printf 'header = "Accept: application/json"\n'
} >"$CURL_CONFIG"
chmod 600 "$CURL_CONFIG"

if [[ "${GRAFANA_SKIP_PLUGIN_CHECK:-0}" = "1" ]]; then
  echo "Skipping Grafana plugin checks because GRAFANA_SKIP_PLUGIN_CHECK=1."
else
  check_plugin "yesoreyeram-infinity-datasource" "Infinity datasource"
  check_plugin "tim012432-calendarheatmap-panel" "Calendar Heatmap"
fi

datasource_is_default=false
datasource_get_body="$TMP_DIR/datasource-get.json"
datasource_get_status="$(api_request GET "/api/datasources/uid/duckdb-query-api" "$datasource_get_body")"
case "$datasource_get_status" in
  2*) datasource_is_default="$(jq -r 'if .isDefault == true then "true" else "false" end' "$datasource_get_body")" ;;
  404) ;;
  *)
    fail "/api/datasources/uid/duckdb-query-api returned HTTP $datasource_get_status: $(body_summary "$datasource_get_body")"
    ;;
esac

datasource_payload="$TMP_DIR/datasource.json"
jq -n \
  --arg url "$QUERY_API_URL" \
  --argjson is_default "$datasource_is_default" \
  '{
    name: "DuckDB Query API",
    uid: "duckdb-query-api",
    type: "yesoreyeram-infinity-datasource",
    access: "proxy",
    url: $url,
    isDefault: $is_default,
    editable: true,
    jsonData: {},
    secureJsonData: {}
  }' >"$datasource_payload"

case "$datasource_get_status" in
  2*)
    datasource_body="$TMP_DIR/datasource-update-response.json"
    datasource_status="$(api_request PUT "/api/datasources/uid/duckdb-query-api" "$datasource_body" "$datasource_payload")"
    expect_success "$datasource_status" "/api/datasources/uid/duckdb-query-api" "$datasource_body"
    echo "Updated Grafana datasource duckdb-query-api."
    ;;
  404)
    datasource_body="$TMP_DIR/datasource-create-response.json"
    datasource_status="$(api_request POST "/api/datasources" "$datasource_body" "$datasource_payload")"
    expect_success "$datasource_status" "/api/datasources" "$datasource_body"
    echo "Created Grafana datasource duckdb-query-api."
    ;;
esac

dashboard_folder_uid="${GRAFANA_FOLDER_UID:-}"
dashboard_resource_version=""
dashboard_api_path="/apis/dashboard.grafana.app/v2/namespaces/$GRAFANA_DASHBOARD_NAMESPACE/dashboards/$GRAFANA_DASHBOARD_UID"
dashboard_get_body="$TMP_DIR/dashboard-get.json"
dashboard_get_status="$(api_request GET "$dashboard_api_path" "$dashboard_get_body")"
case "$dashboard_get_status" in
  2*)
    if [[ -z "$dashboard_folder_uid" ]]; then
      dashboard_folder_uid="$(jq -r '.metadata.annotations["grafana.app/folder"] // ""' "$dashboard_get_body")"
    fi
    dashboard_resource_version="$(jq -r '.metadata.resourceVersion // ""' "$dashboard_get_body")"
    ;;
  404) ;;
  *)
    fail "$dashboard_api_path returned HTTP $dashboard_get_status: $(body_summary "$dashboard_get_body")"
    ;;
esac

dashboard_payload="$TMP_DIR/dashboard.json"
jq \
  --arg uid "$GRAFANA_DASHBOARD_UID" \
  --arg folder_uid "$dashboard_folder_uid" \
  --arg resource_version "$dashboard_resource_version" \
  '
    .apiVersion = "dashboard.grafana.app/v2"
    | .kind = "Dashboard"
    | .metadata.name = $uid
    | .metadata.annotations = (
        (.metadata.annotations // {})
        + {"grafana.app/message": "Provisioned by scripts/deploy-grafana-cloud.sh"}
        + if $folder_uid == "" then {} else {"grafana.app/folder": $folder_uid} end
      )
    | if $resource_version == "" then
        del(.metadata.resourceVersion)
      else
        .metadata.resourceVersion = $resource_version
      end
    | del(
        .metadata.uid,
        .metadata.generation,
        .metadata.creationTimestamp,
        .metadata.managedFields,
        .metadata.namespace
      )
  ' "$DASHBOARD" >"$dashboard_payload"

dashboard_body="$TMP_DIR/dashboard-response.json"
dashboard_status="$(api_request PUT "$dashboard_api_path" "$dashboard_body" "$dashboard_payload")"
expect_success "$dashboard_status" "$dashboard_api_path" "$dashboard_body"
echo "Upserted Grafana dashboard $GRAFANA_DASHBOARD_UID via dashboard.grafana.app/v2."
