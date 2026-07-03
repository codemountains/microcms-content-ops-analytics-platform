#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCRIPT="$ROOT/scripts/deploy-grafana-cloud.sh"

TMP_DIR="$(mktemp -d)"
cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

assert_contains() {
  local file="$1"
  local expected="$2"

  if ! grep -Fq "$expected" "$file"; then
    echo "expected $file to contain: $expected" >&2
    echo "--- $file ---" >&2
    cat "$file" >&2
    exit 1
  fi
}

assert_not_contains() {
  local file="$1"
  local unexpected="$2"

  if grep -Fq "$unexpected" "$file"; then
    echo "expected $file not to contain: $unexpected" >&2
    echo "--- $file ---" >&2
    cat "$file" >&2
    exit 1
  fi
}

assert_json() {
  local file="$1"
  local filter="$2"
  local message="$3"

  if ! jq -e "$filter" "$file" >/dev/null; then
    echo "$message" >&2
    echo "--- $file ---" >&2
    jq . "$file" >&2
    exit 1
  fi
}

write_fake_curl() {
  local mode="$1"
  local bin_dir="$2"

  cat >"$bin_dir/curl" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

method="GET"
output=""
write_out=""
data_file=""
url=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    -X)
      method="$2"
      shift 2
      ;;
    -o)
      output="$2"
      shift 2
      ;;
    -w)
      write_out="$2"
      shift 2
      ;;
    --data-binary)
      data_file="${2#@}"
      shift 2
      ;;
    -H|-sS|-fsS)
      if [[ "$1" = "-H" ]]; then
        shift 2
      else
        shift
      fi
      ;;
    http*)
      url="$1"
      shift
      ;;
    *)
      shift
      ;;
  esac
done

path="${url#https://example.grafana.net}"
printf '%s %s\n' "$method" "$path" >>"$FAKE_CURL_LOG"

status=200
body='{}'

case "$path" in
  /api/plugins/yesoreyeram-infinity-datasource/settings)
    if [[ "$FAKE_CURL_MODE" = "infinity-missing" ]]; then
      status=404
      body='{"message":"Plugin not found"}'
    else
      body='{"id":"yesoreyeram-infinity-datasource","enabled":true}'
    fi
    ;;
  /api/plugins/tim012432-calendarheatmap-panel/settings)
    if [[ "$FAKE_CURL_MODE" = "plugin-missing" ]]; then
      status=404
      body='{"message":"Plugin not found"}'
    else
      body='{"id":"tim012432-calendarheatmap-panel","enabled":true}'
    fi
    ;;
  /api/datasources/uid/duckdb-query-api)
    if [[ "$method" = "PUT" && -n "$data_file" ]]; then
      cp "$data_file" "$FAKE_CURL_TMP/datasource-update.json"
      body='{"message":"Datasource updated"}'
    elif [[ "$FAKE_CURL_MODE" = "datasource-existing-default" ]]; then
      body='{"uid":"duckdb-query-api","isDefault":true}'
    elif [[ "$FAKE_CURL_MODE" = "datasource-existing" ]]; then
      body='{"uid":"duckdb-query-api","isDefault":false}'
    else
      status=404
      body='{"message":"Data source not found"}'
    fi
    ;;
  /api/dashboards/uid/microcms-content-ops)
    if [[ "$FAKE_CURL_MODE" = datasource-existing* ]]; then
      body='{"dashboard":{"uid":"microcms-content-ops"},"meta":{"folderUid":"existing-folder"}}'
    else
      status=404
      body='{"message":"Dashboard not found"}'
    fi
    ;;
  /api/datasources)
    if [[ "$method" != "POST" ]]; then
      status=405
      body='{"message":"unexpected method"}'
    elif [[ -n "$data_file" ]]; then
      cp "$data_file" "$FAKE_CURL_TMP/datasource-create.json"
      body='{"message":"Datasource added"}'
    fi
    ;;
  /api/dashboards/db)
    if [[ "$method" != "POST" ]]; then
      status=405
      body='{"message":"unexpected method"}'
    elif [[ -n "$data_file" ]]; then
      cp "$data_file" "$FAKE_CURL_TMP/dashboard.json"
      body='{"status":"success","uid":"microcms-content-ops"}'
    fi
    ;;
  *)
    status=404
    body='{"message":"not found"}'
    ;;
esac

if [[ -n "$output" ]]; then
  printf '%s' "$body" >"$output"
else
  printf '%s' "$body"
fi

if [[ -n "$write_out" ]]; then
  printf '%s' "$status"
fi
EOF
  chmod +x "$bin_dir/curl"

  cat >"$bin_dir/tofu" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' 'http://alb.example.local'
EOF
  chmod +x "$bin_dir/tofu"

  FAKE_CURL_MODE="$mode"
}

run_with_fakes() {
  local mode="$1"
  shift

  local bin_dir="$TMP_DIR/bin-$mode"
  mkdir -p "$bin_dir"
  write_fake_curl "$mode" "$bin_dir"

  FAKE_CURL_TMP="$TMP_DIR/out-$mode"
  FAKE_CURL_LOG="$FAKE_CURL_TMP/curl.log"
  mkdir -p "$FAKE_CURL_TMP"
  : >"$FAKE_CURL_LOG"

  PATH="$bin_dir:$PATH" \
    FAKE_CURL_MODE="$mode" \
    FAKE_CURL_TMP="$FAKE_CURL_TMP" \
    FAKE_CURL_LOG="$FAKE_CURL_LOG" \
    "$@"
}

test_missing_required_env() {
  local out="$TMP_DIR/missing-env.out"

  if env -u GRAFANA_STACK_URL -u GRAFANA_STACK_SERVICE_ACCOUNT_TOKEN "$SCRIPT" >"$out" 2>&1; then
    echo "expected missing env validation to fail" >&2
    exit 1
  fi

  assert_contains "$out" "GRAFANA_STACK_URL is required"
}

test_create_datasource_and_dashboard() {
  run_with_fakes "datasource-missing" env \
    GRAFANA_STACK_URL="https://example.grafana.net/" \
    GRAFANA_STACK_SERVICE_ACCOUNT_TOKEN="secret-token" \
    GRAFANA_FOLDER_UID="ops" \
    "$SCRIPT"

  assert_contains "$FAKE_CURL_LOG" "GET /api/plugins/tim012432-calendarheatmap-panel/settings"
  assert_contains "$FAKE_CURL_LOG" "GET /api/plugins/yesoreyeram-infinity-datasource/settings"
  assert_contains "$FAKE_CURL_LOG" "GET /api/datasources/uid/duckdb-query-api"
  assert_contains "$FAKE_CURL_LOG" "POST /api/datasources"
  assert_contains "$FAKE_CURL_LOG" "POST /api/dashboards/db"
  assert_not_contains "$FAKE_CURL_LOG" "secret-token"

  assert_json "$FAKE_CURL_TMP/datasource-create.json" \
    '.uid == "duckdb-query-api" and .type == "yesoreyeram-infinity-datasource" and .access == "proxy" and .url == "http://alb.example.local" and .isDefault == false' \
    "datasource create payload did not match"
  assert_json "$FAKE_CURL_TMP/dashboard.json" \
    '.overwrite == true and .folderUid == "ops" and .dashboard.id == null and .dashboard.uid == "microcms-content-ops"' \
    "dashboard payload did not match"
}

test_update_existing_datasource_with_explicit_query_url() {
  run_with_fakes "datasource-existing" env \
    GRAFANA_STACK_URL="https://example.grafana.net" \
    GRAFANA_STACK_SERVICE_ACCOUNT_TOKEN="secret-token" \
    QUERY_API_URL="https://query.example.com" \
    "$SCRIPT"

  assert_contains "$FAKE_CURL_LOG" "PUT /api/datasources/uid/duckdb-query-api"
  assert_json "$FAKE_CURL_TMP/datasource-update.json" \
    '.uid == "duckdb-query-api" and .url == "https://query.example.com" and .isDefault == false' \
    "datasource update payload did not match"
  assert_json "$FAKE_CURL_TMP/dashboard.json" \
    '.folderUid == "existing-folder"' \
    "dashboard payload did not preserve existing folder"
}

test_update_preserves_existing_default_datasource() {
  run_with_fakes "datasource-existing-default" env \
    GRAFANA_STACK_URL="https://example.grafana.net" \
    GRAFANA_STACK_SERVICE_ACCOUNT_TOKEN="secret-token" \
    QUERY_API_URL="https://query.example.com" \
    "$SCRIPT"

  assert_json "$FAKE_CURL_TMP/datasource-update.json" \
    '.uid == "duckdb-query-api" and .isDefault == true' \
    "datasource update payload did not preserve existing default flag"
}

test_plugin_missing_can_be_skipped() {
  local out="$TMP_DIR/plugin-missing.out"

  if run_with_fakes "plugin-missing" env \
    GRAFANA_STACK_URL="https://example.grafana.net" \
    GRAFANA_STACK_SERVICE_ACCOUNT_TOKEN="secret-token" \
    QUERY_API_URL="https://query.example.com" \
    "$SCRIPT" >"$out" 2>&1; then
    echo "expected plugin check failure" >&2
    exit 1
  fi
  assert_contains "$out" "Calendar Heatmap plugin is not installed"

  run_with_fakes "plugin-missing" env \
    GRAFANA_STACK_URL="https://example.grafana.net" \
    GRAFANA_STACK_SERVICE_ACCOUNT_TOKEN="secret-token" \
    QUERY_API_URL="https://query.example.com" \
    GRAFANA_SKIP_PLUGIN_CHECK=1 \
    "$SCRIPT"
  assert_contains "$FAKE_CURL_LOG" "POST /api/dashboards/db"
}

test_missing_required_env
test_create_datasource_and_dashboard
test_update_existing_datasource_with_explicit_query_url
test_update_preserves_existing_default_datasource
test_plugin_missing_can_be_skipped

echo "deploy-grafana-cloud tests passed"
