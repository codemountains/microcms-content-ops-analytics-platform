#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [[ $# -gt 1 ]]; then
  echo "usage: $0 [dashboard.json]" >&2
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
  echo "Grafana dashboard validation failed: dashboard file not found: $DASHBOARD" >&2
  exit 1
fi

fail() {
  echo "Grafana dashboard validation failed: $1" >&2
  exit 1
}

run_jq() {
  local name="$1"
  shift
  jq "$@" "$DASHBOARD" >/dev/null || fail "$name"
}

run_jq "dashboard JSON must be valid" empty

infinity_type="yesoreyeram-infinity-datasource"
old_json_type="marcusolsson-json-datasource"

if grep -q "$old_json_type" \
  "$ROOT/docker-compose.yml" \
  "$ROOT/docker-compose.local.yml" \
  "$ROOT/grafana/provisioning/datasources/datasource.yml" \
  "$DASHBOARD"; then
  fail "legacy JSON datasource plugin must not be referenced"
fi

for file in \
  "$ROOT/docker-compose.yml" \
  "$ROOT/docker-compose.local.yml" \
  "$ROOT/grafana/provisioning/datasources/datasource.yml" \
  "$DASHBOARD"; do
  if ! grep -q "$infinity_type" "$file"; then
    fail "Infinity datasource plugin must be referenced in ${file#$ROOT/}"
  fi
done

run_jq "dashboard timezone must be Asia/Tokyo" -e '.timezone == "Asia/Tokyo"'

run_jq "all panels must have non-empty descriptions" -e '
  all(
    .panels[];
    (.description | type == "string")
      and (.description | gsub("\\s"; "") | length > 0)
  )
'

run_jq "all panel queries use Infinity datasource model" -e --arg infinity_type "$infinity_type" '
  all(
    .panels[];
    .datasource.type == $infinity_type and .datasource.uid == "duckdb-query-api"
  )
  and all(
    .panels[].targets[];
    .datasource.type == $infinity_type
      and .datasource.uid == "duckdb-query-api"
      and .type == "json"
      and .source == "url"
      and .parser == "backend"
      and .format == "table"
      and .root_selector == "$"
      and .url_options.method == "GET"
      and (.data // "") == ""
      and (.filters // []) == []
      and (has("cacheDurationSeconds") | not)
      and (has("fields") | not)
      and (has("urlPath") | not)
      and (has("queryParams") | not)
  )
'

run_jq "Calendar Heatmap panel wiring" -e '
  .panels[]
  | select(.title == "Calendar Heatmap")
  | select(.type == "tim012432-calendarheatmap-panel")
  | select(.options.colorScheme == "green")
  | .targets[]
  | select(.url == "/metrics/calendar-heatmap")
  | select((.url_options.params // []) == [
      {"key":"from","value":"${__timeFrom}"},
      {"key":"to","value":"${__timeTo}"}
    ])
  | [.columns[] | {selector, text, type}] as $fields
  | all(
      [
        {"selector":"time","text":"time","type":"timestamp"},
        {"selector":"value","text":"value","type":"number"}
      ][];
      . as $required | $fields | index($required)
    )
'

run_jq "API Activity field mappings" -e '
  .panels[]
  | select(.title == "API Activity")
  | .targets[]
  | select(.url == "/metrics/api-activity")
  | select((.url_options.params // []) == [{"key":"days","value":"30"}])
  | [.columns[] | {selector, text, type}] as $fields
  | all(
      [
        {"selector":"api","text":"api","type":"string"},
        {"selector":"create_draft_count","text":"create_draft","type":"number"},
        {"selector":"create_publish_count","text":"create_publish","type":"number"},
        {"selector":"first_publish_count","text":"first_publish","type":"number"},
        {"selector":"update_publish_count","text":"update_publish","type":"number"},
        {"selector":"unpublish_count","text":"unpublish","type":"number"},
        {"selector":"delete_count","text":"delete","type":"number"}
      ][];
      . as $required | $fields | index($required)
    )
'

run_jq "API Activity series order transformation" -e '
  .panels[]
  | select(.title == "API Activity")
  | .transformations
  | index([{
      "id": "organize",
      "options": {
        "indexByName": {
          "api": 0,
          "create_draft": 1,
          "create_publish": 2,
          "first_publish": 3,
          "update_publish": 4,
          "unpublish": 5,
          "delete": 6
        }
      }
    }])
'

run_jq "Top Updated Contents count field mapping" -e '
  .panels[]
  | select(.title == "Top Updated Contents")
  | .targets[]
  | select(.url == "/metrics/top-updated-contents")
  | select((.url_options.params // []) == [
      {"key":"days","value":"30"},
      {"key":"limit","value":"20"}
    ])
  | .columns[]
  | select(.selector == "count" and .text == "updated_count" and .type == "number")
'

run_jq "Top Updated Contents last_event_at display override" -e '
  .panels[]
  | select(.title == "Top Updated Contents")
  | .fieldConfig.overrides[]
  | select(.matcher.options == "last_event_at")
  | .properties[]
  | select(.id == "unit" and .value == "dateTimeAsLocal")
'

run_jq "publish_duration_unit dashboard variable" -e '
  .templating.list[]
  | select(.name == "publish_duration_unit" and .type == "custom" and .current.value == "days")
  | .options as $options
  | any($options[]; .value == "days") and any($options[]; .value == "hours")
'

run_jq "Average Time to Publish by API panel wiring" -e '
  .panels[]
  | select(.title == "Average Time to Publish by API")
  | .targets[]
  | select(.url == "/metrics/average-time-to-publish-by-api")
  | select((.url_options.params // []) == [
      {"key":"days","value":"30"},
      {"key":"unit","value":"${publish_duration_unit}"}
    ])
  | .columns as $fields
  | any($fields[]; .selector == "api" and .text == "api" and .type == "string")
    and any($fields[]; .selector == "avg_days" and .text == "avg_days" and .type == "number")
    and any($fields[]; .selector == "avg_hours" and .text == "avg_hours" and .type == "number")
'

run_jq "Average Time to Publish by API duration field filter" -e '
  .panels[]
  | select(.title == "Average Time to Publish by API")
  | .transformations[]
  | select(
      .id == "filterFieldsByName"
        and .options.include.pattern == "^(api|avg_${publish_duration_unit})$"
    )
'

run_jq "Average Time to Publish by API duration thresholds" -e '
  .panels[]
  | select(.title == "Average Time to Publish by API")
  | .fieldConfig.overrides as $overrides
  | any(
      $overrides[];
      .matcher.options == "avg_days"
        and any(.properties[]; .id == "unit" and .value == "d")
        and any(
          .properties[];
          .id == "thresholds"
            and (.value.steps | any(.value == 1))
            and (.value.steps | any(.value == 3))
        )
    )
    and any(
      $overrides[];
      .matcher.options == "avg_hours"
        and any(.properties[]; .id == "unit" and .value == "h")
        and any(
          .properties[];
          .id == "thresholds"
            and (.value.steps | any(.value == 24))
            and (.value.steps | any(.value == 72))
        )
    )
'

run_jq "Average Draft to Publish by API panel wiring" -e '
  .panels[]
  | select(.title == "Average Draft to Publish by API")
  | .targets[]
  | select(.url == "/metrics/average-draft-to-publish-by-api")
  | select((.url_options.params // []) == [
      {"key":"days","value":"30"},
      {"key":"unit","value":"${publish_duration_unit}"}
    ])
  | .columns as $fields
  | any($fields[]; .selector == "api" and .text == "api" and .type == "string")
    and any($fields[]; .selector == "avg_days" and .text == "avg_days" and .type == "number")
    and any($fields[]; .selector == "avg_hours" and .text == "avg_hours" and .type == "number")
    and all($fields[]; .selector != "sample_count")
'

run_jq "Average Draft to Publish by API duration field filter" -e '
  .panels[]
  | select(.title == "Average Draft to Publish by API")
  | .transformations[]
  | select(
      .id == "filterFieldsByName"
        and .options.include.pattern == "^(api|avg_${publish_duration_unit})$"
    )
'

run_jq "Average Draft to Publish by API duration thresholds" -e '
  .panels[]
  | select(.title == "Average Draft to Publish by API")
  | .fieldConfig.overrides as $overrides
  | any(
      $overrides[];
      .matcher.options == "avg_days"
        and any(.properties[]; .id == "unit" and .value == "d")
        and any(
          .properties[];
          .id == "thresholds"
            and (.value.steps | any(.value == 1))
            and (.value.steps | any(.value == 3))
        )
    )
    and any(
      $overrides[];
      .matcher.options == "avg_hours"
        and any(.properties[]; .id == "unit" and .value == "h")
        and any(
          .properties[];
          .id == "thresholds"
            and (.value.steps | any(.value == 24))
            and (.value.steps | any(.value == 72))
        )
    )
'
