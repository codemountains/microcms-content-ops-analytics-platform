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

run_jq "primary panels are laid out in dashboard order" -e '
  (.panels[] | select(.title == "API Activity") | .type == "barchart" and .gridPos == {"h":12,"w":24,"x":0,"y":0})
    and (.panels[] | select(.title == "Publish Action Trend") | .type == "timeseries" and .gridPos == {"h":8,"w":24,"x":0,"y":12})
    and (.panels[] | select(.title == "Publish Count") | .type == "stat" and .gridPos == {"h":5,"w":12,"x":0,"y":20})
    and (.panels[] | select(.title == "Published State Rate") | .type == "gauge" and .gridPos == {"h":5,"w":12,"x":12,"y":20})
'

run_jq "Publish Count panel wiring" -e '
  .panels[]
  | select(.title == "Publish Count")
  | .targets[]
  | select(.url == "/metrics/publish-action-summary")
  | select((.url_options.params // []) == [
      {"key":"from","value":"${__timeFrom}"},
      {"key":"to","value":"${__timeTo}"}
    ])
  | .columns[]
  | select(.selector == "publish_count" and .text == "publish_count" and .type == "number")
'

run_jq "Published State Rate panel wiring" -e '
  .panels[]
  | select(.title == "Published State Rate")
  | select(.fieldConfig.defaults.unit == "percentunit")
  | .targets[]
  | select(.url == "/metrics/publish-action-summary")
  | select((.url_options.params // []) == [
      {"key":"from","value":"${__timeFrom}"},
      {"key":"to","value":"${__timeTo}"}
    ])
  | .columns[]
  | select(.selector == "published_state_rate" and .text == "published_state_rate" and .type == "number")
'

run_jq "Publish Action Trend panel wiring" -e '
  .panels[]
  | select(.title == "Publish Action Trend")
  | select(.fieldConfig.defaults.custom.drawStyle == "bars")
  | select(.fieldConfig.defaults.custom.stacking.mode == "normal")
  | .targets[]
  | select(.url == "/metrics/publish-action-trend")
  | select((.url_options.params // []) == [
      {"key":"from","value":"${__timeFrom}"},
      {"key":"to","value":"${__timeTo}"}
    ])
  | [.columns[] | {selector, text, type}] as $fields
  | all(
      [
        {"selector":"time","text":"time","type":"timestamp"},
        {"selector":"publish_from_draft_count","text":"publish_from_draft","type":"number"},
        {"selector":"initial_publish_count","text":"initial_publish","type":"number"},
        {"selector":"republish_from_closed_count","text":"republish_from_closed","type":"number"}
      ][];
      . as $required | $fields | index($required)
    )
'

run_jq "API Activity field mappings" -e '
  .panels[]
  | select(.title == "API Activity")
  | .targets[]
  | select(.url == "/metrics/api-activity")
  | select((.url_options.params // []) == [
      {"key":"from","value":"${__timeFrom}"},
      {"key":"to","value":"${__timeTo}"}
    ])
  | [.columns[] | {selector, text, type}] as $fields
  | all(
      [
        {"selector":"api","text":"api","type":"string"},
        {"selector":"initial_draft_count","text":"initial_draft","type":"number"},
        {"selector":"save_draft_count","text":"save_draft","type":"number"},
        {"selector":"publish_from_draft_count","text":"publish_from_draft","type":"number"},
        {"selector":"initial_publish_count","text":"initial_publish","type":"number"},
        {"selector":"update_published_count","text":"update_published","type":"number"},
        {"selector":"add_draft_to_published_count","text":"add_draft_to_published","type":"number"},
        {"selector":"discard_draft_on_published_count","text":"discard_draft_on_published","type":"number"},
        {"selector":"unpublish_to_draft_count","text":"unpublish_to_draft","type":"number"},
        {"selector":"unpublish_to_closed_count","text":"unpublish_to_closed","type":"number"},
        {"selector":"reopen_to_draft_count","text":"reopen_to_draft","type":"number"},
        {"selector":"republish_from_closed_count","text":"republish_from_closed","type":"number"},
        {"selector":"delete_draft_count","text":"delete_draft","type":"number"},
        {"selector":"delete_published_count","text":"delete_published","type":"number"},
        {"selector":"delete_closed_count","text":"delete_closed","type":"number"}
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
          "initial_draft": 1,
          "save_draft": 2,
          "publish_from_draft": 3,
          "initial_publish": 4,
          "update_published": 5,
          "add_draft_to_published": 6,
          "discard_draft_on_published": 7,
          "unpublish_to_draft": 8,
          "unpublish_to_closed": 9,
          "reopen_to_draft": 10,
          "republish_from_closed": 11,
          "delete_draft": 12,
          "delete_published": 13,
          "delete_closed": 14
        }
      }
    }])
'

run_jq "api_activity_view dashboard variable" -e '
  .templating.list[]
  | select(.name == "api_activity_view" and .type == "custom")
  | .options as $options
  | any(
      $options[];
      .text == "カテゴリ集約"
        and .value == "^(api|draft_activity|publish_activity|unpublish_activity|delete_activity)$"
    )
    and any(
      $options[];
      .text == "詳細"
        and .value == "^(api|initial_draft|save_draft|publish_from_draft|initial_publish|update_published|add_draft_to_published|discard_draft_on_published|unpublish_to_draft|unpublish_to_closed|reopen_to_draft|republish_from_closed|delete_draft|delete_published|delete_closed)$"
    )
'

run_jq "api_activity_view query renders custom options" -e '
  .templating.list[]
  | select(.name == "api_activity_view" and .type == "custom")
  | .query as $query
  | ($query | length > 0)
    and ($query | contains(" : "))
    and ($query | contains("^(api|draft_activity|publish_activity|unpublish_activity|delete_activity)$"))
    and ($query | contains("^(api|initial_draft|save_draft|publish_from_draft|initial_publish|update_published|add_draft_to_published|discard_draft_on_published|unpublish_to_draft|unpublish_to_closed|reopen_to_draft|republish_from_closed|delete_draft|delete_published|delete_closed)$"))
'

run_jq "API Activity category aggregation transformations" -e '
  .panels[]
  | select(.title == "API Activity")
  | .transformations
  | map(select(.id == "calculateField" and .options.mode == "reduceRow")) as $calcs
  | ($calcs | length) == 4
    and any($calcs[]; .options.alias == "draft_activity")
    and any($calcs[]; .options.alias == "publish_activity")
    and any($calcs[]; .options.alias == "unpublish_activity")
    and any($calcs[]; .options.alias == "delete_activity")
'

run_jq "API Activity field filter by view" -e '
  .panels[]
  | select(.title == "API Activity")
  | .transformations[]
  | select(
      .id == "filterFieldsByName"
        and .options.include.pattern == "${api_activity_view}"
    )
'

run_jq "API Activity horizontal bar chart options" -e '
  .panels[]
  | select(.title == "API Activity")
  | select(.type == "barchart")
  | select(.options.orientation == "horizontal")
  | select(.options.tooltip.mode == "multi")
  | select(.options.stacking == "normal")
'

run_jq "Operation Category Breakdown panel wiring" -e '
  .panels[]
  | select(.title == "Operation Category Breakdown")
  | select(.type == "piechart")
  | select(.gridPos == {"h":9,"w":12,"x":0,"y":25})
  | .targets[]
  | select(.url == "/metrics/api-activity")
  | select((.url_options.params // []) == [
      {"key":"from","value":"${__timeFrom}"},
      {"key":"to","value":"${__timeTo}"}
    ])
'

run_jq "Top Updated Contents panel position" -e '
  .panels[]
  | select(.title == "Top Updated Contents")
  | .gridPos == {"h":9,"w":12,"x":0,"y":34}
'

run_jq "Operation Category Breakdown category aggregation transformations" -e '
  .panels[]
  | select(.title == "Operation Category Breakdown")
  | .transformations
  | map(select(.id == "calculateField" and .options.mode == "reduceRow")) as $calcs
  | ($calcs | length) == 4
    and any($calcs[]; .options.alias == "draft_activity")
    and any($calcs[]; .options.alias == "publish_activity")
    and any($calcs[]; .options.alias == "unpublish_activity")
    and any($calcs[]; .options.alias == "delete_activity")
'

run_jq "Operation Category Breakdown reduces all APIs to totals" -e '
  .panels[]
  | select(.title == "Operation Category Breakdown")
  | .transformations as $transformations
  | any(
      $transformations[];
      .id == "filterFieldsByName"
        and .options.include.pattern == "^(draft_activity|publish_activity|unpublish_activity|delete_activity)$"
    )
    and any(
      $transformations[];
      .id == "reduce"
        and any(.options.reducers[]; . == "sum")
    )
'

run_jq "Top Updated Contents count field mapping" -e '
  .panels[]
  | select(.title == "Top Updated Contents")
  | .targets[]
  | select(.url == "/metrics/top-updated-contents")
  | select((.url_options.params // []) == [
      {"key":"from","value":"${__timeFrom}"},
      {"key":"to","value":"${__timeTo}"},
      {"key":"limit","value":"20"}
    ])
  | .columns[]
  | select(.selector == "count" and .text == "updated_count" and .type == "number")
'

run_jq "Top Updated Contents column order transformation" -e '
  .panels[]
  | select(.title == "Top Updated Contents")
  | .transformations
  | index([{
      "id": "organize",
      "options": {
        "indexByName": {
          "api": 0,
          "content_id": 1,
          "updated_count": 2,
          "last_event_at": 3
        }
      }
    }])
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
      {"key":"from","value":"${__timeFrom}"},
      {"key":"to","value":"${__timeTo}"},
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
      {"key":"from","value":"${__timeFrom}"},
      {"key":"to","value":"${__timeTo}"},
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
