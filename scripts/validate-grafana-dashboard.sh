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

run_jq "dashboard timezone must be Asia/Tokyo" -e '.timezone == "Asia/Tokyo"'

run_jq "all panel queries disable JSON datasource cache" -e '
  [.panels[].targets[] | select(.cacheDurationSeconds != 0)] | length == 0
'

run_jq "Calendar Heatmap panel wiring" -e '
  .panels[]
  | select(.title == "Calendar Heatmap")
  | select(.type == "tim012432-calendarheatmap-panel")
  | select(.options.colorScheme == "green")
  | .targets[]
  | select(.urlPath == "/metrics/calendar-heatmap" and .queryParams == "from=${__from}&to=${__to}")
  | [.fields[] | {jsonPath, name, type}] as $fields
  | all(
      [
        {"jsonPath":"$[*].time","name":"time","type":"time"},
        {"jsonPath":"$[*].value","name":"value","type":"number"}
      ][];
      . as $required | $fields | index($required)
    )
'

run_jq "API Activity field mappings" -e '
  [.panels[] | select(.title == "API Activity") | .targets[].fields[] | {jsonPath, name, type}] as $fields
  | all(
      [
        {"jsonPath":"$[*].create_draft_count","name":"create_draft","type":"number"},
        {"jsonPath":"$[*].create_publish_count","name":"create_publish","type":"number"},
        {"jsonPath":"$[*].first_publish_count","name":"first_publish","type":"number"},
        {"jsonPath":"$[*].update_publish_count","name":"update_publish","type":"number"},
        {"jsonPath":"$[*].unpublish_count","name":"unpublish","type":"number"},
        {"jsonPath":"$[*].delete_count","name":"delete","type":"number"}
      ][];
      . as $required | $fields | index($required)
    )
'

run_jq "Top Updated Contents count field mapping" -e '
  .panels[]
  | select(.title == "Top Updated Contents")
  | .targets[].fields[]
  | select(.jsonPath == "$[*].count" and .name == "updated_count" and .type == "number")
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
  | select(.urlPath == "/metrics/average-time-to-publish-by-api")
  | select(.cacheDurationSeconds == 0)
  | select(
      (.params // []) as $params
      | any($params[]; .[0] == "days" and .[1] == "30")
        and any($params[]; .[0] == "unit" and .[1] == "${publish_duration_unit}")
    )
  | .fields as $fields
  | any($fields[]; .jsonPath == "$[*].avg_days" and .name == "avg_days" and .type == "number")
    and any($fields[]; .jsonPath == "$[*].avg_hours" and .name == "avg_hours" and .type == "number")
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
  | select(.urlPath == "/metrics/average-draft-to-publish-by-api")
  | select(.cacheDurationSeconds == 0)
  | select(
      (.params // []) as $params
      | any($params[]; .[0] == "days" and .[1] == "30")
        and any($params[]; .[0] == "unit" and .[1] == "${publish_duration_unit}")
    )
  | .fields as $fields
  | any($fields[]; .jsonPath == "$[*].avg_days" and .name == "avg_days" and .type == "number")
    and any($fields[]; .jsonPath == "$[*].avg_hours" and .name == "avg_hours" and .type == "number")
    and all($fields[]; .jsonPath != "$[*].sample_count")
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
