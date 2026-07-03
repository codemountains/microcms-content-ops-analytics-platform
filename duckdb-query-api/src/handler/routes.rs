use axum::{
    Json, Router,
    extract::{Query, State},
    routing::get,
};
use serde::{Deserialize, Serialize};

use super::{
    AppState, validate_days, validate_limit, validate_publish_duration_unit, validate_time_range,
};
use crate::ApiError;
use crate::query::{
    ApiActivityRow, AverageDraftToPublishRow, AverageTimeToPublishRow, CalendarHeatmapRow,
    PublishActionSummaryRow, PublishActionTrendRow, TopUpdatedContentRow, query_api_activity_rows,
    query_average_draft_to_publish_rows, query_average_time_to_publish_rows,
    query_calendar_heatmap_rows, query_publish_action_summary_rows,
    query_publish_action_trend_rows, query_top_updated_contents_rows,
};

#[derive(Debug, Deserialize)]
struct DaysQuery {
    days: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct AverageTimeToPublishQuery {
    days: Option<u32>,
    unit: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TimeRangeQuery {
    from: Option<i64>,
    to: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct LimitedQuery {
    days: Option<u32>,
    limit: Option<u32>,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    ok: bool,
}

pub(crate) fn app(state: AppState) -> Result<Router, ApiError> {
    Ok(Router::new()
        .route("/health", get(health))
        .route("/metrics/calendar-heatmap", get(calendar_heatmap))
        .route(
            "/metrics/publish-action-summary",
            get(publish_action_summary),
        )
        .route("/metrics/publish-action-trend", get(publish_action_trend))
        .route("/metrics/api-activity", get(api_activity))
        .route("/metrics/top-updated-contents", get(top_updated_contents))
        .route(
            "/metrics/average-time-to-publish-by-api",
            get(average_time_to_publish_by_api),
        )
        .route(
            "/metrics/average-draft-to-publish-by-api",
            get(average_draft_to_publish_by_api),
        )
        .with_state(state))
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { ok: true })
}

async fn calendar_heatmap(
    State(state): State<AppState>,
    Query(query): Query<TimeRangeQuery>,
) -> Result<Json<Vec<CalendarHeatmapRow>>, ApiError> {
    let (from_ms, to_ms) = validate_time_range(query.from, query.to)?;
    state
        .duckdb
        .query(move |connection, events_sql| {
            query_calendar_heatmap_rows(connection, events_sql, from_ms, to_ms)
        })
        .await
        .map(Json)
}

async fn api_activity(
    State(state): State<AppState>,
    Query(query): Query<DaysQuery>,
) -> Result<Json<Vec<ApiActivityRow>>, ApiError> {
    let days = validate_days(query.days)?;
    state
        .duckdb
        .query(move |connection, events_sql| query_api_activity_rows(connection, events_sql, days))
        .await
        .map(Json)
}

async fn publish_action_summary(
    State(state): State<AppState>,
    Query(query): Query<DaysQuery>,
) -> Result<Json<Vec<PublishActionSummaryRow>>, ApiError> {
    let days = validate_days(query.days)?;
    state
        .duckdb
        .query(move |connection, events_sql| {
            query_publish_action_summary_rows(connection, events_sql, days)
        })
        .await
        .map(Json)
}

async fn publish_action_trend(
    State(state): State<AppState>,
    Query(query): Query<TimeRangeQuery>,
) -> Result<Json<Vec<PublishActionTrendRow>>, ApiError> {
    let (from_ms, to_ms) = validate_time_range(query.from, query.to)?;
    state
        .duckdb
        .query(move |connection, events_sql| {
            query_publish_action_trend_rows(connection, events_sql, from_ms, to_ms)
        })
        .await
        .map(Json)
}

async fn top_updated_contents(
    State(state): State<AppState>,
    Query(query): Query<LimitedQuery>,
) -> Result<Json<Vec<TopUpdatedContentRow>>, ApiError> {
    let days = validate_days(query.days)?;
    let limit = validate_limit(query.limit)?;
    state
        .duckdb
        .query(move |connection, events_sql| {
            query_top_updated_contents_rows(connection, events_sql, days, limit)
        })
        .await
        .map(Json)
}

async fn average_time_to_publish_by_api(
    State(state): State<AppState>,
    Query(query): Query<AverageTimeToPublishQuery>,
) -> Result<Json<Vec<AverageTimeToPublishRow>>, ApiError> {
    let days = validate_days(query.days)?;
    let unit = validate_publish_duration_unit(query.unit.as_deref())?;
    state
        .duckdb
        .query(move |connection, events_sql| {
            query_average_time_to_publish_rows(connection, events_sql, days, unit)
        })
        .await
        .map(Json)
}

async fn average_draft_to_publish_by_api(
    State(state): State<AppState>,
    Query(query): Query<AverageTimeToPublishQuery>,
) -> Result<Json<Vec<AverageDraftToPublishRow>>, ApiError> {
    let days = validate_days(query.days)?;
    let unit = validate_publish_duration_unit(query.unit.as_deref())?;
    state
        .duckdb
        .query(move |connection, events_sql| {
            query_average_draft_to_publish_rows(connection, events_sql, days, unit)
        })
        .await
        .map(Json)
}
