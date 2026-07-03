mod api_activity;
mod average_draft_to_publish;
mod average_time_to_publish;
mod calendar_heatmap;
mod event_kind;
mod publish_actions;
mod top_updated_contents;

use serde::Serialize;

pub(crate) use api_activity::query_api_activity_rows;
pub(crate) use average_draft_to_publish::query_average_draft_to_publish_rows;
pub(crate) use average_time_to_publish::query_average_time_to_publish_rows;
pub(crate) use calendar_heatmap::query_calendar_heatmap_rows;
pub(crate) use publish_actions::{
    query_publish_action_summary_rows, query_publish_action_trend_rows,
};
pub(crate) use top_updated_contents::query_top_updated_contents_rows;

pub(crate) const JST_OFFSET_INTERVAL: &str = "9 HOURS";
pub(crate) const PARTITION_TIME_JST_SUFFIX: &str = "T00:00:00+09:00";

#[cfg(test)]
pub(crate) fn format_partition_time(dt: &str) -> String {
    format!("{dt}{PARTITION_TIME_JST_SUFFIX}")
}

#[derive(Debug, Serialize)]
pub struct CalendarHeatmapRow {
    pub time: String,
    pub value: i64,
}

#[derive(Debug, Serialize)]
pub struct ApiActivityRow {
    pub api: Option<String>,
    pub initial_draft_count: i64,
    pub save_draft_count: i64,
    pub publish_from_draft_count: i64,
    pub initial_publish_count: i64,
    pub update_published_count: i64,
    pub add_draft_to_published_count: i64,
    pub discard_draft_on_published_count: i64,
    pub unpublish_to_draft_count: i64,
    pub unpublish_to_closed_count: i64,
    pub reopen_to_draft_count: i64,
    pub republish_from_closed_count: i64,
    pub delete_draft_count: i64,
    pub delete_published_count: i64,
    pub delete_closed_count: i64,
    pub total_count: i64,
}

#[derive(Debug, Serialize)]
pub struct TopUpdatedContentRow {
    pub api: Option<String>,
    pub content_id: Option<String>,
    pub count: i64,
    pub last_event_at: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct AverageTimeToPublishRow {
    pub api: Option<String>,
    pub avg_days: Option<f64>,
    pub avg_hours: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct AverageDraftToPublishRow {
    pub api: Option<String>,
    pub avg_days: Option<f64>,
    pub avg_hours: Option<f64>,
    pub sample_count: i64,
}

#[derive(Debug, Serialize)]
pub struct PublishActionSummaryRow {
    pub publish_count: i64,
    pub total_count: i64,
    pub publish_rate: Option<f64>,
}

#[derive(Debug, Serialize)]
pub struct PublishActionTrendRow {
    pub time: String,
    pub publish_from_draft_count: i64,
    pub initial_publish_count: i64,
    pub republish_from_closed_count: i64,
    pub publish_count: i64,
}

pub(crate) fn collect_rows<T>(
    rows: duckdb::MappedRows<'_, impl FnMut(&duckdb::Row<'_>) -> duckdb::Result<T>>,
) -> duckdb::Result<Vec<T>> {
    rows.collect()
}
