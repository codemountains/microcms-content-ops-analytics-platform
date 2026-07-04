mod config;
mod error;
mod handler;
mod query;
mod storage;

pub use config::AppConfig;
pub use error::ApiError;
pub use query::{
    ApiActivityRow, AverageDraftToPublishRow, AverageTimeToPublishRow, CalendarHeatmapRow,
    PublishActionSummaryRow, PublishActionTrendRow, TopUpdatedContentRow,
};

pub fn try_app(config: AppConfig) -> Result<axum::Router, ApiError> {
    handler::app(handler::AppState::try_new(config)?)
}

#[deprecated(note = "use try_app(config) to handle DuckDB initialization errors explicitly")]
pub fn app(config: AppConfig) -> axum::Router {
    try_app(config).expect("failed to initialize duckdb-query-api application")
}

#[cfg(test)]
mod tests {
    use std::fs;

    use ::duckdb::Connection;
    use chrono::{Duration, NaiveDate, TimeZone, Utc};
    use tempfile::tempdir;

    use crate::handler::PublishDurationUnit;
    use crate::query::{
        format_partition_time, query_api_activity_rows, query_average_draft_to_publish_rows,
        query_average_time_to_publish_rows, query_calendar_heatmap_rows,
        query_publish_action_summary_rows, query_publish_action_trend_rows,
        query_top_updated_contents_rows,
    };
    use crate::storage::{DuckDbEngine, sql_string_literal};

    const SMOKE_BLOG_LIFECYCLE_CONTENT_ULID: &str = "01J1DVG0000000000000000001";
    const SMOKE_BLOG_DIRECT_CONTENT_ULID: &str = "01J1DVG0000000000000000002";

    #[tokio::test]
    async fn queries_refreshed_metrics_from_local_hive_partitioned_parquet() {
        let tempdir = tempdir().unwrap();
        let connection = Connection::open_in_memory().unwrap();
        let today: String = connection
            .query_row(
                "SELECT CAST(CAST(current_timestamp AS TIMESTAMP) + INTERVAL '9 HOURS' AS DATE)::VARCHAR",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let today = NaiveDate::parse_from_str(&today, "%Y-%m-%d").unwrap();
        let event_date = today - Duration::days(2);
        let zero_date = event_date - Duration::days(1);
        let created_date = event_date - Duration::days(2);
        let updated_before_date = event_date - Duration::days(1);
        let draft_created_date = event_date - Duration::days(5);
        let partition_dir = tempdir.path().join(format!(
            "microcms_events/service=example-service/api=blogs/dt={event_date}"
        ));
        let authors_partition_dir = tempdir.path().join(format!(
            "microcms_events/service=example-service/api=authors/dt={event_date}"
        ));
        fs::create_dir_all(&partition_dir).unwrap();
        fs::create_dir_all(&authors_partition_dir).unwrap();
        let parquet_path = partition_dir.join("events.parquet");
        let authors_parquet_path = authors_partition_dir.join("events.parquet");
        let parquet_path_sql = sql_string_literal(&parquet_path.to_string_lossy());
        let authors_parquet_path_sql = sql_string_literal(&authors_parquet_path.to_string_lossy());
        let extension_dir = tempdir.path().join("duckdb_extensions");
        let extension_dir_sql = sql_string_literal(&extension_dir.to_string_lossy());

        connection
            .execute_batch(&format!(
                r#"
                SET extension_directory = '{extension_dir_sql}';
                INSTALL parquet;
                LOAD parquet;
                COPY (
                  SELECT
                    TIMESTAMP '{event_date} 12:00:00' AS received_at,
                    'example-service' AS service,
                    'blogs' AS api,
                    '{SMOKE_BLOG_LIFECYCLE_CONTENT_ULID}' AS content_id,
                    'PUBLISH_FROM_DRAFT' AS event_kind,
                    'edit' AS event_type,
                    'DRAFT' AS old_status,
                    'PUBLISH' AS new_status,
                    TIMESTAMP '{updated_before_date} 12:00:00' AS old_updated_at,
                    TIMESTAMP '{event_date} 12:00:00' AS new_updated_at,
                    NULL::TIMESTAMP AS draft_created_at,
                    TIMESTAMP '{created_date} 12:00:00' AS content_created_at,
                    TIMESTAMP '{event_date} 12:00:00' AS content_published_at,
                    '{{}}' AS raw_payload
                  UNION ALL
                  SELECT
                    TIMESTAMP '{event_date} 11:00:00',
                    'example-service',
                    'blogs',
                    '{SMOKE_BLOG_LIFECYCLE_CONTENT_ULID}',
                    'INITIAL_DRAFT',
                    'new',
                    NULL,
                    'DRAFT',
                    NULL,
                    TIMESTAMP '{event_date} 11:00:00',
                    TIMESTAMP '{draft_created_date} 12:00:00',
                    NULL,
                    NULL,
                    '{{}}'
                  UNION ALL
                  SELECT
                    TIMESTAMP '{event_date} 13:00:00',
                    'example-service',
                    'blogs',
                    '{SMOKE_BLOG_LIFECYCLE_CONTENT_ULID}',
                    'UPDATE_PUBLISHED',
                    'edit',
                    'PUBLISH',
                    'PUBLISH',
                    TIMESTAMP '{event_date} 12:00:00',
                    TIMESTAMP '{event_date} 13:00:00',
                    NULL,
                    NULL,
                    NULL,
                    '{{}}'
                  UNION ALL
                  SELECT
                    TIMESTAMP '{event_date} 15:00:00',
                    'example-service',
                    'blogs',
                    NULL,
                    'UNPUBLISH_TO_DRAFT',
                    'edit',
                    'PUBLISH',
                    'DRAFT',
                    TIMESTAMP '{event_date} 14:00:00',
                    TIMESTAMP '{event_date} 15:00:00',
                    NULL,
                    NULL,
                    NULL,
                    '{{}}'
                  UNION ALL
                  SELECT
                    TIMESTAMP '{event_date} 15:30:00',
                    'example-service',
                    'blogs',
                    NULL,
                    'UNPUBLISH_TO_CLOSED',
                    'edit',
                    'PUBLISH',
                    'CLOSED',
                    TIMESTAMP '{event_date} 14:30:00',
                    TIMESTAMP '{event_date} 15:30:00',
                    NULL,
                    NULL,
                    NULL,
                    '{{}}'
                  UNION ALL
                  SELECT
                    TIMESTAMP '{event_date} 15:45:00',
                    'example-service',
                    'blogs',
                    NULL,
                    'ADD_DRAFT_TO_PUBLISHED',
                    'edit',
                    'PUBLISH',
                    'DRAFT,PUBLISH',
                    TIMESTAMP '{event_date} 14:45:00',
                    TIMESTAMP '{event_date} 15:45:00',
                    NULL,
                    NULL,
                    NULL,
                    '{{}}'
                  UNION ALL
                  SELECT
                    TIMESTAMP '{event_date} 16:00:00',
                    'example-service',
                    'blogs',
                    NULL,
                    'DISCARD_DRAFT_ON_PUBLISHED',
                    'edit',
                    'DRAFT,PUBLISH',
                    'PUBLISH',
                    TIMESTAMP '{event_date} 15:45:00',
                    TIMESTAMP '{event_date} 16:00:00',
                    NULL,
                    NULL,
                    NULL,
                    '{{}}'
                  UNION ALL
                  SELECT
                    TIMESTAMP '{event_date} 16:30:00',
                    'example-service',
                    'blogs',
                    NULL,
                    NULL,
                    'edit',
                    NULL,
                    NULL,
                    NULL,
                    TIMESTAMP '{event_date} 16:30:00',
                    NULL,
                    NULL,
                    NULL,
                    '{{}}'
                  UNION ALL
                  SELECT
                    TIMESTAMP '{event_date} 14:00:00',
                    'example-service',
                    'blogs',
                    '{SMOKE_BLOG_DIRECT_CONTENT_ULID}',
                    'INITIAL_PUBLISH',
                    'new',
                    NULL,
                    'PUBLISH',
                    NULL,
                    TIMESTAMP '{event_date} 14:00:00',
                    NULL,
                    TIMESTAMP '{event_date} 08:00:00',
                    TIMESTAMP '{event_date} 14:00:00',
                    '{{}}'
                ) TO '{parquet_path_sql}' (FORMAT PARQUET);

                COPY (
                  SELECT
                    TIMESTAMP '{event_date} 15:00:00' AS received_at,
                    'example-service' AS service,
                    'authors' AS api,
                    NULL AS content_id,
                    'DELETE_PUBLISHED' AS event_kind,
                    'delete' AS event_type,
                    'PUBLISH' AS old_status,
                    NULL AS new_status,
                    TIMESTAMP '{event_date} 15:00:00' AS old_updated_at,
                    NULL AS new_updated_at,
                    NULL AS draft_created_at,
                    NULL AS content_created_at,
                    NULL AS content_published_at,
                    '{{}}' AS raw_payload
                ) TO '{authors_parquet_path_sql}' (FORMAT PARQUET)
                "#
            ))
            .unwrap();

        let events_path = format!("{}/microcms_events/**/*.parquet", tempdir.path().display());
        let engine = DuckDbEngine::new(
            &events_path,
            "ap-northeast-1",
            &extension_dir.to_string_lossy(),
            None,
            "vhost",
            true,
        )
        .unwrap();

        let from_ms = Utc
            .from_utc_datetime(&(today - Duration::days(3)).and_hms_opt(0, 0, 0).unwrap())
            .timestamp_millis();
        let to_ms = Utc
            .from_utc_datetime(&today.and_hms_opt(23, 59, 59).unwrap())
            .timestamp_millis();

        let (
            calendar,
            activity,
            top_contents,
            publish_duration,
            draft_duration,
            publish_summary,
            publish_trend,
        ) = engine
            .query(move |connection, events_sql| {
                Ok((
                    query_calendar_heatmap_rows(connection, events_sql, from_ms, to_ms)?,
                    query_api_activity_rows(connection, events_sql, from_ms, to_ms)?,
                    query_top_updated_contents_rows(connection, events_sql, from_ms, to_ms, 20)?,
                    query_average_time_to_publish_rows(
                        connection,
                        events_sql,
                        from_ms,
                        to_ms,
                        PublishDurationUnit::Days,
                    )?,
                    query_average_draft_to_publish_rows(
                        connection,
                        events_sql,
                        from_ms,
                        to_ms,
                        PublishDurationUnit::Days,
                    )?,
                    query_publish_action_summary_rows(connection, events_sql, from_ms, to_ms)?,
                    query_publish_action_trend_rows(connection, events_sql, from_ms, to_ms)?,
                ))
            })
            .await
            .unwrap();

        assert!(calendar.iter().any(|row| {
            row.time == format_partition_time(&zero_date.to_string()) && row.value == 0
        }));
        assert!(calendar.iter().any(|row| {
            row.time == format_partition_time(&event_date.to_string()) && row.value == 10
        }));

        assert_eq!(activity.len(), 2);
        assert_eq!(activity[0].api.as_deref(), Some("blogs"));
        assert_eq!(activity[0].initial_draft_count, 1);
        assert_eq!(activity[0].save_draft_count, 0);
        assert_eq!(activity[0].publish_from_draft_count, 1);
        assert_eq!(activity[0].initial_publish_count, 1);
        assert_eq!(activity[0].update_published_count, 1);
        assert_eq!(activity[0].add_draft_to_published_count, 1);
        assert_eq!(activity[0].discard_draft_on_published_count, 1);
        assert_eq!(activity[0].unpublish_to_draft_count, 1);
        assert_eq!(activity[0].unpublish_to_closed_count, 1);
        assert_eq!(activity[0].reopen_to_draft_count, 0);
        assert_eq!(activity[0].republish_from_closed_count, 0);
        assert_eq!(activity[0].delete_draft_count, 0);
        assert_eq!(activity[0].delete_published_count, 0);
        assert_eq!(activity[0].delete_closed_count, 0);
        assert_eq!(activity[0].total_count, 9);
        assert_eq!(activity[1].api.as_deref(), Some("authors"));
        assert_eq!(activity[1].delete_published_count, 1);
        assert_eq!(activity[1].total_count, 1);

        assert_eq!(top_contents.len(), 2);
        assert_eq!(top_contents[0].api.as_deref(), Some("blogs"));
        assert_eq!(
            top_contents[0].content_id.as_deref(),
            Some(SMOKE_BLOG_LIFECYCLE_CONTENT_ULID)
        );
        assert_eq!(top_contents[0].count, 3);
        let expected_last_event_at = format!("{event_date}T13:00:00Z");
        assert_eq!(
            top_contents[0].last_event_at.as_deref(),
            Some(expected_last_event_at.as_str())
        );

        assert_eq!(publish_duration.len(), 1);
        assert_eq!(publish_duration[0].api.as_deref(), Some("blogs"));
        assert_eq!(publish_duration[0].avg_days, Some(1.125));
        assert_eq!(publish_duration[0].avg_hours, None);

        assert_eq!(draft_duration.len(), 1);
        assert_eq!(draft_duration[0].api.as_deref(), Some("blogs"));
        assert_eq!(draft_duration[0].avg_days, Some(5.0));
        assert_eq!(draft_duration[0].avg_hours, None);
        assert_eq!(draft_duration[0].sample_count, 1);

        assert_eq!(publish_summary.len(), 1);
        assert_eq!(publish_summary[0].publish_count, 2);
        assert_eq!(publish_summary[0].published_state_count, 3);
        assert_eq!(publish_summary[0].state_arrival_count, 6);
        assert_eq!(publish_summary[0].published_state_rate, Some(0.5));

        assert!(publish_trend.iter().any(|row| {
            row.time == format_partition_time(&event_date.to_string())
                && row.publish_from_draft_count == 1
                && row.initial_publish_count == 1
                && row.republish_from_closed_count == 0
                && row.publish_count == 2
        }));
    }

    #[tokio::test]
    async fn duckdb_engine_reuses_configured_connection_between_requests() {
        let tempdir = tempdir().unwrap();
        let engine = DuckDbEngine::new_with_pool_size(
            &format!("{}/missing/**/*.parquet", tempdir.path().display()),
            "ap-northeast-1",
            &tempdir.path().join("duckdb_extensions").to_string_lossy(),
            None,
            "vhost",
            true,
            1,
        )
        .unwrap();

        engine
            .query(|connection, _events_sql| {
                connection
                    .execute_batch("CREATE TEMP TABLE connection_marker AS SELECT 42 AS value")?;
                Ok(())
            })
            .await
            .unwrap();

        let value: i32 = engine
            .query(|connection, _events_sql| {
                connection.query_row("SELECT value FROM connection_marker", [], |row| row.get(0))
            })
            .await
            .unwrap();

        assert_eq!(value, 42);
    }

    #[tokio::test]
    async fn duckdb_engine_can_use_multiple_initialized_connections() {
        let tempdir = tempdir().unwrap();
        let engine = DuckDbEngine::new_with_pool_size(
            &format!("{}/missing/**/*.parquet", tempdir.path().display()),
            "ap-northeast-1",
            &tempdir.path().join("duckdb_extensions").to_string_lossy(),
            None,
            "vhost",
            true,
            2,
        )
        .unwrap();

        engine
            .query(|connection, _events_sql| {
                connection.execute_batch("CREATE TEMP TABLE pool_marker AS SELECT 1 AS value")?;
                Ok(())
            })
            .await
            .unwrap();
        engine
            .query(|connection, _events_sql| {
                connection.execute_batch("CREATE TEMP TABLE pool_marker AS SELECT 2 AS value")?;
                Ok(())
            })
            .await
            .unwrap();
    }
}
