use std::env;
use std::fs;
use std::sync::Arc;

use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use duckdb::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub events_path: String,
    pub aws_region: String,
    pub duckdb_extension_directory: String,
    pub duckdb_s3_endpoint: Option<String>,
    pub duckdb_s3_url_style: String,
    pub duckdb_s3_use_ssl: bool,
    pub port: u16,
}

#[derive(Debug, Clone)]
struct AppState {
    events_path: Arc<str>,
    aws_region: Arc<str>,
    duckdb_extension_directory: Arc<str>,
    duckdb_s3_endpoint: Option<Arc<str>>,
    duckdb_s3_url_style: Arc<str>,
    duckdb_s3_use_ssl: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("missing required environment variable: {0}")]
    MissingEnv(&'static str),
    #[error("invalid query parameter: {0}")]
    InvalidQuery(&'static str),
    #[error("duckdb query failed: {0}")]
    DuckDb(String),
}

#[derive(Debug, Deserialize)]
struct DaysQuery {
    days: Option<u32>,
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

#[derive(Debug, Serialize)]
pub struct CalendarHeatmapRow {
    pub time: String,
    pub value: i64,
}

#[derive(Debug, Serialize)]
pub struct ApiActivityRow {
    pub api: Option<String>,
    pub new_count: i64,
    pub edit_count: i64,
    pub delete_count: i64,
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
    pub avg_days: f64,
}

impl AppConfig {
    pub fn from_env() -> Result<Self, ApiError> {
        let events_path = required_env("EVENTS_PATH")?;
        let aws_region = env::var("AWS_REGION").unwrap_or_else(|_| "ap-northeast-1".to_owned());
        let duckdb_extension_directory = env::var("DUCKDB_EXTENSION_DIRECTORY")
            .unwrap_or_else(|_| "/tmp/duckdb_extensions".to_owned());
        let duckdb_s3_endpoint = optional_env("DUCKDB_S3_ENDPOINT");
        let duckdb_s3_url_style =
            env::var("DUCKDB_S3_URL_STYLE").unwrap_or_else(|_| "vhost".to_owned());
        let duckdb_s3_use_ssl = env::var("DUCKDB_S3_USE_SSL")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(true);
        let port = env::var("PORT")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(8000);

        Ok(Self {
            events_path,
            aws_region,
            duckdb_extension_directory,
            duckdb_s3_endpoint,
            duckdb_s3_url_style,
            duckdb_s3_use_ssl,
            port,
        })
    }
}

fn required_env(key: &'static str) -> Result<String, ApiError> {
    let value = env::var(key).map_err(|_| ApiError::MissingEnv(key))?;
    if value.trim().is_empty() {
        return Err(ApiError::MissingEnv(key));
    }

    Ok(value)
}

fn optional_env(key: &'static str) -> Option<String> {
    env::var(key)
        .ok()
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
}

pub fn app(config: AppConfig) -> Router {
    let state = AppState {
        events_path: Arc::from(config.events_path),
        aws_region: Arc::from(config.aws_region),
        duckdb_extension_directory: Arc::from(config.duckdb_extension_directory),
        duckdb_s3_endpoint: config.duckdb_s3_endpoint.map(Arc::from),
        duckdb_s3_url_style: Arc::from(config.duckdb_s3_url_style),
        duckdb_s3_use_ssl: config.duckdb_s3_use_ssl,
    };

    Router::new()
        .route("/health", get(health))
        .route("/metrics/calendar-heatmap", get(calendar_heatmap))
        .route("/metrics/api-activity", get(api_activity))
        .route("/metrics/top-updated-contents", get(top_updated_contents))
        .route(
            "/metrics/average-time-to-publish-by-api",
            get(average_time_to_publish_by_api),
        )
        .with_state(state)
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { ok: true })
}

async fn calendar_heatmap(
    State(state): State<AppState>,
    Query(query): Query<TimeRangeQuery>,
) -> Result<Json<Vec<CalendarHeatmapRow>>, ApiError> {
    let (from_ms, to_ms) = validate_time_range(query.from, query.to)?;
    query_duckdb(state, move |connection, events_sql| {
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
    query_duckdb(state, move |connection, events_sql| {
        query_api_activity_rows(connection, events_sql, days)
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
    query_duckdb(state, move |connection, events_sql| {
        query_top_updated_contents_rows(connection, events_sql, days, limit)
    })
    .await
    .map(Json)
}

async fn average_time_to_publish_by_api(
    State(state): State<AppState>,
    Query(query): Query<DaysQuery>,
) -> Result<Json<Vec<AverageTimeToPublishRow>>, ApiError> {
    let days = validate_days(query.days)?;
    query_duckdb(state, move |connection, events_sql| {
        query_average_time_to_publish_rows(connection, events_sql, days)
    })
    .await
    .map(Json)
}

fn query_calendar_heatmap_rows(
    connection: &Connection,
    events_sql: &str,
    from_ms: i64,
    to_ms: i64,
) -> duckdb::Result<Vec<CalendarHeatmapRow>> {
    let sql = format!(
        r#"
        WITH bounds AS (
          SELECT
            CAST(epoch_ms({from_ms}) AS DATE) AS start_date,
            CAST(epoch_ms({to_ms}) AS DATE) AS end_date
        ),
        calendar AS (
          SELECT CAST(day AS DATE) AS dt
          FROM generate_series(
            (SELECT start_date FROM bounds),
            (SELECT end_date FROM bounds),
            INTERVAL 1 DAY
          ) AS series(day)
        ),
        daily AS (
          SELECT
            dt,
            COUNT(*) AS event_count
          FROM {events_sql}
          WHERE
            dt >= (SELECT start_date FROM bounds)
            AND dt <= (SELECT end_date FROM bounds)
          GROUP BY dt
        )
        SELECT
          CONCAT(CAST(calendar.dt AS VARCHAR), 'T00:00:00Z') AS time,
          COALESCE(daily.event_count, 0) AS value
        FROM calendar
        LEFT JOIN daily ON daily.dt = calendar.dt
        ORDER BY calendar.dt
        "#
    );

    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([], |row| {
        Ok(CalendarHeatmapRow {
            time: row.get(0)?,
            value: row.get(1)?,
        })
    })?;
    collect_rows(rows)
}

fn query_api_activity_rows(
    connection: &Connection,
    events_sql: &str,
    days: u32,
) -> duckdb::Result<Vec<ApiActivityRow>> {
    let days_minus_one = days - 1;
    let sql = format!(
        r#"
        SELECT
          api,
          SUM(CASE WHEN event_type = 'new' THEN 1 ELSE 0 END) AS new_count,
          SUM(CASE WHEN event_type = 'edit' THEN 1 ELSE 0 END) AS edit_count,
          SUM(CASE WHEN event_type = 'delete' THEN 1 ELSE 0 END) AS delete_count,
          COUNT(*) AS total_count
        FROM {events_sql}
        WHERE dt >= CAST(CAST(current_timestamp AS TIMESTAMP) AS DATE) - INTERVAL '{days_minus_one} DAY'
        GROUP BY api
        ORDER BY total_count DESC, api
        "#
    );

    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([], |row| {
        Ok(ApiActivityRow {
            api: row.get(0)?,
            new_count: row.get(1)?,
            edit_count: row.get(2)?,
            delete_count: row.get(3)?,
            total_count: row.get(4)?,
        })
    })?;
    collect_rows(rows)
}

fn query_top_updated_contents_rows(
    connection: &Connection,
    events_sql: &str,
    days: u32,
    limit: u32,
) -> duckdb::Result<Vec<TopUpdatedContentRow>> {
    let days_minus_one = days - 1;
    let sql = format!(
        r#"
        SELECT
          api,
          content_id,
          COUNT(*) AS count,
          CAST(MAX(received_at) AS VARCHAR) AS last_event_at
        FROM {events_sql}
        WHERE
          dt >= CAST(CAST(current_timestamp AS TIMESTAMP) AS DATE) - INTERVAL '{days_minus_one} DAY'
          AND event_type IN ('new', 'edit')
          AND content_id IS NOT NULL
        GROUP BY api, content_id
        ORDER BY count DESC, last_event_at DESC
        LIMIT {limit}
        "#
    );

    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([], |row| {
        Ok(TopUpdatedContentRow {
            api: row.get(0)?,
            content_id: row.get(1)?,
            count: row.get(2)?,
            last_event_at: row.get(3)?,
        })
    })?;
    collect_rows(rows)
}

fn query_average_time_to_publish_rows(
    connection: &Connection,
    events_sql: &str,
    days: u32,
) -> duckdb::Result<Vec<AverageTimeToPublishRow>> {
    let days_minus_one = days - 1;
    let sql = format!(
        r#"
        SELECT
          api,
          AVG(date_diff('second', content_created_at, content_published_at) / 86400.0) AS avg_days
        FROM {events_sql}
        WHERE
          dt >= CAST(CAST(current_timestamp AS TIMESTAMP) AS DATE) - INTERVAL '{days_minus_one} DAY'
          AND event_kind IN ('CREATE_PUBLISH', 'FIRST_PUBLISH')
          AND content_created_at IS NOT NULL
          AND content_published_at IS NOT NULL
          AND content_published_at >= content_created_at
        GROUP BY api
        ORDER BY avg_days DESC, api
        "#
    );

    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([], |row| {
        Ok(AverageTimeToPublishRow {
            api: row.get(0)?,
            avg_days: row.get(1)?,
        })
    })?;
    collect_rows(rows)
}

async fn query_duckdb<T, F>(state: AppState, query: F) -> Result<T, ApiError>
where
    T: Send + 'static,
    F: FnOnce(&Connection, &str) -> duckdb::Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let connection = Connection::open_in_memory()?;
        configure_connection(
            &connection,
            &state.aws_region,
            &state.events_path,
            &state.duckdb_extension_directory,
            state.duckdb_s3_endpoint.as_deref(),
            &state.duckdb_s3_url_style,
            state.duckdb_s3_use_ssl,
        )?;
        let events_sql = read_parquet_sql(&state.events_path);
        query(&connection, &events_sql)
    })
    .await
    .map_err(|error| ApiError::DuckDb(error.to_string()))?
    .map_err(|error| ApiError::DuckDb(error.to_string()))
}

fn configure_connection(
    connection: &Connection,
    aws_region: &str,
    events_path: &str,
    extension_directory: &str,
    s3_endpoint: Option<&str>,
    s3_url_style: &str,
    s3_use_ssl: bool,
) -> duckdb::Result<()> {
    let _ = fs::create_dir_all(extension_directory);
    connection.execute_batch(&format!(
        "SET extension_directory = '{}';",
        sql_string_literal(extension_directory)
    ))?;

    if events_path.starts_with("s3://") {
        connection.execute_batch(
            r#"
            INSTALL httpfs;
            LOAD httpfs;
            "#,
        )?;
        connection.execute("SET s3_region = ?1", params![aws_region])?;
        connection.execute("SET s3_url_style = ?1", params![s3_url_style])?;
        connection.execute("SET s3_use_ssl = ?1", params![s3_use_ssl])?;
        if let Some(endpoint) = s3_endpoint {
            connection.execute(
                "SET s3_endpoint = ?1",
                params![normalize_duckdb_s3_endpoint(endpoint)],
            )?;
        }

        let endpoint_clause = s3_endpoint
            .map(|endpoint| {
                format!(
                    ",\n              ENDPOINT '{}'",
                    sql_string_literal(&normalize_duckdb_s3_endpoint(endpoint))
                )
            })
            .unwrap_or_default();
        connection.execute_batch(&format!(
            r#"
            CREATE OR REPLACE SECRET microcms_events_s3 (
              TYPE S3,
              PROVIDER CREDENTIAL_CHAIN,
              REGION '{}',
              URL_STYLE '{}',
              USE_SSL {}{}
            );
            "#,
            sql_string_literal(aws_region),
            sql_string_literal(s3_url_style),
            s3_use_ssl,
            endpoint_clause
        ))?;
    }

    Ok(())
}

fn normalize_duckdb_s3_endpoint(endpoint: &str) -> String {
    endpoint
        .trim()
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_end_matches('/')
        .to_owned()
}

fn read_parquet_sql(events_path: &str) -> String {
    format!(
        "read_parquet('{}', hive_partitioning = true, union_by_name = true)",
        sql_string_literal(events_path)
    )
}

fn sql_string_literal(value: &str) -> String {
    value.replace('\'', "''")
}

fn validate_days(days: Option<u32>) -> Result<u32, ApiError> {
    validate_days_with_default(days, 30)
}

fn validate_days_with_default(days: Option<u32>, default: u32) -> Result<u32, ApiError> {
    let days = days.unwrap_or(default);
    if (1..=3660).contains(&days) {
        Ok(days)
    } else {
        Err(ApiError::InvalidQuery("days must be between 1 and 3660"))
    }
}

const MAX_CALENDAR_RANGE_MS: i64 = 3660 * 24 * 60 * 60 * 1000;
const DEFAULT_CALENDAR_RANGE_MS: i64 = 365 * 24 * 60 * 60 * 1000;

fn validate_time_range(from: Option<i64>, to: Option<i64>) -> Result<(i64, i64), ApiError> {
    match (from, to) {
        (Some(from_ms), Some(to_ms)) => {
            if from_ms > to_ms {
                return Err(ApiError::InvalidQuery(
                    "from must be less than or equal to to",
                ));
            }
            if to_ms - from_ms > MAX_CALENDAR_RANGE_MS {
                return Err(ApiError::InvalidQuery(
                    "time range must not exceed 3660 days",
                ));
            }
            Ok((from_ms, to_ms))
        }
        (None, None) => {
            let to_ms = chrono::Utc::now().timestamp_millis();
            Ok((to_ms - DEFAULT_CALENDAR_RANGE_MS, to_ms))
        }
        _ => Err(ApiError::InvalidQuery(
            "from and to must both be provided",
        )),
    }
}

fn validate_limit(limit: Option<u32>) -> Result<u32, ApiError> {
    let limit = limit.unwrap_or(20);
    if (1..=1000).contains(&limit) {
        Ok(limit)
    } else {
        Err(ApiError::InvalidQuery("limit must be between 1 and 1000"))
    }
}

fn collect_rows<T>(
    rows: duckdb::MappedRows<'_, impl FnMut(&duckdb::Row<'_>) -> duckdb::Result<T>>,
) -> duckdb::Result<Vec<T>> {
    rows.collect()
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match self {
            ApiError::InvalidQuery(_) => StatusCode::BAD_REQUEST,
            ApiError::MissingEnv(_) | ApiError::DuckDb(_) => StatusCode::INTERNAL_SERVER_ERROR,
        };

        (
            status,
            Json(json!({
                "ok": false,
                "message": self.to_string(),
            })),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, NaiveDate, TimeZone, Utc};
    use tempfile::tempdir;

    #[test]
    fn validates_time_range() {
        let from_ms = 1_000_i64;
        let to_ms = 2_000_i64;
        assert_eq!(
            validate_time_range(Some(from_ms), Some(to_ms)).unwrap(),
            (from_ms, to_ms)
        );
        assert!(validate_time_range(Some(to_ms), Some(from_ms)).is_err());
        assert!(validate_time_range(Some(from_ms), None).is_err());
        assert!(validate_time_range(None, Some(to_ms)).is_err());

        let (default_from, default_to) = validate_time_range(None, None).unwrap();
        assert!(default_to > default_from);
        assert_eq!(default_to - default_from, DEFAULT_CALENDAR_RANGE_MS);
    }

    #[test]
    fn validates_days() {
        assert_eq!(validate_days(None).unwrap(), 30);
        assert_eq!(validate_days_with_default(None, 365).unwrap(), 365);
        assert_eq!(validate_days(Some(1)).unwrap(), 1);
        assert_eq!(validate_days(Some(3660)).unwrap(), 3660);
        assert!(validate_days(Some(0)).is_err());
        assert!(validate_days(Some(3661)).is_err());
    }

    #[test]
    fn validates_limit() {
        assert_eq!(validate_limit(None).unwrap(), 20);
        assert_eq!(validate_limit(Some(1)).unwrap(), 1);
        assert_eq!(validate_limit(Some(1000)).unwrap(), 1000);
        assert!(validate_limit(Some(0)).is_err());
        assert!(validate_limit(Some(1001)).is_err());
    }

    #[test]
    fn escapes_events_path_for_read_parquet_sql() {
        assert_eq!(
            read_parquet_sql("s3://bucket/path/**/*.parquet"),
            "read_parquet('s3://bucket/path/**/*.parquet', hive_partitioning = true, union_by_name = true)"
        );
        assert_eq!(
            read_parquet_sql("s3://bucket/it's/**/*.parquet"),
            "read_parquet('s3://bucket/it''s/**/*.parquet', hive_partitioning = true, union_by_name = true)"
        );
    }

    #[test]
    fn normalizes_duckdb_s3_endpoint() {
        assert_eq!(
            normalize_duckdb_s3_endpoint("http://floci:4566/"),
            "floci:4566"
        );
        assert_eq!(
            normalize_duckdb_s3_endpoint("https://localhost:4566"),
            "localhost:4566"
        );
    }

    #[tokio::test]
    async fn queries_refreshed_metrics_from_local_hive_partitioned_parquet() {
        let tempdir = tempdir().unwrap();
        let connection = Connection::open_in_memory().unwrap();
        let today: String = connection
            .query_row(
                "SELECT CAST(CAST(current_timestamp AS TIMESTAMP) AS DATE)::VARCHAR",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let today = NaiveDate::parse_from_str(&today, "%Y-%m-%d").unwrap();
        let event_date = today - Duration::days(2);
        let zero_date = event_date - Duration::days(1);
        let created_date = event_date - Duration::days(2);
        let updated_before_date = event_date - Duration::days(1);
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
                    'content-1' AS content_id,
                    'FIRST_PUBLISH' AS event_kind,
                    'edit' AS event_type,
                    'DRAFT' AS old_status,
                    'PUBLISH' AS new_status,
                    TIMESTAMP '{updated_before_date} 12:00:00' AS old_updated_at,
                    TIMESTAMP '{event_date} 12:00:00' AS new_updated_at,
                    TIMESTAMP '{created_date} 12:00:00' AS content_created_at,
                    TIMESTAMP '{event_date} 12:00:00' AS content_published_at,
                    '{{}}' AS raw_payload
                  UNION ALL
                  SELECT
                    TIMESTAMP '{event_date} 13:00:00',
                    'example-service',
                    'blogs',
                    'content-1',
                    'UPDATE_PUBLISH',
                    'edit',
                    'PUBLISH',
                    'PUBLISH',
                    TIMESTAMP '{event_date} 12:00:00',
                    TIMESTAMP '{event_date} 13:00:00',
                    NULL,
                    NULL,
                    '{{}}'
                  UNION ALL
                  SELECT
                    TIMESTAMP '{event_date} 14:00:00',
                    'example-service',
                    'blogs',
                    'content-2',
                    'CREATE_PUBLISH',
                    'new',
                    NULL,
                    'PUBLISH',
                    NULL,
                    TIMESTAMP '{event_date} 14:00:00',
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
                    'DELETE' AS event_kind,
                    'delete' AS event_type,
                    'PUBLISH' AS old_status,
                    NULL AS new_status,
                    TIMESTAMP '{event_date} 15:00:00' AS old_updated_at,
                    NULL AS new_updated_at,
                    NULL AS content_created_at,
                    NULL AS content_published_at,
                    '{{}}' AS raw_payload
                ) TO '{authors_parquet_path_sql}' (FORMAT PARQUET)
                "#
            ))
            .unwrap();

        let events_path = format!("{}/microcms_events/**/*.parquet", tempdir.path().display());
        let state = AppState {
            events_path: Arc::from(events_path),
            aws_region: Arc::from("ap-northeast-1"),
            duckdb_extension_directory: Arc::from(extension_dir.to_string_lossy().to_string()),
            duckdb_s3_endpoint: None,
            duckdb_s3_url_style: Arc::from("vhost"),
            duckdb_s3_use_ssl: true,
        };

        let from_ms = Utc
            .from_utc_datetime(
                &(today - Duration::days(3))
                    .and_hms_opt(0, 0, 0)
                    .unwrap(),
            )
            .timestamp_millis();
        let to_ms = Utc
            .from_utc_datetime(&today.and_hms_opt(23, 59, 59).unwrap())
            .timestamp_millis();

        let (calendar, activity, top_contents, publish_duration) =
            query_duckdb(state, move |connection, events_sql| {
                Ok((
                    query_calendar_heatmap_rows(connection, events_sql, from_ms, to_ms)?,
                    query_api_activity_rows(connection, events_sql, 4)?,
                    query_top_updated_contents_rows(connection, events_sql, 4, 20)?,
                    query_average_time_to_publish_rows(connection, events_sql, 4)?,
                ))
            })
            .await
            .unwrap();

        assert!(calendar.iter().any(|row| {
            row.time == format!("{zero_date}T00:00:00Z") && row.value == 0
        }));
        assert!(calendar.iter().any(|row| {
            row.time == format!("{event_date}T00:00:00Z") && row.value == 4
        }));

        assert_eq!(activity.len(), 2);
        assert_eq!(activity[0].api.as_deref(), Some("blogs"));
        assert_eq!(activity[0].new_count, 1);
        assert_eq!(activity[0].edit_count, 2);
        assert_eq!(activity[0].delete_count, 0);
        assert_eq!(activity[0].total_count, 3);
        assert_eq!(activity[1].api.as_deref(), Some("authors"));
        assert_eq!(activity[1].delete_count, 1);

        assert_eq!(top_contents.len(), 2);
        assert_eq!(top_contents[0].api.as_deref(), Some("blogs"));
        assert_eq!(top_contents[0].content_id.as_deref(), Some("content-1"));
        assert_eq!(top_contents[0].count, 2);

        assert_eq!(publish_duration.len(), 1);
        assert_eq!(publish_duration[0].api.as_deref(), Some("blogs"));
        assert_eq!(publish_duration[0].avg_days, 1.125);
    }
}
