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
struct TopEditedQuery {
    days: Option<u32>,
    limit: Option<u32>,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    ok: bool,
}

#[derive(Debug, Serialize)]
pub struct DailyEventsRow {
    pub dt: String,
    pub api: Option<String>,
    pub event_type: Option<String>,
    pub event_count: i64,
}

#[derive(Debug, Serialize)]
pub struct EventsByApiRow {
    pub api: Option<String>,
    pub event_count: i64,
    pub content_count: i64,
}

#[derive(Debug, Serialize)]
pub struct TopEditedContentRow {
    pub api: Option<String>,
    pub content_id: Option<String>,
    pub title: Option<String>,
    pub edit_count: i64,
    pub last_event_at: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct StatusEventsRow {
    pub dt: String,
    pub api: Option<String>,
    pub status: Option<String>,
    pub event_count: i64,
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
        .route("/metrics/daily-events", get(daily_events))
        .route("/metrics/events-by-api", get(events_by_api))
        .route("/metrics/top-edited-contents", get(top_edited_contents))
        .route("/metrics/status-events", get(status_events))
        .with_state(state)
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { ok: true })
}

async fn daily_events(
    State(state): State<AppState>,
    Query(query): Query<DaysQuery>,
) -> Result<Json<Vec<DailyEventsRow>>, ApiError> {
    let days = validate_days(query.days)?;
    query_duckdb(state, move |connection, events_sql| {
        let sql = format!(
            r#"
            SELECT
              CAST(dt AS VARCHAR) AS dt,
              api,
              event_type,
              COUNT(*) AS event_count
            FROM {events_sql}
            WHERE dt >= CAST(CAST(current_timestamp AS TIMESTAMP) - INTERVAL '{days} DAY' AS DATE)
            GROUP BY dt, api, event_type
            ORDER BY dt, api, event_type
            "#
        );

        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map([], |row| {
            Ok(DailyEventsRow {
                dt: row.get(0)?,
                api: row.get(1)?,
                event_type: row.get(2)?,
                event_count: row.get(3)?,
            })
        })?;
        collect_rows(rows)
    })
    .await
    .map(Json)
}

async fn events_by_api(
    State(state): State<AppState>,
    Query(query): Query<DaysQuery>,
) -> Result<Json<Vec<EventsByApiRow>>, ApiError> {
    let days = validate_days(query.days)?;
    query_duckdb(state, move |connection, events_sql| {
        let sql = format!(
            r#"
            SELECT
              api,
              COUNT(*) AS event_count,
              COUNT(DISTINCT content_id) AS content_count
            FROM {events_sql}
            WHERE dt >= CAST(CAST(current_timestamp AS TIMESTAMP) - INTERVAL '{days} DAY' AS DATE)
            GROUP BY api
            ORDER BY event_count DESC
            "#
        );

        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map([], |row| {
            Ok(EventsByApiRow {
                api: row.get(0)?,
                event_count: row.get(1)?,
                content_count: row.get(2)?,
            })
        })?;
        collect_rows(rows)
    })
    .await
    .map(Json)
}

async fn top_edited_contents(
    State(state): State<AppState>,
    Query(query): Query<TopEditedQuery>,
) -> Result<Json<Vec<TopEditedContentRow>>, ApiError> {
    let days = validate_days(query.days)?;
    let limit = validate_limit(query.limit)?;
    query_duckdb(state, move |connection, events_sql| {
        let sql = format!(
            r#"
            SELECT
              api,
              content_id,
              any_value(title) AS title,
              COUNT(*) AS edit_count,
              CAST(MAX(received_at) AS VARCHAR) AS last_event_at
            FROM {events_sql}
            WHERE
              dt >= CAST(CAST(current_timestamp AS TIMESTAMP) - INTERVAL '{days} DAY' AS DATE)
              AND event_type = 'edit'
            GROUP BY api, content_id
            ORDER BY edit_count DESC, last_event_at DESC
            LIMIT {limit}
            "#
        );

        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map([], |row| {
            Ok(TopEditedContentRow {
                api: row.get(0)?,
                content_id: row.get(1)?,
                title: row.get(2)?,
                edit_count: row.get(3)?,
                last_event_at: row.get(4)?,
            })
        })?;
        collect_rows(rows)
    })
    .await
    .map(Json)
}

async fn status_events(
    State(state): State<AppState>,
    Query(query): Query<DaysQuery>,
) -> Result<Json<Vec<StatusEventsRow>>, ApiError> {
    let days = validate_days(query.days)?;
    query_duckdb(state, move |connection, events_sql| {
        let sql = format!(
            r#"
            SELECT
              CAST(dt AS VARCHAR) AS dt,
              api,
              new_status AS status,
              COUNT(*) AS event_count
            FROM {events_sql}
            WHERE dt >= CAST(CAST(current_timestamp AS TIMESTAMP) - INTERVAL '{days} DAY' AS DATE)
            GROUP BY dt, api, new_status
            ORDER BY dt, api, new_status
            "#
        );

        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map([], |row| {
            Ok(StatusEventsRow {
                dt: row.get(0)?,
                api: row.get(1)?,
                status: row.get(2)?,
                event_count: row.get(3)?,
            })
        })?;
        collect_rows(rows)
    })
    .await
    .map(Json)
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
    let days = days.unwrap_or(30);
    if (1..=3660).contains(&days) {
        Ok(days)
    } else {
        Err(ApiError::InvalidQuery("days must be between 1 and 3660"))
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
    use tempfile::tempdir;

    #[test]
    fn validates_days() {
        assert_eq!(validate_days(None).unwrap(), 30);
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
    async fn queries_local_hive_partitioned_parquet() {
        let tempdir = tempdir().unwrap();
        let partition_dir = tempdir
            .path()
            .join("microcms_events/service=example-service/api=blogs/dt=2026-06-29");
        fs::create_dir_all(&partition_dir).unwrap();
        let parquet_path = partition_dir.join("events.parquet");
        let parquet_path_sql = sql_string_literal(&parquet_path.to_string_lossy());
        let extension_dir = tempdir.path().join("duckdb_extensions");
        let extension_dir_sql = sql_string_literal(&extension_dir.to_string_lossy());

        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(&format!(
                r#"
                SET extension_directory = '{extension_dir_sql}';
                INSTALL parquet;
                LOAD parquet;
                COPY (
                  SELECT
                    TIMESTAMP '2026-06-29 12:00:00' AS received_at,
                    'example-service' AS service,
                    'blogs' AS api,
                    'content-1' AS content_id,
                    'edit' AS event_type,
                    'DRAFT' AS old_status,
                    'PUBLISH' AS new_status,
                    TIMESTAMP '2026-06-28 12:00:00' AS old_updated_at,
                    TIMESTAMP '2026-06-29 12:00:00' AS new_updated_at,
                    'Title 1' AS title,
                    '{{}}' AS raw_payload
                  UNION ALL
                  SELECT
                    TIMESTAMP '2026-06-29 13:00:00',
                    'example-service',
                    'blogs',
                    'content-1',
                    'edit',
                    'PUBLISH',
                    'PUBLISH',
                    TIMESTAMP '2026-06-29 12:00:00',
                    TIMESTAMP '2026-06-29 13:00:00',
                    'Title 1',
                    '{{}}'
                ) TO '{parquet_path_sql}' (FORMAT PARQUET)
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

        let rows = query_duckdb(state, move |connection, events_sql| {
            let sql = format!(
                r#"
                SELECT
                  api,
                  COUNT(*) AS event_count,
                  COUNT(DISTINCT content_id) AS content_count
                FROM {events_sql}
                WHERE dt >= CAST(CAST(current_timestamp AS TIMESTAMP) - INTERVAL '3660 DAY' AS DATE)
                GROUP BY api
                ORDER BY event_count DESC
                "#
            );
            let mut statement = connection.prepare(&sql)?;
            let rows = statement.query_map([], |row| {
                Ok(EventsByApiRow {
                    api: row.get(0)?,
                    event_count: row.get(1)?,
                    content_count: row.get(2)?,
                })
            })?;
            collect_rows(rows)
        })
        .await
        .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].api.as_deref(), Some("blogs"));
        assert_eq!(rows[0].event_count, 2);
        assert_eq!(rows[0].content_count, 1);
    }
}
