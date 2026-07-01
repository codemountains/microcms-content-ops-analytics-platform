use std::sync::{Arc, Mutex};

use duckdb::Connection;

use super::connection::{configure_connection, read_parquet_sql};
use crate::ApiError;

#[derive(Clone)]
pub(crate) struct DuckDbEngine {
    inner: Arc<DuckDbEngineInner>,
}

struct DuckDbEngineInner {
    connection: Mutex<Connection>,
    events_sql: Arc<str>,
}

impl DuckDbEngine {
    pub(crate) fn new(
        events_path: &str,
        aws_region: &str,
        extension_directory: &str,
        s3_endpoint: Option<&str>,
        s3_url_style: &str,
        s3_use_ssl: bool,
    ) -> Result<Self, ApiError> {
        let connection =
            Connection::open_in_memory().map_err(|error| ApiError::DuckDb(error.to_string()))?;
        configure_connection(
            &connection,
            aws_region,
            events_path,
            extension_directory,
            s3_endpoint,
            s3_url_style,
            s3_use_ssl,
        )
        .map_err(|error| ApiError::DuckDb(error.to_string()))?;

        Ok(Self {
            inner: Arc::new(DuckDbEngineInner {
                connection: Mutex::new(connection),
                events_sql: Arc::from(read_parquet_sql(events_path)),
            }),
        })
    }

    pub(crate) async fn query<T, F>(&self, query: F) -> Result<T, ApiError>
    where
        T: Send + 'static,
        F: FnOnce(&Connection, &str) -> duckdb::Result<T> + Send + 'static,
    {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let connection = inner
                .connection
                .lock()
                .map_err(|error| ApiError::DuckDb(error.to_string()))?;
            query(&connection, &inner.events_sql)
                .map_err(|error| ApiError::DuckDb(error.to_string()))
        })
        .await
        .map_err(|error| ApiError::DuckDb(error.to_string()))?
    }
}
