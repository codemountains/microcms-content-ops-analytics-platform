use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use duckdb::Connection;

use super::connection::{configure_connection, read_parquet_sql};
use crate::ApiError;

#[derive(Clone)]
pub(crate) struct DuckDbEngine {
    inner: Arc<DuckDbEngineInner>,
}

struct DuckDbEngineInner {
    connections: Vec<Mutex<Connection>>,
    events_sql: Arc<str>,
    next_connection: AtomicUsize,
}

const DEFAULT_POOL_SIZE: usize = 4;

impl DuckDbEngine {
    pub(crate) fn new(
        events_path: &str,
        aws_region: &str,
        extension_directory: &str,
        s3_endpoint: Option<&str>,
        s3_url_style: &str,
        s3_use_ssl: bool,
    ) -> Result<Self, ApiError> {
        Self::new_with_pool_size(
            events_path,
            aws_region,
            extension_directory,
            s3_endpoint,
            s3_url_style,
            s3_use_ssl,
            DEFAULT_POOL_SIZE,
        )
    }

    pub(crate) fn new_with_pool_size(
        events_path: &str,
        aws_region: &str,
        extension_directory: &str,
        s3_endpoint: Option<&str>,
        s3_url_style: &str,
        s3_use_ssl: bool,
        pool_size: usize,
    ) -> Result<Self, ApiError> {
        let pool_size = pool_size.max(1);
        let mut connections = Vec::with_capacity(pool_size);
        for _ in 0..pool_size {
            connections.push(Mutex::new(configured_connection(
                events_path,
                aws_region,
                extension_directory,
                s3_endpoint,
                s3_url_style,
                s3_use_ssl,
            )?));
        }

        Ok(Self {
            inner: Arc::new(DuckDbEngineInner {
                connections,
                events_sql: Arc::from(read_parquet_sql(events_path)),
                next_connection: AtomicUsize::new(0),
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
            let connection_index =
                inner.next_connection.fetch_add(1, Ordering::Relaxed) % inner.connections.len();
            let connection = inner.connections[connection_index]
                .lock()
                .map_err(|error| ApiError::DuckDb(error.to_string()))?;
            query(&connection, &inner.events_sql)
                .map_err(|error| ApiError::DuckDb(error.to_string()))
        })
        .await
        .map_err(|error| ApiError::DuckDb(error.to_string()))?
    }
}

fn configured_connection(
    events_path: &str,
    aws_region: &str,
    extension_directory: &str,
    s3_endpoint: Option<&str>,
    s3_url_style: &str,
    s3_use_ssl: bool,
) -> Result<Connection, ApiError> {
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

    Ok(connection)
}
