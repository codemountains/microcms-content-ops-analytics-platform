use crate::ApiError;
use crate::config::AppConfig;
use crate::storage::DuckDbEngine;

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) duckdb: DuckDbEngine,
}

impl AppState {
    pub(crate) fn try_new(config: AppConfig) -> Result<Self, ApiError> {
        let duckdb = DuckDbEngine::new(
            &config.events_path,
            &config.aws_region,
            &config.duckdb_extension_directory,
            config.duckdb_s3_endpoint.as_deref(),
            &config.duckdb_s3_url_style,
            config.duckdb_s3_use_ssl,
        )?;

        Ok(Self { duckdb })
    }
}
