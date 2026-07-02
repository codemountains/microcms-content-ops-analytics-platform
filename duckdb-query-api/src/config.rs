use std::env;

use crate::ApiError;

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
