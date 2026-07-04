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
    pub mcp_enabled: bool,
    pub mcp_bearer_token: Option<String>,
    pub mcp_allowed_origins: Vec<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct McpConfig {
    pub(crate) bearer_token: String,
    pub(crate) allowed_origins: Vec<String>,
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
        let mcp_enabled = env::var("MCP_ENABLED")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(false);
        let mcp_bearer_token = if mcp_enabled {
            Some(required_env("MCP_BEARER_TOKEN")?)
        } else {
            optional_env("MCP_BEARER_TOKEN")
        };
        let mcp_allowed_origins = optional_env("MCP_ALLOWED_ORIGINS")
            .map(|value| parse_csv(&value))
            .unwrap_or_default();
        if mcp_enabled && mcp_allowed_origins.is_empty() {
            return Err(ApiError::MissingEnv("MCP_ALLOWED_ORIGINS"));
        }

        Ok(Self {
            events_path,
            aws_region,
            duckdb_extension_directory,
            duckdb_s3_endpoint,
            duckdb_s3_url_style,
            duckdb_s3_use_ssl,
            port,
            mcp_enabled,
            mcp_bearer_token,
            mcp_allowed_origins,
        })
    }

    pub(crate) fn mcp_config(&self) -> Result<Option<McpConfig>, ApiError> {
        if !self.mcp_enabled {
            return Ok(None);
        }

        let bearer_token = self
            .mcp_bearer_token
            .clone()
            .filter(|value| !value.trim().is_empty())
            .ok_or(ApiError::MissingEnv("MCP_BEARER_TOKEN"))?;
        if self.mcp_allowed_origins.is_empty() {
            return Err(ApiError::MissingEnv("MCP_ALLOWED_ORIGINS"));
        }

        Ok(Some(McpConfig {
            bearer_token,
            allowed_origins: self.mcp_allowed_origins.clone(),
        }))
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

fn parse_csv(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}
