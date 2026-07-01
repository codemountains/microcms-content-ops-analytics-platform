use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("missing required environment variable: {0}")]
    MissingEnv(&'static str),
    #[error("invalid query parameter: {0}")]
    InvalidQuery(&'static str),
    #[error("duckdb query failed: {0}")]
    DuckDb(String),
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
