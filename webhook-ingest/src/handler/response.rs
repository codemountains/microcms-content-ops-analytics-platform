use lambda_http::{Response, http::StatusCode};
use serde_json::{Value, json};

use crate::IngestError;

pub(super) fn success_response(key: String) -> Response<String> {
    json_response(
        StatusCode::OK,
        json!({
            "ok": true,
            "s3_key": key,
        }),
    )
}

pub(super) fn error_response(error: IngestError) -> Response<String> {
    error_response_ref(&error)
}

pub(super) fn error_response_ref(error: &IngestError) -> Response<String> {
    let status = match error {
        IngestError::MissingSignature | IngestError::InvalidSignature => StatusCode::UNAUTHORIZED,
        IngestError::MissingEnv(_) => StatusCode::INTERNAL_SERVER_ERROR,
        IngestError::InvalidBody | IngestError::ParsePayload(_) => StatusCode::BAD_REQUEST,
        IngestError::Parquet(_) | IngestError::S3(_) => StatusCode::INTERNAL_SERVER_ERROR,
    };

    json_response(
        status,
        json!({
            "ok": false,
            "message": error.to_string(),
        }),
    )
}

fn json_response(status: StatusCode, body: Value) -> Response<String> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(body.to_string())
        .expect("valid response")
}
