mod response;

use std::sync::Arc;

use aws_sdk_s3::Client as S3Client;
use bytes::Bytes;
use chrono::Utc;
use lambda_http::{Body, Error, IntoResponse, Request};
use uuid::Uuid;

use self::response::{error_response, success_response};
use crate::config::Config;
use crate::s3::client_from_env;
use crate::{IngestError, build_s3_key, event_to_parquet, normalize_payload, verify_signature};

#[derive(Clone)]
pub struct AppState {
    config: Arc<Config>,
    s3_client: S3Client,
}

impl AppState {
    pub async fn from_env() -> Result<Self, IngestError> {
        Ok(Self {
            config: Arc::new(Config::from_env()?),
            s3_client: client_from_env().await,
        })
    }
}

pub async fn handler(request: Request, state: AppState) -> Result<impl IntoResponse, Error> {
    let signature = match header_value(&request, "x-microcms-signature") {
        Some(signature) => signature.to_owned(),
        None => return Ok(error_response(IngestError::MissingSignature)),
    };

    let body = request_body(request.body());
    if !verify_signature(body.as_ref(), state.config.secret.as_bytes(), &signature) {
        return Ok(error_response(IngestError::InvalidSignature));
    }

    let received_at = Utc::now();
    let event = match normalize_payload(body.as_ref(), received_at) {
        Ok(event) => event,
        Err(error) => return Ok(error_response(error)),
    };

    let parquet = match event_to_parquet(&event) {
        Ok(parquet) => parquet,
        Err(error) => return Ok(error_response(error)),
    };

    let service = event.service.as_deref().unwrap_or("unknown");
    let api = event.api.as_deref().unwrap_or("unknown");
    let event_id = Uuid::now_v7();
    let key = build_s3_key(
        &state.config.prefix,
        service,
        api,
        event.received_at,
        &event_id.to_string(),
    );

    if let Err(error) = state
        .s3_client
        .put_object()
        .bucket(&state.config.bucket)
        .key(&key)
        .content_type("application/vnd.apache.parquet")
        .body(parquet.into())
        .send()
        .await
    {
        return Ok(error_response(IngestError::S3(error.to_string())));
    }

    Ok(success_response(key))
}

fn request_body(body: &Body) -> Bytes {
    match body {
        Body::Text(text) => Bytes::copy_from_slice(text.as_bytes()),
        Body::Binary(bytes) => Bytes::copy_from_slice(bytes),
        Body::Empty => Bytes::new(),
    }
}

fn header_value<'a>(request: &'a Request, name: &str) -> Option<&'a str> {
    request
        .headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .or_else(|| {
            request
                .headers()
                .get(name.to_ascii_lowercase())
                .and_then(|value| value.to_str().ok())
        })
}
