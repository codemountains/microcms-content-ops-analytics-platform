use std::env;
use std::io::Cursor;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, RecordBatch,
    builder::{StringBuilder, TimestampMicrosecondBuilder},
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client as S3Client;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use lambda_http::{Body, Error, IntoResponse, Request, Response, http::StatusCode};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::Sha256;
use uuid::Uuid;

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("missing required header: x-microcms-signature")]
    MissingSignature,
    #[error("invalid webhook signature")]
    InvalidSignature,
    #[error("missing required environment variable: {0}")]
    MissingEnv(&'static str),
    #[error("invalid request body")]
    InvalidBody,
    #[error("failed to parse webhook payload: {0}")]
    ParsePayload(#[from] serde_json::Error),
    #[error("failed to build parquet: {0}")]
    Parquet(String),
    #[error("failed to put object to s3: {0}")]
    S3(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NormalizedEvent {
    pub received_at: DateTime<Utc>,
    pub service: Option<String>,
    pub api: Option<String>,
    pub content_id: Option<String>,
    pub event_type: Option<String>,
    pub old_status: Option<String>,
    pub new_status: Option<String>,
    pub old_updated_at: Option<DateTime<Utc>>,
    pub new_updated_at: Option<DateTime<Utc>>,
    pub title: Option<String>,
    pub raw_payload: String,
}

#[derive(Debug, Deserialize)]
struct MicrocmsWebhook {
    service: Option<String>,
    api: Option<String>,
    id: Option<String>,
    #[serde(rename = "type")]
    event_type: Option<String>,
    contents: Option<WebhookContents>,
}

#[derive(Debug, Deserialize)]
struct WebhookContents {
    old: Option<Value>,
    new: Option<Value>,
}

#[derive(Debug, Clone)]
struct Config {
    bucket: String,
    prefix: String,
    secret: String,
}

pub async fn handler(request: Request) -> Result<impl IntoResponse, Error> {
    let config = match Config::from_env() {
        Ok(config) => config,
        Err(error) => return Ok(error_response(error)),
    };

    let signature = match header_value(&request, "x-microcms-signature") {
        Some(signature) => signature.to_owned(),
        None => return Ok(error_response(IngestError::MissingSignature)),
    };

    let body = match request.body() {
        Body::Text(text) => Bytes::copy_from_slice(text.as_bytes()),
        Body::Binary(bytes) => Bytes::copy_from_slice(bytes),
        Body::Empty => Bytes::new(),
    };

    if !verify_signature(body.as_ref(), config.secret.as_bytes(), &signature) {
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
        &config.prefix,
        service,
        api,
        event.received_at,
        &event_id.to_string(),
    );

    let aws_config = aws_config::defaults(BehaviorVersion::latest()).load().await;
    let mut s3_config_builder = aws_sdk_s3::config::Builder::from(&aws_config);
    if let Ok(endpoint_url) = env::var("AWS_ENDPOINT_URL") {
        s3_config_builder = s3_config_builder.endpoint_url(endpoint_url);
    }
    if env_bool("AWS_S3_FORCE_PATH_STYLE") {
        s3_config_builder = s3_config_builder.force_path_style(true);
    }
    let s3_client = S3Client::from_conf(s3_config_builder.build());
    if let Err(error) = s3_client
        .put_object()
        .bucket(&config.bucket)
        .key(&key)
        .content_type("application/vnd.apache.parquet")
        .body(parquet.into())
        .send()
        .await
    {
        return Ok(error_response(IngestError::S3(error.to_string())));
    }

    Ok(json_response(
        StatusCode::OK,
        json!({
            "ok": true,
            "s3_key": key,
        }),
    ))
}

impl Config {
    fn from_env() -> Result<Self, IngestError> {
        let bucket = required_env("EVENT_BUCKET")?;
        let prefix = env::var("EVENT_PREFIX").unwrap_or_else(|_| "microcms_events".to_owned());
        let secret = required_env("MICROCMS_WEBHOOK_SECRET")?;

        Ok(Self {
            bucket,
            prefix: prefix.trim_matches('/').to_owned(),
            secret,
        })
    }
}

fn required_env(key: &'static str) -> Result<String, IngestError> {
    env::var(key).map_err(|_| IngestError::MissingEnv(key))
}

fn env_bool(key: &str) -> bool {
    env::var(key)
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
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

pub fn verify_signature(body: &[u8], secret: &[u8], signature: &str) -> bool {
    let mut mac = HmacSha256::new_from_slice(secret).expect("HMAC accepts arbitrary key length");
    mac.update(body);
    let digest = mac.finalize().into_bytes();
    let signature = signature.trim();
    let signature = signature
        .strip_prefix("sha256=")
        .or_else(|| signature.strip_prefix("SHA256="))
        .unwrap_or(signature);

    if let Ok(expected) = hex::decode(signature) {
        return constant_time_eq(&digest, &expected);
    }

    if let Ok(expected) = BASE64_STANDARD.decode(signature) {
        return constant_time_eq(&digest, &expected);
    }

    false
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }

    left.iter()
        .zip(right)
        .fold(0_u8, |acc, (left, right)| acc | (left ^ right))
        == 0
}

pub fn normalize_payload(
    body: &[u8],
    received_at: DateTime<Utc>,
) -> Result<NormalizedEvent, IngestError> {
    let raw_payload = std::str::from_utf8(body)
        .map_err(|_| IngestError::InvalidBody)?
        .to_owned();
    let payload: MicrocmsWebhook = serde_json::from_str(&raw_payload)?;
    let old = payload
        .contents
        .as_ref()
        .and_then(|contents| contents.old.as_ref());
    let new = payload
        .contents
        .as_ref()
        .and_then(|contents| contents.new.as_ref());

    Ok(NormalizedEvent {
        received_at,
        service: payload.service,
        api: payload.api,
        content_id: payload.id,
        event_type: payload.event_type,
        old_status: extract_string(old, &["status"]),
        new_status: extract_string(new, &["status"]),
        old_updated_at: extract_timestamp(old, &["updatedAt", "updated_at"]),
        new_updated_at: extract_timestamp(new, &["updatedAt", "updated_at"]),
        title: extract_string(new, &["title"]).or_else(|| extract_string(old, &["title"])),
        raw_payload,
    })
}

fn extract_string(value: Option<&Value>, keys: &[&str]) -> Option<String> {
    let object = value?.as_object()?;
    keys.iter()
        .filter_map(|key| object.get(*key))
        .find_map(|value| value.as_str().map(ToOwned::to_owned))
}

fn extract_timestamp(value: Option<&Value>, keys: &[&str]) -> Option<DateTime<Utc>> {
    extract_string(value, keys)
        .and_then(|value| DateTime::parse_from_rfc3339(&value).ok())
        .map(|value| value.with_timezone(&Utc))
}

pub fn event_to_parquet(event: &NormalizedEvent) -> Result<Vec<u8>, IngestError> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "received_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("service", DataType::Utf8, true),
        Field::new("api", DataType::Utf8, true),
        Field::new("content_id", DataType::Utf8, true),
        Field::new("event_type", DataType::Utf8, true),
        Field::new("old_status", DataType::Utf8, true),
        Field::new("new_status", DataType::Utf8, true),
        Field::new(
            "old_updated_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "new_updated_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("title", DataType::Utf8, true),
        Field::new("raw_payload", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            timestamp_required(event.received_at),
            string_optional(event.service.as_deref()),
            string_optional(event.api.as_deref()),
            string_optional(event.content_id.as_deref()),
            string_optional(event.event_type.as_deref()),
            string_optional(event.old_status.as_deref()),
            string_optional(event.new_status.as_deref()),
            timestamp_optional(event.old_updated_at),
            timestamp_optional(event.new_updated_at),
            string_optional(event.title.as_deref()),
            string_required(&event.raw_payload),
        ],
    )
    .map_err(|error| IngestError::Parquet(error.to_string()))?;

    let mut buffer = Cursor::new(Vec::new());
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props))
        .map_err(|error| IngestError::Parquet(error.to_string()))?;
    writer
        .write(&batch)
        .map_err(|error| IngestError::Parquet(error.to_string()))?;
    writer
        .close()
        .map_err(|error| IngestError::Parquet(error.to_string()))?;

    Ok(buffer.into_inner())
}

fn string_required(value: &str) -> ArrayRef {
    let mut builder = StringBuilder::with_capacity(1, value.len());
    builder.append_value(value);
    Arc::new(builder.finish())
}

fn string_optional(value: Option<&str>) -> ArrayRef {
    let mut builder = StringBuilder::with_capacity(1, value.map(str::len).unwrap_or_default());
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
    Arc::new(builder.finish())
}

fn timestamp_required(value: DateTime<Utc>) -> ArrayRef {
    let mut builder = TimestampMicrosecondBuilder::with_capacity(1).with_timezone("UTC");
    builder.append_value(value.timestamp_micros());
    Arc::new(builder.finish())
}

fn timestamp_optional(value: Option<DateTime<Utc>>) -> ArrayRef {
    let mut builder = TimestampMicrosecondBuilder::with_capacity(1).with_timezone("UTC");
    match value {
        Some(value) => builder.append_value(value.timestamp_micros()),
        None => builder.append_null(),
    }
    Arc::new(builder.finish())
}

pub fn build_s3_key(
    prefix: &str,
    service: &str,
    api: &str,
    received_at: DateTime<Utc>,
    event_id: &str,
) -> String {
    format!(
        "{}/service={}/api={}/dt={}/{}.parquet",
        prefix.trim_matches('/'),
        partition_escape(service),
        partition_escape(api),
        received_at.format("%Y-%m-%d"),
        event_id
    )
}

fn partition_escape(value: &str) -> String {
    value
        .chars()
        .map(|char| match char {
            '/' | '=' | '?' | '#' | '[' | ']' | ' ' => '_',
            char => char,
        })
        .collect()
}

fn json_response(status: StatusCode, body: Value) -> Response<String> {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(body.to_string())
        .expect("valid response")
}

fn error_response(error: IngestError) -> Response<String> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, StringArray, TimestampMicrosecondArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    fn sample_body() -> &'static [u8] {
        br#"{
          "service": "example-service",
          "api": "blogs",
          "id": "content-id",
          "type": "edit",
          "contents": {
            "old": {"status": "DRAFT", "updatedAt": "2026-06-28T12:00:00Z", "title": "Old title"},
            "new": {"status": "PUBLISH", "updatedAt": "2026-06-29T12:00:00Z", "title": "Example title"}
          }
        }"#
    }

    #[test]
    fn verifies_hex_hmac_signature() {
        let mut mac = HmacSha256::new_from_slice(b"secret").unwrap();
        mac.update(sample_body());
        let signature = hex::encode(mac.finalize().into_bytes());

        assert!(verify_signature(sample_body(), b"secret", &signature));
        assert!(verify_signature(
            sample_body(),
            b"secret",
            &format!("sha256={signature}")
        ));
        assert!(!verify_signature(sample_body(), b"wrong", &signature));
    }

    #[test]
    fn normalizes_microcms_payload() {
        let received_at = DateTime::parse_from_rfc3339("2026-06-29T01:02:03Z")
            .unwrap()
            .with_timezone(&Utc);
        let event = normalize_payload(sample_body(), received_at).unwrap();

        assert_eq!(event.service.as_deref(), Some("example-service"));
        assert_eq!(event.api.as_deref(), Some("blogs"));
        assert_eq!(event.content_id.as_deref(), Some("content-id"));
        assert_eq!(event.event_type.as_deref(), Some("edit"));
        assert_eq!(event.old_status.as_deref(), Some("DRAFT"));
        assert_eq!(event.new_status.as_deref(), Some("PUBLISH"));
        assert_eq!(event.title.as_deref(), Some("Example title"));
        assert_eq!(
            event.new_updated_at.unwrap().to_rfc3339(),
            "2026-06-29T12:00:00+00:00"
        );
    }

    #[test]
    fn builds_partitioned_s3_key() {
        let received_at = DateTime::parse_from_rfc3339("2026-06-29T01:02:03Z")
            .unwrap()
            .with_timezone(&Utc);

        assert_eq!(
            build_s3_key(
                "/microcms_events/",
                "example/service",
                "blogs api",
                received_at,
                "event-id"
            ),
            "microcms_events/service=example_service/api=blogs_api/dt=2026-06-29/event-id.parquet"
        );
    }

    #[test]
    fn writes_single_event_parquet() {
        let received_at = DateTime::parse_from_rfc3339("2026-06-29T01:02:03Z")
            .unwrap()
            .with_timezone(&Utc);
        let event = normalize_payload(sample_body(), received_at).unwrap();
        let parquet = event_to_parquet(&event).unwrap();
        assert!(!parquet.is_empty());

        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(parquet)).unwrap();
        let mut reader = builder.build().unwrap();
        let batch = reader.next().unwrap().unwrap();

        let received_at = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(received_at.value(0), event.received_at.timestamp_micros());

        let api = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(api.value(0), "blogs");
        assert_eq!(batch.num_rows(), 1);
    }
}
