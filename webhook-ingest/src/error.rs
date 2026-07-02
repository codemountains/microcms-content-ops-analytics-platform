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
