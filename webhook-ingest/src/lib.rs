mod config;
mod error;
mod handler;
mod normalize;
mod parquet;
mod s3;
mod signature;

pub use error::IngestError;
pub use handler::{AppState, handler};
pub use normalize::{NormalizedEvent, normalize_payload};
pub use parquet::event_to_parquet;
pub use s3::build_s3_key;
pub use signature::verify_signature;
