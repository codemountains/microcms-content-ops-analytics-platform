mod config;
mod error;
mod handler;
mod ingest;
mod signature;
mod storage;

pub use error::IngestError;
pub use handler::{AppState, handler};
pub use ingest::{NormalizedEvent, event_to_parquet, normalize_payload};
pub use signature::verify_signature;
pub use storage::build_s3_key;
