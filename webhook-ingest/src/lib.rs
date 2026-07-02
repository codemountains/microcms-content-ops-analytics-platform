mod config;
mod debug_seed;
mod error;
mod handler;
mod ingest;
mod signature;
mod storage;

pub use debug_seed::{
    DebugSeedConfig, DebugSeedPreset, DebugSeedSummary, generate_debug_parquet_files,
};
pub use error::IngestError;
pub use handler::{AppState, AppStateInit, handler, handler_from_init};
pub use ingest::{NormalizedEvent, event_to_parquet, events_to_parquet, normalize_payload};
pub use signature::verify_signature;
pub use storage::build_s3_key;
