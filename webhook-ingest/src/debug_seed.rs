mod bulk;
mod config;
mod fixture;
mod io;
mod rng;
mod smoke;
mod time;
mod ulid;

use crate::IngestError;

pub use config::{DebugSeedConfig, DebugSeedPreset, DebugSeedSummary};

pub(super) const EVENT_PREFIX: &str = "microcms_events";
pub(super) const SERVICE_ID: &str = "example-service";

pub fn generate_debug_parquet_files(
    config: &DebugSeedConfig,
) -> Result<DebugSeedSummary, IngestError> {
    validate_config(config)?;
    io::prepare_output_events_dir(&config.output_dir)?;

    match config.preset {
        DebugSeedPreset::Smoke => smoke::generate_smoke_files(config),
        DebugSeedPreset::Bulk => bulk::generate_bulk_files(config),
    }
}

fn validate_config(config: &DebugSeedConfig) -> Result<(), IngestError> {
    if config.days == 0 || config.days > 3660 {
        return Err(IngestError::Parquet(
            "days must be between 1 and 3660".to_owned(),
        ));
    }
    if config.preset == DebugSeedPreset::Bulk && config.count == 0 {
        return Err(IngestError::Parquet(
            "count must be greater than 0".to_owned(),
        ));
    }
    if config.preset == DebugSeedPreset::Bulk && config.contents == 0 {
        return Err(IngestError::Parquet(
            "contents must be greater than 0".to_owned(),
        ));
    }
    if config.rows_per_file == 0 {
        return Err(IngestError::Parquet(
            "rows_per_file must be greater than 0".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
