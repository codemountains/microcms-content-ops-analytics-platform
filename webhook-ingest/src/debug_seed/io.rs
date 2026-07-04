use std::fs;
use std::path::Path;

use chrono::NaiveDate;

use crate::{IngestError, NormalizedEvent, build_s3_key, event_to_parquet, events_to_parquet};

use super::EVENT_PREFIX;
use super::time::jst_date;

pub(super) fn prepare_output_events_dir(output_dir: &Path) -> Result<(), IngestError> {
    fs::create_dir_all(output_dir).map_err(|error| IngestError::Parquet(error.to_string()))?;
    let events_root = output_dir.join(EVENT_PREFIX);
    if events_root.exists() {
        fs::remove_dir_all(&events_root)
            .map_err(|error| IngestError::Parquet(error.to_string()))?;
    }
    Ok(())
}

pub(super) fn write_single_event_file(
    path: &Path,
    event: &NormalizedEvent,
) -> Result<(), IngestError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| IngestError::Parquet(error.to_string()))?;
    }
    let parquet = event_to_parquet(event)?;
    fs::write(path, parquet).map_err(|error| IngestError::Parquet(error.to_string()))
}

pub(super) fn write_multi_event_file(
    path: &Path,
    events: &[NormalizedEvent],
) -> Result<(), IngestError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| IngestError::Parquet(error.to_string()))?;
    }
    let parquet = events_to_parquet(events)?;
    fs::write(path, parquet).map_err(|error| IngestError::Parquet(error.to_string()))
}

pub(super) fn partition_dir_for_event(event: &NormalizedEvent) -> String {
    let service = event.service.as_deref().unwrap_or("unknown");
    let api = event.api.as_deref().unwrap_or("unknown");
    let key = build_s3_key(EVENT_PREFIX, service, api, event.received_at, "placeholder");
    partition_dir_from_key(&key)
}

pub(super) fn partition_dir_from_key(key: &str) -> String {
    key.rsplit_once('/')
        .map(|(dir, _)| dir.to_owned())
        .unwrap_or_else(|| key.to_owned())
}

pub(super) fn track_date(
    event: &NormalizedEvent,
    min_dt: &mut Option<NaiveDate>,
    max_dt: &mut Option<NaiveDate>,
) {
    let date = jst_date(event.received_at);
    *min_dt = Some(min_dt.map_or(date, |current| current.min(date)));
    *max_dt = Some(max_dt.map_or(date, |current| current.max(date)));
}
