use std::path::PathBuf;

use chrono::NaiveDate;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DebugSeedPreset {
    Smoke,
    Bulk,
}

#[derive(Debug, Clone)]
pub struct DebugSeedConfig {
    pub output_dir: PathBuf,
    pub preset: DebugSeedPreset,
    pub count: u32,
    pub days: u32,
    pub contents: u32,
    pub rows_per_file: u32,
    pub seed: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DebugSeedSummary {
    pub event_count: usize,
    pub file_count: usize,
    pub partition_count: usize,
    pub min_dt: Option<NaiveDate>,
    pub max_dt: Option<NaiveDate>,
}
