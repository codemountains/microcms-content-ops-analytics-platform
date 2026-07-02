use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use webhook_ingest::{
    DebugSeedConfig, DebugSeedPreset, DebugSeedSummary, IngestError, generate_debug_parquet_files,
};

fn main() -> ExitCode {
    match run() {
        Ok(summary) => {
            print_summary(&summary);
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<DebugSeedSummary, IngestError> {
    let mut preset = DebugSeedPreset::Smoke;
    let mut output_dir = PathBuf::from(".debug/parquet");
    let mut count = 10_000;
    let mut days = 365;
    let mut contents = 200;
    let mut rows_per_file = 500;
    let mut seed = 42;

    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--preset" => {
                preset = parse_preset(require_value(&mut args, "--preset")?)?;
            }
            "--output-dir" => {
                output_dir = PathBuf::from(require_value(&mut args, "--output-dir")?);
            }
            "--count" => {
                count = require_value(&mut args, "--count")?
                    .parse()
                    .map_err(|_| IngestError::Parquet("invalid --count".into()))?;
            }
            "--days" => {
                days = require_value(&mut args, "--days")?
                    .parse()
                    .map_err(|_| IngestError::Parquet("invalid --days".into()))?;
            }
            "--contents" => {
                contents = require_value(&mut args, "--contents")?
                    .parse()
                    .map_err(|_| IngestError::Parquet("invalid --contents".into()))?;
            }
            "--rows-per-file" => {
                rows_per_file = require_value(&mut args, "--rows-per-file")?
                    .parse()
                    .map_err(|_| IngestError::Parquet("invalid --rows-per-file".into()))?;
            }
            "--seed" => {
                seed = require_value(&mut args, "--seed")?
                    .parse()
                    .map_err(|_| IngestError::Parquet("invalid --seed".into()))?;
            }
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            other => {
                return Err(IngestError::Parquet(format!("unknown argument: {other}")));
            }
        }
    }

    generate_debug_parquet_files(&DebugSeedConfig {
        output_dir,
        preset,
        count,
        days,
        contents,
        rows_per_file,
        seed,
    })
}

fn print_summary(summary: &DebugSeedSummary) {
    println!("Generated debug Parquet seed data.");
    println!("  events: {}", summary.event_count);
    println!("  files: {}", summary.file_count);
    println!("  partitions: {}", summary.partition_count);
    if let (Some(min_dt), Some(max_dt)) = (summary.min_dt, summary.max_dt) {
        println!("  date range (JST): {min_dt} .. {max_dt}");
    }
}

fn require_value(
    args: &mut impl Iterator<Item = String>,
    flag: &str,
) -> Result<String, IngestError> {
    args.next()
        .ok_or_else(|| IngestError::Parquet(format!("missing value for {flag}")))
}

fn parse_preset(value: String) -> Result<DebugSeedPreset, IngestError> {
    match value.as_str() {
        "smoke" => Ok(DebugSeedPreset::Smoke),
        "bulk" => Ok(DebugSeedPreset::Bulk),
        _ => Err(IngestError::Parquet("preset must be smoke or bulk".into())),
    }
}

fn print_help() {
    println!(
        r#"Generate local debug Parquet files for microCMS content ops analytics.

Usage:
  debug-parquet-seed [--preset smoke|bulk] [options]

Options:
  --output-dir <path>     Output directory (default: .debug/parquet)
  --preset <name>         smoke (8 handler-compatible files) or bulk (default: smoke)
  --count <n>             Bulk event count (default: 10000)
  --days <n>              Bulk JST day span, inclusive (default: 365)
  --contents <n>          Bulk unique content IDs (default: 200)
  --rows-per-file <n>     Bulk rows per batched parquet file (default: 500)
  --seed <n>              Bulk RNG seed (default: 42)
  -h, --help              Show this help
"#
    );
}
