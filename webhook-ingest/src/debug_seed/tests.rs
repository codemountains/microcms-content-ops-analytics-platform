use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use arrow_array::StringArray;
use bytes::Bytes;
use chrono::{Duration, NaiveDate, NaiveTime};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use tempfile::tempdir;

use super::bulk::activity::compute_activity_targets;
use super::bulk::schedule::build_realistic_bulk_day_schedule;
use super::bulk::timing::api_timing_profile;
use super::bulk::{BULK_APIS, generate_bulk_activity_events};
use super::rng::SeededRng;
use super::smoke::{
    SMOKE_BLOG_DIRECT_CONTENT_ULID, SMOKE_BLOG_LIFECYCLE_CONTENT_ULID, SMOKE_EVENT_IDS,
    smoke_fixtures,
};
use super::time::{jst_date, jst_datetime, jst_today};
use super::*;
use crate::{NormalizedEvent, normalize_payload};

#[test]
fn generates_smoke_parquet_files_with_handler_compatible_layout() {
    let tempdir = tempdir().unwrap();
    let summary = generate_debug_parquet_files(&DebugSeedConfig {
        output_dir: tempdir.path().to_path_buf(),
        preset: DebugSeedPreset::Smoke,
        count: 0,
        days: 7,
        contents: 1,
        rows_per_file: 500,
        seed: 42,
    })
    .unwrap();

    assert_eq!(summary.event_count, 8);
    assert_eq!(summary.file_count, 8);
    assert!(summary.partition_count >= 2);
    assert!(summary.min_dt.is_some());
    assert!(summary.max_dt.is_some());

    let parquet_files: Vec<_> = walk_parquet_files(tempdir.path());
    assert_eq!(parquet_files.len(), 8);
    for path in &parquet_files {
        let key = path.strip_prefix(tempdir.path()).unwrap().to_string_lossy();
        assert!(key.contains("/service=example-service/"));
        assert!(key.contains("/api="));
        assert!(key.contains("/dt="));
        assert!(!key.contains("events-"));
        assert!(
            SMOKE_EVENT_IDS
                .iter()
                .any(|event_id| key.ends_with(&format!("{event_id}.parquet"))),
            "unexpected smoke parquet path: {key}"
        );
        read_parquet_row_count(path, 1);
    }
}

#[test]
fn smoke_fixtures_align_content_ids_with_duckdb_integration_test() {
    let event_date = NaiveDate::from_ymd_opt(2026, 6, 29).unwrap();
    let fixtures = smoke_fixtures(
        event_date,
        event_date - Duration::days(1),
        event_date - Duration::days(2),
        event_date - Duration::days(5),
    );
    let received_at = jst_datetime(event_date, NaiveTime::from_hms_opt(12, 0, 0).unwrap());
    let events: Vec<_> = fixtures
        .into_iter()
        .map(|(body, at)| normalize_payload(body.as_bytes(), at).unwrap())
        .collect();

    assert_eq!(events[3].content_id, None);
    assert_eq!(events[4].content_id, None);
    assert_eq!(events[5].content_id, None);
    assert_eq!(events[7].content_id, None);
    assert_ulid(events[0].content_id.as_deref().unwrap());
    assert_ulid(events[6].content_id.as_deref().unwrap());
    assert_eq!(
        events[0].content_id.as_deref(),
        Some(SMOKE_BLOG_LIFECYCLE_CONTENT_ULID)
    );
    assert_eq!(
        events[6].content_id.as_deref(),
        Some(SMOKE_BLOG_DIRECT_CONTENT_ULID)
    );
    assert_eq!(events[0].content_id, events[1].content_id);
    assert_eq!(events[1].content_id, events[2].content_id);
    assert_ne!(events[0].content_id, events[6].content_id);
    assert_eq!(events[0].event_kind.as_deref(), Some("PUBLISH_FROM_DRAFT"));
    assert_eq!(events[3].event_kind.as_deref(), Some("UNPUBLISH_TO_DRAFT"));
    assert_eq!(events[4].event_kind.as_deref(), Some("UNPUBLISH_TO_CLOSED"));
    assert_eq!(events[7].event_kind.as_deref(), Some("DELETE_PUBLISHED"));
    assert_eq!(received_at, events[0].received_at);
}

#[test]
fn generates_bulk_parquet_files_in_batched_layout() {
    let tempdir = tempdir().unwrap();
    let summary = generate_debug_parquet_files(&DebugSeedConfig {
        output_dir: tempdir.path().to_path_buf(),
        preset: DebugSeedPreset::Bulk,
        count: 500,
        days: 90,
        contents: 10,
        rows_per_file: 10,
        seed: 42,
    })
    .unwrap();

    assert_eq!(summary.event_count, 500);
    assert!(summary.file_count < summary.event_count);
    assert!(summary.partition_count > 1);
    let span = summary
        .max_dt
        .unwrap()
        .signed_duration_since(summary.min_dt.unwrap());
    assert!(span.num_days() <= 89);

    let parquet_files = walk_parquet_files(tempdir.path());
    assert_eq!(parquet_files.len(), summary.file_count);
    assert!(
        parquet_files
            .iter()
            .any(|path| path.to_string_lossy().contains("events-000.parquet"))
    );

    let multi_row = parquet_files
        .iter()
        .find(|path| read_parquet_row_count(path, 1) > 1)
        .cloned();
    assert!(multi_row.is_some());
}

#[test]
fn bulk_seed_uses_sparse_calendar_days() {
    let tempdir = tempdir().unwrap();
    let summary = generate_debug_parquet_files(&DebugSeedConfig {
        output_dir: tempdir.path().to_path_buf(),
        preset: DebugSeedPreset::Bulk,
        count: 2_000,
        days: 90,
        contents: 20,
        rows_per_file: 100,
        seed: 42,
    })
    .unwrap();

    assert_eq!(summary.event_count, 2_000);
    let unique_days = count_unique_partition_dates(tempdir.path());
    assert!(
        unique_days <= 77,
        "expected some zero-event days in heatmap, got {unique_days}"
    );
    assert!(
        unique_days >= 67,
        "expected most days to be active for large realistic seed, got {unique_days}"
    );
}

#[test]
fn compute_activity_targets_match_api_activity_ratios() {
    let targets = compute_activity_targets(50_000, 365);
    assert_eq!(targets.total(), 50_000);
    assert_eq!(targets.initial_draft, 10_000);
    assert_eq!(targets.save_draft, 7_500);
    assert_eq!(targets.publish_from_draft, 1_325);
    assert_eq!(targets.initial_publish, 300);
    assert_eq!(targets.update_published, 9_425);
    assert_eq!(targets.add_draft_to_published, 10_000);
    assert_eq!(targets.discard_draft_on_published, 2_500);
    assert_eq!(targets.unpublish_to_draft, 4_000);
    assert_eq!(targets.unpublish_to_closed, 2_000);
    assert_eq!(targets.reopen_to_draft, 500);
    assert_eq!(targets.republish_from_closed, 200);
    assert_eq!(targets.delete_draft, 500);
    assert_eq!(targets.delete_published, 1_500);
    assert_eq!(targets.delete_closed, 250);
}

#[test]
fn bulk_api_set_includes_realistic_content_types() {
    assert_eq!(
        BULK_APIS,
        [
            "blogs",
            "authors",
            "news",
            "categories",
            "pages",
            "advertisements",
            "tags",
            "labels",
            "papers",
            "cards"
        ]
    );
}

#[test]
fn api_timing_profiles_cover_bulk_api_set() {
    for api in BULK_APIS {
        let profile = api_timing_profile(api);
        assert_eq!(profile.api, *api);
        assert!(profile.publish_lead_base_days > 0);
        assert!(profile.draft_to_publish_base_days > 0);
    }
}

#[test]
fn bulk_seed_honors_requested_count_below_legacy_baseline() {
    let tempdir = tempdir().unwrap();
    let summary = generate_debug_parquet_files(&DebugSeedConfig {
        output_dir: tempdir.path().to_path_buf(),
        preset: DebugSeedPreset::Bulk,
        count: 50,
        days: 90,
        contents: 10,
        rows_per_file: 10,
        seed: 42,
    })
    .unwrap();

    assert_eq!(summary.event_count, 50);
}

#[test]
fn regenerate_removes_stale_local_parquet_files() {
    let tempdir = tempdir().unwrap();
    let stale_path = tempdir
        .path()
        .join("microcms_events/service=example-service/api=blogs/dt=2026-01-01/stale.parquet");
    fs::create_dir_all(stale_path.parent().unwrap()).unwrap();
    fs::write(&stale_path, b"stale").unwrap();

    generate_debug_parquet_files(&DebugSeedConfig {
        output_dir: tempdir.path().to_path_buf(),
        preset: DebugSeedPreset::Smoke,
        count: 0,
        days: 7,
        contents: 1,
        rows_per_file: 500,
        seed: 42,
    })
    .unwrap();

    assert!(!stale_path.exists());
    assert_eq!(walk_parquet_files(tempdir.path()).len(), 8);
}

#[test]
fn bulk_seed_uses_ulid_content_ids_without_losing_lifecycle_pairs() {
    let tempdir = tempdir().unwrap();
    generate_debug_parquet_files(&DebugSeedConfig {
        output_dir: tempdir.path().to_path_buf(),
        preset: DebugSeedPreset::Bulk,
        count: 2_000,
        days: 90,
        contents: 20,
        rows_per_file: 100,
        seed: 42,
    })
    .unwrap();

    let mut draft_content_ids = BTreeMap::new();
    let mut publish_from_draft_content_ids = Vec::new();
    let mut distinct_content_ids = BTreeMap::new();
    for path in walk_parquet_files(tempdir.path()) {
        let bytes = fs::read(&path).unwrap();
        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes)).unwrap();
        let reader = builder.build().unwrap();
        for batch in reader {
            let batch = batch.unwrap();
            let content_ids = batch
                .column_by_name("content_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let event_kinds = batch
                .column_by_name("event_kind")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let content_id = content_ids.value(row);
                assert_ulid(content_id);
                distinct_content_ids.insert(content_id.to_owned(), ());
                let kind = event_kinds.value(row);
                if kind == "INITIAL_DRAFT" {
                    draft_content_ids.insert(content_id.to_owned(), ());
                } else if kind == "PUBLISH_FROM_DRAFT" {
                    publish_from_draft_content_ids.push(content_id.to_owned());
                }
            }
        }
    }

    let targets = compute_activity_targets(2_000, 90);
    assert_eq!(
        publish_from_draft_content_ids.len(),
        targets.publish_from_draft as usize
    );
    for content_id in publish_from_draft_content_ids {
        assert!(
            draft_content_ids.contains_key(&content_id),
            "PUBLISH_FROM_DRAFT must share a ULID with its INITIAL_DRAFT: {content_id}"
        );
    }
    assert!(
        distinct_content_ids.len() < 2_000,
        "bulk seed should keep repeated content_id distribution for top contents"
    );
}

#[test]
fn bulk_seed_generates_both_unpublish_variants() {
    let events = generate_test_bulk_events(10_000);

    assert!(
        events
            .iter()
            .any(|event| event.event_kind.as_deref() == Some("UNPUBLISH_TO_DRAFT"))
    );
    assert!(
        events
            .iter()
            .any(|event| event.event_kind.as_deref() == Some("UNPUBLISH_TO_CLOSED"))
    );
    assert!(
        events
            .iter()
            .all(|event| event.event_kind.as_deref() != Some("UNPUBLISH"))
    );
    assert!(
        events
            .iter()
            .any(|event| event.event_kind.as_deref() == Some("ADD_DRAFT_TO_PUBLISHED"))
    );
    assert!(
        events
            .iter()
            .any(|event| { event.event_kind.as_deref() == Some("DISCARD_DRAFT_ON_PUBLISHED") })
    );
    assert!(
        events
            .iter()
            .any(|event| event.event_kind.as_deref() == Some("REOPEN_TO_DRAFT"))
    );
    assert!(
        events
            .iter()
            .any(|event| event.event_kind.as_deref() == Some("REPUBLISH_FROM_CLOSED"))
    );
}

#[test]
fn metric_lifecycle_draft_to_publish_days_stay_within_api_ranges() {
    let events = generate_test_bulk_events(50_000);

    for api in BULK_APIS {
        let mut durations = Vec::new();
        for publish in events.iter().filter(|event| {
            event.api.as_deref() == Some(*api)
                && event.event_kind.as_deref() == Some("PUBLISH_FROM_DRAFT")
        }) {
            let content_id = publish.content_id.as_deref().expect("publish content_id");
            assert_ulid(content_id);
            let draft = events
                .iter()
                .find(|event| {
                    event.content_id.as_deref() == Some(content_id)
                        && event.event_kind.as_deref() == Some("INITIAL_DRAFT")
                })
                .expect("draft event");
            durations.push(
                (publish.content_published_at.unwrap() - draft.draft_created_at.unwrap())
                    .num_days(),
            );
        }

        let min = *durations.iter().min().unwrap();
        let max = *durations.iter().max().unwrap();
        let base = api_timing_profile(api).draft_to_publish_base_days;
        assert!(
            min >= base && max <= base + 5,
            "api={api} expected {base}..={} days, got {min}..={max}",
            base + 5
        );
    }
}

#[test]
fn metric_lifecycle_events_vary_publish_and_draft_durations_by_api() {
    let events = generate_test_bulk_events(50_000);

    let blogs_lead = publish_lead_days_for_api(&events, "blogs");
    let pages_lead = publish_lead_days_for_api(&events, "pages");
    assert!(blogs_lead < pages_lead);

    let labels_draft_lead = draft_to_publish_days_for_api(&events, "labels");
    let papers_draft_lead = draft_to_publish_days_for_api(&events, "papers");
    assert!(labels_draft_lead < papers_draft_lead);
}

#[test]
fn publish_actions_follow_new_ratio_and_year_distribution() {
    let events = generate_test_bulk_events_for_days(50_000, 365);
    let today = jst_today();
    let start = today - Duration::days(364);
    let midpoint = start + Duration::days(182);
    let mut first_half = 0usize;
    let mut second_half = 0usize;
    let mut daily_counts: BTreeMap<NaiveDate, usize> = BTreeMap::new();

    for event in events {
        if !matches!(
            event.event_kind.as_deref(),
            Some("PUBLISH_FROM_DRAFT") | Some("INITIAL_PUBLISH") | Some("REPUBLISH_FROM_CLOSED")
        ) {
            continue;
        }
        let day = jst_date(event.received_at);
        *daily_counts.entry(day).or_default() += 1;
        if day <= midpoint {
            first_half += 1;
        } else {
            second_half += 1;
        }
    }

    let publish_actions = first_half + second_half;
    let average_daily = publish_actions as f64 / 365.0;
    let max_daily = daily_counts.values().copied().max().unwrap_or(0);
    let ratio = first_half as f64 / second_half as f64;
    assert_eq!(publish_actions, 1_825);
    assert!(
        (4.9..=5.1).contains(&average_daily),
        "publish actions should average around 5 per day, got {average_daily}"
    );
    assert!(
        max_daily <= 20,
        "publish actions should stay below the intended daily cap, got max_daily={max_daily}"
    );
    assert!(
        (0.80..=1.25).contains(&ratio),
        "publish actions should look natural across the full year, got first_half={first_half}, second_half={second_half}, ratio={ratio}"
    );
}

#[test]
fn bulk_seed_matches_new_event_kind_ratios() {
    let events = generate_test_bulk_events_for_days(50_000, 365);
    let targets = compute_activity_targets(50_000, 365);

    assert_eq!(
        count_events(&events, "INITIAL_DRAFT"),
        targets.initial_draft as usize
    );
    assert_eq!(
        count_events(&events, "SAVE_DRAFT"),
        targets.save_draft as usize
    );
    assert_eq!(
        count_events(&events, "PUBLISH_FROM_DRAFT"),
        targets.publish_from_draft as usize
    );
    assert_eq!(
        count_events(&events, "INITIAL_PUBLISH"),
        targets.initial_publish as usize
    );
    assert_eq!(
        count_events(&events, "UPDATE_PUBLISHED"),
        targets.update_published as usize
    );
    assert_eq!(
        count_events(&events, "ADD_DRAFT_TO_PUBLISHED"),
        targets.add_draft_to_published as usize
    );
    assert_eq!(
        count_events(&events, "DISCARD_DRAFT_ON_PUBLISHED"),
        targets.discard_draft_on_published as usize
    );
    assert_eq!(
        count_events(&events, "UNPUBLISH_TO_DRAFT"),
        targets.unpublish_to_draft as usize
    );
    assert_eq!(
        count_events(&events, "UNPUBLISH_TO_CLOSED"),
        targets.unpublish_to_closed as usize
    );
    assert_eq!(
        count_events(&events, "REOPEN_TO_DRAFT"),
        targets.reopen_to_draft as usize
    );
    assert_eq!(
        count_events(&events, "REPUBLISH_FROM_CLOSED"),
        targets.republish_from_closed as usize
    );
    assert_eq!(
        count_events(&events, "DELETE_DRAFT"),
        targets.delete_draft as usize
    );
    assert_eq!(
        count_events(&events, "DELETE_PUBLISHED"),
        targets.delete_published as usize
    );
    assert_eq!(
        count_events(&events, "DELETE_CLOSED"),
        targets.delete_closed as usize
    );
}

#[test]
fn bulk_seed_weights_api_activity_by_content_update_frequency() {
    let events = generate_test_bulk_events_for_days(50_000, 365);
    let counts = count_apis(&events);

    assert_api_share(&counts, "blogs", 0.235..=0.245);
    assert_api_share(&counts, "news", 0.175..=0.185);
    assert_api_share(&counts, "papers", 0.145..=0.155);
    assert_api_share(&counts, "pages", 0.125..=0.135);

    assert_api_share(&counts, "cards", 0.075..=0.085);
    assert_api_share(&counts, "tags", 0.055..=0.065);
    assert_api_share(&counts, "labels", 0.045..=0.055);
    assert_api_share(&counts, "advertisements", 0.045..=0.055);

    assert_api_share(&counts, "authors", 0.025..=0.035);
    assert_api_share(&counts, "categories", 0.025..=0.035);
}

fn generate_test_bulk_events(count: u32) -> Vec<NormalizedEvent> {
    generate_test_bulk_events_for_days(count, 90)
}

fn count_events(events: &[NormalizedEvent], event_kind: &str) -> usize {
    events
        .iter()
        .filter(|event| event.event_kind.as_deref() == Some(event_kind))
        .count()
}

fn count_apis(events: &[NormalizedEvent]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events {
        let api = event.api.as_deref().expect("bulk event api");
        *counts.entry(api.to_owned()).or_default() += 1;
    }
    counts
}

fn assert_api_share(
    counts: &BTreeMap<String, usize>,
    api: &str,
    expected: std::ops::RangeInclusive<f64>,
) {
    let total: usize = counts.values().sum();
    let actual = *counts.get(api).unwrap_or(&0) as f64 / total as f64;
    assert!(
        expected.contains(&actual),
        "api={api} expected share {expected:?}, got {actual:.4}"
    );
}

fn generate_test_bulk_events_for_days(count: u32, days: u32) -> Vec<NormalizedEvent> {
    let mut events = Vec::new();
    let mut rng = SeededRng::new(42);
    let end_date = jst_today();
    let start_date = end_date - Duration::days(i64::from(days - 1));
    let schedule = build_realistic_bulk_day_schedule(&mut rng, start_date, days);
    let targets = compute_activity_targets(count, days);
    generate_bulk_activity_events(
        &DebugSeedConfig {
            output_dir: PathBuf::new(),
            preset: DebugSeedPreset::Bulk,
            count,
            days,
            contents: 20,
            rows_per_file: 100,
            seed: 42,
        },
        &mut rng,
        &schedule,
        &targets,
        &mut events,
        &mut None,
        &mut None,
    )
    .unwrap();
    events
}

#[test]
fn realistic_bulk_day_schedule_covers_most_of_range_with_zero_days() {
    let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let schedule = build_realistic_bulk_day_schedule(&mut SeededRng::new(42), start, 365);
    assert!(schedule.days.len() <= 310);
    assert!(schedule.days.len() >= 274);
    let total_weight: u32 = schedule.days.iter().map(|day| day.weight).sum();
    assert!(total_weight > schedule.days.len() as u32);
}

fn publish_lead_days_for_api(events: &[NormalizedEvent], api: &str) -> i64 {
    let event = events
        .iter()
        .find(|event| {
            event.api.as_deref() == Some(api)
                && matches!(
                    event.event_kind.as_deref(),
                    Some("PUBLISH_FROM_DRAFT")
                        | Some("INITIAL_PUBLISH")
                        | Some("REPUBLISH_FROM_CLOSED")
                )
                && event.content_created_at.is_some()
                && event.content_published_at.is_some()
        })
        .expect("publish event");
    (event.content_published_at.unwrap() - event.content_created_at.unwrap()).num_days()
}

fn draft_to_publish_days_for_api(events: &[NormalizedEvent], api: &str) -> i64 {
    let content_id = first_publish_from_draft_content_id(events, api);
    let draft = events
        .iter()
        .find(|event| {
            event.content_id.as_deref() == Some(content_id.as_str())
                && event.event_kind.as_deref() == Some("INITIAL_DRAFT")
        })
        .expect("draft event");
    let publish = events
        .iter()
        .find(|event| {
            event.content_id.as_deref() == Some(content_id.as_str())
                && event.event_kind.as_deref() == Some("PUBLISH_FROM_DRAFT")
        })
        .expect("publish event");
    (publish.content_published_at.unwrap() - draft.draft_created_at.unwrap()).num_days()
}

fn first_publish_from_draft_content_id(events: &[NormalizedEvent], api: &str) -> String {
    events
        .iter()
        .find(|event| {
            event.api.as_deref() == Some(api)
                && event.event_kind.as_deref() == Some("PUBLISH_FROM_DRAFT")
        })
        .and_then(|event| event.content_id.clone())
        .unwrap_or_else(|| panic!("publish-from-draft pair for api={api}"))
}

fn assert_ulid(value: &str) {
    assert_eq!(value.len(), 26, "ULID must be 26 chars: {value}");
    assert!(
        value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'A'..=b'H' | b'J'..=b'K' | b'M'..=b'N' | b'P'..=b'T' | b'V'..=b'Z')),
        "ULID contains invalid Crockford Base32 chars: {value}"
    );
}

fn count_unique_partition_dates(root: &Path) -> usize {
    let mut dates = BTreeMap::new();
    for path in walk_parquet_files(root) {
        if let Some(rest) = path.to_string_lossy().split("/dt=").nth(1)
            && let Some(date) = rest.split('/').next()
        {
            dates.insert(date.to_owned(), ());
        }
    }
    dates.len()
}

fn walk_parquet_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    walk_parquet_files_inner(root, &mut files);
    files.sort();
    files
}

fn walk_parquet_files_inner(dir: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.is_dir() {
            walk_parquet_files_inner(&path, files);
        } else if path.extension().and_then(|value| value.to_str()) == Some("parquet") {
            files.push(path);
        }
    }
}

fn read_parquet_row_count(path: &Path, min_rows: usize) -> usize {
    let bytes = fs::read(path).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes)).unwrap();
    let mut reader = builder.build().unwrap();
    let batch = reader.next().unwrap().unwrap();
    assert!(batch.num_rows() >= min_rows);
    batch.num_rows()
}
