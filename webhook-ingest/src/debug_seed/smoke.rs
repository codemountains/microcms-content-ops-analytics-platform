use std::collections::BTreeMap;

use chrono::{DateTime, Duration, NaiveDate, NaiveTime, Utc};

use crate::{IngestError, build_s3_key, normalize_payload};

use super::EVENT_PREFIX;
use super::config::{DebugSeedConfig, DebugSeedSummary};
use super::fixture::smoke_fixture;
use super::io::{partition_dir_from_key, track_date, write_single_event_file};
use super::time::{jst_datetime, jst_today};

pub(super) const SMOKE_EVENT_IDS: [&str; 8] = [
    "018f1001-0000-7000-8000-000000000001",
    "018f1001-0000-7000-8000-000000000002",
    "018f1001-0000-7000-8000-000000000003",
    "018f1001-0000-7000-8000-000000000004",
    "018f1001-0000-7000-8000-000000000005",
    "018f1001-0000-7000-8000-000000000006",
    "018f1001-0000-7000-8000-000000000007",
    "018f1001-0000-7000-8000-000000000008",
];
pub(super) const SMOKE_BLOG_LIFECYCLE_CONTENT_ULID: &str = "01J1DVG0000000000000000001";
pub(super) const SMOKE_BLOG_DIRECT_CONTENT_ULID: &str = "01J1DVG0000000000000000002";

pub(super) fn generate_smoke_files(
    config: &DebugSeedConfig,
) -> Result<DebugSeedSummary, IngestError> {
    let event_date = jst_today() - Duration::days(2);
    let updated_before_date = event_date - Duration::days(1);
    let created_date = event_date - Duration::days(2);
    let draft_created_date = event_date - Duration::days(5);

    let fixtures = smoke_fixtures(
        event_date,
        updated_before_date,
        created_date,
        draft_created_date,
    );
    let mut min_dt = None;
    let mut max_dt = None;
    let mut partitions = BTreeMap::new();

    for (event_id, (body, received_at)) in SMOKE_EVENT_IDS.iter().zip(fixtures) {
        let event = normalize_payload(body.as_bytes(), received_at)?;
        track_date(&event, &mut min_dt, &mut max_dt);
        let service = event.service.as_deref().unwrap_or("unknown");
        let api = event.api.as_deref().unwrap_or("unknown");
        let key = build_s3_key(EVENT_PREFIX, service, api, event.received_at, event_id);
        let path = config.output_dir.join(&key);
        write_single_event_file(&path, &event)?;
        partitions.insert(partition_dir_from_key(&key), ());
    }

    Ok(DebugSeedSummary {
        event_count: 8,
        file_count: 8,
        partition_count: partitions.len(),
        min_dt,
        max_dt,
    })
}

pub(super) fn smoke_fixtures(
    event_date: NaiveDate,
    updated_before_date: NaiveDate,
    created_date: NaiveDate,
    draft_created_date: NaiveDate,
) -> Vec<(String, DateTime<Utc>)> {
    let dates = SmokeFixtureDates {
        event_date,
        updated_before_date,
        created_date,
        draft_created_date,
    };

    SMOKE_FIXTURE_CASES
        .iter()
        .map(|case| {
            smoke_fixture(
                case.api,
                case.content_id.map(SmokeContentId::as_ulid),
                case.event_type,
                case.old_status,
                case.new_status,
                case.received_at.resolve(dates),
                case.old_updated_at.map(|value| value.resolve(dates)),
                case.new_updated_at.map(|value| value.resolve(dates)),
                case.draft_created_at.map(|value| value.resolve(dates)),
                case.content_created_at.map(|value| value.resolve(dates)),
                case.content_published_at.map(|value| value.resolve(dates)),
            )
        })
        .collect()
}

#[derive(Debug, Clone, Copy)]
struct SmokeFixtureCase {
    api: &'static str,
    content_id: Option<SmokeContentId>,
    event_type: &'static str,
    old_status: Option<&'static str>,
    new_status: Option<&'static str>,
    received_at: SmokeDateTime,
    old_updated_at: Option<SmokeDateTime>,
    new_updated_at: Option<SmokeDateTime>,
    draft_created_at: Option<SmokeDateTime>,
    content_created_at: Option<SmokeDateTime>,
    content_published_at: Option<SmokeDateTime>,
}

#[derive(Debug, Clone, Copy)]
enum SmokeContentId {
    BlogLifecycle,
    BlogDirect,
}

impl SmokeContentId {
    const fn as_ulid(self) -> &'static str {
        match self {
            SmokeContentId::BlogLifecycle => SMOKE_BLOG_LIFECYCLE_CONTENT_ULID,
            SmokeContentId::BlogDirect => SMOKE_BLOG_DIRECT_CONTENT_ULID,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct SmokeFixtureDates {
    event_date: NaiveDate,
    updated_before_date: NaiveDate,
    created_date: NaiveDate,
    draft_created_date: NaiveDate,
}

#[derive(Debug, Clone, Copy)]
struct SmokeDateTime {
    date: SmokeDate,
    hour: u32,
    minute: u32,
}

impl SmokeDateTime {
    fn resolve(self, dates: SmokeFixtureDates) -> DateTime<Utc> {
        jst_datetime(
            self.date.resolve(dates),
            NaiveTime::from_hms_opt(self.hour, self.minute, 0).expect("valid smoke fixture time"),
        )
    }
}

#[derive(Debug, Clone, Copy)]
enum SmokeDate {
    Event,
    UpdatedBefore,
    Created,
    DraftCreated,
}

impl SmokeDate {
    fn resolve(self, dates: SmokeFixtureDates) -> NaiveDate {
        match self {
            SmokeDate::Event => dates.event_date,
            SmokeDate::UpdatedBefore => dates.updated_before_date,
            SmokeDate::Created => dates.created_date,
            SmokeDate::DraftCreated => dates.draft_created_date,
        }
    }
}

const fn at(date: SmokeDate, hour: u32, minute: u32) -> SmokeDateTime {
    SmokeDateTime { date, hour, minute }
}

const SMOKE_FIXTURE_CASES: &[SmokeFixtureCase] = &[
    SmokeFixtureCase {
        api: "blogs",
        content_id: Some(SmokeContentId::BlogLifecycle),
        event_type: "edit",
        old_status: Some("DRAFT"),
        new_status: Some("PUBLISH"),
        received_at: at(SmokeDate::Event, 12, 0),
        old_updated_at: Some(at(SmokeDate::UpdatedBefore, 12, 0)),
        new_updated_at: Some(at(SmokeDate::Event, 12, 0)),
        draft_created_at: None,
        content_created_at: Some(at(SmokeDate::Created, 12, 0)),
        content_published_at: Some(at(SmokeDate::Event, 12, 0)),
    },
    SmokeFixtureCase {
        api: "blogs",
        content_id: Some(SmokeContentId::BlogLifecycle),
        event_type: "new",
        old_status: None,
        new_status: Some("DRAFT"),
        received_at: at(SmokeDate::Event, 11, 0),
        old_updated_at: None,
        new_updated_at: Some(at(SmokeDate::Event, 11, 0)),
        draft_created_at: Some(at(SmokeDate::DraftCreated, 12, 0)),
        content_created_at: None,
        content_published_at: None,
    },
    SmokeFixtureCase {
        api: "blogs",
        content_id: Some(SmokeContentId::BlogLifecycle),
        event_type: "edit",
        old_status: Some("PUBLISH"),
        new_status: Some("PUBLISH"),
        received_at: at(SmokeDate::Event, 13, 0),
        old_updated_at: Some(at(SmokeDate::Event, 12, 0)),
        new_updated_at: Some(at(SmokeDate::Event, 13, 0)),
        draft_created_at: None,
        content_created_at: None,
        content_published_at: None,
    },
    SmokeFixtureCase {
        api: "blogs",
        content_id: None,
        event_type: "edit",
        old_status: Some("PUBLISH"),
        new_status: Some("DRAFT"),
        received_at: at(SmokeDate::Event, 15, 0),
        old_updated_at: Some(at(SmokeDate::Event, 14, 0)),
        new_updated_at: Some(at(SmokeDate::Event, 15, 0)),
        draft_created_at: None,
        content_created_at: None,
        content_published_at: None,
    },
    SmokeFixtureCase {
        api: "blogs",
        content_id: None,
        event_type: "edit",
        old_status: Some("PUBLISH"),
        new_status: Some("CLOSED"),
        received_at: at(SmokeDate::Event, 15, 30),
        old_updated_at: Some(at(SmokeDate::Event, 14, 30)),
        new_updated_at: Some(at(SmokeDate::Event, 15, 30)),
        draft_created_at: None,
        content_created_at: None,
        content_published_at: None,
    },
    SmokeFixtureCase {
        api: "blogs",
        content_id: None,
        event_type: "edit",
        old_status: Some("DRAFT"),
        new_status: Some("DRAFT"),
        received_at: at(SmokeDate::Event, 16, 0),
        old_updated_at: None,
        new_updated_at: Some(at(SmokeDate::Event, 16, 0)),
        draft_created_at: None,
        content_created_at: None,
        content_published_at: None,
    },
    SmokeFixtureCase {
        api: "blogs",
        content_id: Some(SmokeContentId::BlogDirect),
        event_type: "new",
        old_status: None,
        new_status: Some("PUBLISH"),
        received_at: at(SmokeDate::Event, 14, 0),
        old_updated_at: None,
        new_updated_at: Some(at(SmokeDate::Event, 14, 0)),
        draft_created_at: None,
        content_created_at: Some(at(SmokeDate::Event, 8, 0)),
        content_published_at: Some(at(SmokeDate::Event, 14, 0)),
    },
    SmokeFixtureCase {
        api: "authors",
        content_id: None,
        event_type: "delete",
        old_status: Some("PUBLISH"),
        new_status: None,
        received_at: at(SmokeDate::Event, 15, 0),
        old_updated_at: Some(at(SmokeDate::Event, 15, 0)),
        new_updated_at: None,
        draft_created_at: None,
        content_created_at: None,
        content_published_at: None,
    },
];
