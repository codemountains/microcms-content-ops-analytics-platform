use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Datelike, Duration, FixedOffset, NaiveDate, NaiveTime, TimeZone, Utc};

use crate::{
    IngestError, NormalizedEvent, build_s3_key, event_to_parquet, events_to_parquet,
    normalize_payload,
};

const EVENT_PREFIX: &str = "microcms_events";
const SERVICE_ID: &str = "example-service";
const BULK_APIS: &[&str] = &[
    "blogs",
    "authors",
    "news",
    "categories",
    "pages",
    "advertisements",
    "tags",
    "labels",
    "papers",
    "cards",
];
/// Share of calendar days that receive events in bulk seed data. Remaining days stay at zero.
const BULK_ACTIVE_DAY_DENSITY: f64 = 0.80;
const BULK_ACTIVITY_WEIGHT_TOTAL: u32 = 2_000;
const BULK_WEIGHT_INITIAL_DRAFT: u32 = 400;
const BULK_WEIGHT_SAVE_DRAFT: u32 = 300;
const BULK_WEIGHT_PUBLISH_FROM_DRAFT: u32 = 53;
const BULK_WEIGHT_INITIAL_PUBLISH: u32 = 12;
const BULK_WEIGHT_UPDATE_PUBLISHED: u32 = 377;
const BULK_WEIGHT_ADD_DRAFT_TO_PUBLISHED: u32 = 400;
const BULK_WEIGHT_DISCARD_DRAFT_ON_PUBLISHED: u32 = 100;
const BULK_WEIGHT_UNPUBLISH_TO_DRAFT: u32 = 160;
const BULK_WEIGHT_UNPUBLISH_TO_CLOSED: u32 = 80;
const BULK_WEIGHT_REOPEN_TO_DRAFT: u32 = 20;
const BULK_WEIGHT_REPUBLISH_FROM_CLOSED: u32 = 8;
const BULK_WEIGHT_DELETE_DRAFT: u32 = 20;
const BULK_WEIGHT_DELETE_PUBLISHED: u32 = 60;
const BULK_WEIGHT_DELETE_CLOSED: u32 = 10;
const SMOKE_EVENT_IDS: [&str; 8] = [
    "018f1001-0000-7000-8000-000000000001",
    "018f1001-0000-7000-8000-000000000002",
    "018f1001-0000-7000-8000-000000000003",
    "018f1001-0000-7000-8000-000000000004",
    "018f1001-0000-7000-8000-000000000005",
    "018f1001-0000-7000-8000-000000000006",
    "018f1001-0000-7000-8000-000000000007",
    "018f1001-0000-7000-8000-000000000008",
];

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

pub fn generate_debug_parquet_files(
    config: &DebugSeedConfig,
) -> Result<DebugSeedSummary, IngestError> {
    validate_config(config)?;
    prepare_output_events_dir(&config.output_dir)?;

    match config.preset {
        DebugSeedPreset::Smoke => generate_smoke_files(config),
        DebugSeedPreset::Bulk => generate_bulk_files(config),
    }
}

fn prepare_output_events_dir(output_dir: &Path) -> Result<(), IngestError> {
    fs::create_dir_all(output_dir).map_err(|error| IngestError::Parquet(error.to_string()))?;
    let events_root = output_dir.join(EVENT_PREFIX);
    if events_root.exists() {
        fs::remove_dir_all(&events_root)
            .map_err(|error| IngestError::Parquet(error.to_string()))?;
    }
    Ok(())
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

fn generate_smoke_files(config: &DebugSeedConfig) -> Result<DebugSeedSummary, IngestError> {
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

fn generate_bulk_files(config: &DebugSeedConfig) -> Result<DebugSeedSummary, IngestError> {
    let end_date = jst_today();
    let start_date = end_date - Duration::days(i64::from(config.days - 1));
    let mut rng = SeededRng::new(config.seed);
    let targets = compute_activity_targets(config.count, config.days);
    debug_assert_eq!(targets.total(), config.count);
    let schedule = build_realistic_bulk_day_schedule(&mut rng, start_date, config.days);
    let mut events = Vec::with_capacity(config.count as usize);
    let mut min_dt = None;
    let mut max_dt = None;

    generate_bulk_activity_events(
        config,
        &mut rng,
        &schedule,
        &targets,
        &mut events,
        &mut min_dt,
        &mut max_dt,
    )?;

    let mut partitions: BTreeMap<String, Vec<NormalizedEvent>> = BTreeMap::new();
    for event in events {
        let partition = partition_dir_for_event(&event);
        partitions.entry(partition).or_default().push(event);
    }

    let partition_count = partitions.len();
    let event_count = partitions.values().map(Vec::len).sum();
    let mut file_count = 0;
    for (partition, partition_events) in partitions {
        for (shard, chunk) in partition_events
            .chunks(config.rows_per_file as usize)
            .enumerate()
        {
            let path = config
                .output_dir
                .join(format!("{partition}/events-{shard:03}.parquet"));
            write_multi_event_file(&path, chunk)?;
            file_count += 1;
        }
    }

    Ok(DebugSeedSummary {
        event_count,
        file_count,
        partition_count,
        min_dt,
        max_dt,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ActivityTargets {
    initial_draft: u32,
    save_draft: u32,
    publish_from_draft: u32,
    initial_publish: u32,
    update_published: u32,
    add_draft_to_published: u32,
    discard_draft_on_published: u32,
    unpublish_to_draft: u32,
    unpublish_to_closed: u32,
    reopen_to_draft: u32,
    republish_from_closed: u32,
    delete_draft: u32,
    delete_published: u32,
    delete_closed: u32,
}

impl ActivityTargets {
    fn total(self) -> u32 {
        self.initial_draft
            + self.save_draft
            + self.publish_from_draft
            + self.initial_publish
            + self.update_published
            + self.add_draft_to_published
            + self.discard_draft_on_published
            + self.unpublish_to_draft
            + self.unpublish_to_closed
            + self.reopen_to_draft
            + self.republish_from_closed
            + self.delete_draft
            + self.delete_published
            + self.delete_closed
    }
}

fn compute_activity_targets(total: u32, _days: u32) -> ActivityTargets {
    let weights = [
        BULK_WEIGHT_INITIAL_DRAFT,
        BULK_WEIGHT_SAVE_DRAFT,
        BULK_WEIGHT_PUBLISH_FROM_DRAFT,
        BULK_WEIGHT_INITIAL_PUBLISH,
        BULK_WEIGHT_UPDATE_PUBLISHED,
        BULK_WEIGHT_ADD_DRAFT_TO_PUBLISHED,
        BULK_WEIGHT_DISCARD_DRAFT_ON_PUBLISHED,
        BULK_WEIGHT_UNPUBLISH_TO_DRAFT,
        BULK_WEIGHT_UNPUBLISH_TO_CLOSED,
        BULK_WEIGHT_REOPEN_TO_DRAFT,
        BULK_WEIGHT_REPUBLISH_FROM_CLOSED,
        BULK_WEIGHT_DELETE_DRAFT,
        BULK_WEIGHT_DELETE_PUBLISHED,
        BULK_WEIGHT_DELETE_CLOSED,
    ];
    let mut counts = [0_u32; 14];
    let mut assigned = 0_u32;
    for (index, weight) in weights.into_iter().enumerate() {
        counts[index] = total * weight / BULK_ACTIVITY_WEIGHT_TOTAL;
        assigned += counts[index];
    }
    counts[13] += total.saturating_sub(assigned);

    ActivityTargets {
        initial_draft: counts[0],
        save_draft: counts[1],
        publish_from_draft: counts[2],
        initial_publish: counts[3],
        update_published: counts[4],
        add_draft_to_published: counts[5],
        discard_draft_on_published: counts[6],
        unpublish_to_draft: counts[7],
        unpublish_to_closed: counts[8],
        reopen_to_draft: counts[9],
        republish_from_closed: counts[10],
        delete_draft: counts[11],
        delete_published: counts[12],
        delete_closed: counts[13],
    }
}

fn generate_bulk_activity_events(
    config: &DebugSeedConfig,
    rng: &mut SeededRng,
    schedule: &BulkDaySchedule,
    targets: &ActivityTargets,
    events: &mut Vec<NormalizedEvent>,
    min_dt: &mut Option<NaiveDate>,
    max_dt: &mut Option<NaiveDate>,
) -> Result<(), IngestError> {
    let orphan_drafts = targets
        .initial_draft
        .saturating_sub(targets.publish_from_draft);

    for pair_index in 0..targets.publish_from_draft {
        let api = BULK_APIS[pair_index as usize % BULK_APIS.len()];
        let content_id = format!("metric-{api}-{pair_index}");
        let publish_lead_days = api_publish_lead_days(api, pair_index);
        let draft_to_publish_days = api_draft_to_publish_days(api, pair_index);
        let publish_at = random_received_at_on_schedule(rng, schedule);
        let content_created_at = publish_at - Duration::days(publish_lead_days);
        let draft_created_at = publish_at - Duration::days(draft_to_publish_days);
        let publish_day = jst_date(publish_at);
        let eligible_draft_days: Vec<NaiveDate> = schedule
            .days
            .iter()
            .filter_map(|day| {
                (day.date <= publish_day
                    && !(publish_day == schedule.latest_date() && day.date == publish_day))
                    .then_some(day.date)
            })
            .collect();
        let draft_day = if eligible_draft_days.is_empty() {
            publish_day
        } else {
            eligible_draft_days[rng.next_usize(eligible_draft_days.len())]
        };
        let draft_received_at = jst_datetime(
            draft_day,
            NaiveTime::from_hms_opt(9 + (pair_index % 6), 15, 0).unwrap(),
        );

        push_bulk_event(
            events,
            min_dt,
            max_dt,
            api,
            &content_id,
            BulkTemplate::InitialDraft,
            draft_received_at,
            &BulkEventTiming {
                draft_created_at: Some(draft_created_at),
                content_created_at: None,
                content_published_at: None,
            },
        )?;
        push_bulk_event(
            events,
            min_dt,
            max_dt,
            api,
            &content_id,
            BulkTemplate::PublishFromDraft,
            publish_at,
            &BulkEventTiming {
                draft_created_at: None,
                content_created_at: Some(content_created_at),
                content_published_at: Some(publish_at),
            },
        )?;
    }

    for orphan_index in 0..orphan_drafts {
        let api = BULK_APIS[orphan_index as usize % BULK_APIS.len()];
        let content_id = format!("activity-draft-{orphan_index}");
        let received_at = random_received_at_on_schedule(rng, schedule);
        let draft_created_at = received_at - Duration::days(i64::from(orphan_index % 7 + 1));
        push_bulk_event(
            events,
            min_dt,
            max_dt,
            api,
            &content_id,
            BulkTemplate::InitialDraft,
            received_at,
            &BulkEventTiming {
                draft_created_at: Some(draft_created_at),
                content_created_at: None,
                content_published_at: None,
            },
        )?;
    }

    for draft_index in 0..targets.save_draft {
        let api = BULK_APIS[draft_index as usize % BULK_APIS.len()];
        let content_id = format!("activity-save-draft-{draft_index}");
        let received_at = random_received_at_on_schedule(rng, schedule);
        let draft_created_at = received_at - Duration::days(i64::from(draft_index % 10 + 1));
        push_bulk_event(
            events,
            min_dt,
            max_dt,
            api,
            &content_id,
            BulkTemplate::SaveDraft,
            received_at,
            &BulkEventTiming {
                draft_created_at: Some(draft_created_at),
                content_created_at: None,
                content_published_at: None,
            },
        )?;
    }

    for publish_index in 0..targets.initial_publish {
        let api = BULK_APIS[publish_index as usize % BULK_APIS.len()];
        let content_id = format!("metric-{api}-direct-{publish_index}");
        let publish_lead_days =
            api_publish_lead_days(api, publish_index) + i64::from(publish_index % 4);
        let publish_at = random_received_at_on_schedule(rng, schedule);
        let content_created_at = publish_at - Duration::days(publish_lead_days);
        push_bulk_event(
            events,
            min_dt,
            max_dt,
            api,
            &content_id,
            BulkTemplate::InitialPublish,
            publish_at,
            &BulkEventTiming {
                draft_created_at: None,
                content_created_at: Some(content_created_at),
                content_published_at: Some(publish_at),
            },
        )?;
    }

    let mut filler_templates = Vec::with_capacity(
        (targets.update_published
            + targets.add_draft_to_published
            + targets.discard_draft_on_published
            + targets.unpublish_to_draft
            + targets.unpublish_to_closed
            + targets.reopen_to_draft
            + targets.republish_from_closed
            + targets.delete_draft
            + targets.delete_published
            + targets.delete_closed) as usize,
    );
    filler_templates.extend(std::iter::repeat_n(
        BulkTemplate::UpdatePublished,
        targets.update_published as usize,
    ));
    filler_templates.extend(std::iter::repeat_n(
        BulkTemplate::AddDraftToPublished,
        targets.add_draft_to_published as usize,
    ));
    filler_templates.extend(std::iter::repeat_n(
        BulkTemplate::DiscardDraftOnPublished,
        targets.discard_draft_on_published as usize,
    ));
    filler_templates.extend(std::iter::repeat_n(
        BulkTemplate::UnpublishToDraft,
        targets.unpublish_to_draft as usize,
    ));
    filler_templates.extend(std::iter::repeat_n(
        BulkTemplate::UnpublishToClosed,
        targets.unpublish_to_closed as usize,
    ));
    filler_templates.extend(std::iter::repeat_n(
        BulkTemplate::ReopenToDraft,
        targets.reopen_to_draft as usize,
    ));
    filler_templates.extend(std::iter::repeat_n(
        BulkTemplate::RepublishFromClosed,
        targets.republish_from_closed as usize,
    ));
    filler_templates.extend(std::iter::repeat_n(
        BulkTemplate::DeleteDraft,
        targets.delete_draft as usize,
    ));
    filler_templates.extend(std::iter::repeat_n(
        BulkTemplate::DeletePublished,
        targets.delete_published as usize,
    ));
    filler_templates.extend(std::iter::repeat_n(
        BulkTemplate::DeleteClosed,
        targets.delete_closed as usize,
    ));
    shuffle_bulk_templates(rng, &mut filler_templates);

    for template in filler_templates {
        let received_at = random_received_at_on_schedule(rng, schedule);
        let api = BULK_APIS[rng.next_usize(BULK_APIS.len())];
        let content_id = pick_content_id(rng, config.contents);
        push_bulk_event(
            events,
            min_dt,
            max_dt,
            api,
            &content_id,
            template,
            received_at,
            &BulkEventTiming::for_filler(template),
        )?;
    }

    Ok(())
}

fn shuffle_bulk_templates(rng: &mut SeededRng, templates: &mut [BulkTemplate]) {
    for index in (1..templates.len()).rev() {
        let swap_index = rng.next_usize(index + 1);
        templates.swap(index, swap_index);
    }
}

#[allow(clippy::too_many_arguments)]
fn push_bulk_event(
    events: &mut Vec<NormalizedEvent>,
    min_dt: &mut Option<NaiveDate>,
    max_dt: &mut Option<NaiveDate>,
    api: &str,
    content_id: &str,
    template: BulkTemplate,
    received_at: DateTime<Utc>,
    timing: &BulkEventTiming,
) -> Result<(), IngestError> {
    let body = build_bulk_webhook_body(api, content_id, template, received_at, timing);
    let event = normalize_payload(body.as_bytes(), received_at)?;
    track_date(&event, min_dt, max_dt);
    events.push(event);
    Ok(())
}

fn smoke_fixtures(
    event_date: NaiveDate,
    updated_before_date: NaiveDate,
    created_date: NaiveDate,
    draft_created_date: NaiveDate,
) -> Vec<(String, DateTime<Utc>)> {
    vec![
        smoke_fixture(
            "blogs",
            Some("content-1"),
            "edit",
            Some("DRAFT"),
            Some("PUBLISH"),
            jst_datetime(event_date, NaiveTime::from_hms_opt(12, 0, 0).unwrap()),
            Some(jst_datetime(
                updated_before_date,
                NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
            )),
            Some(jst_datetime(
                event_date,
                NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
            )),
            None,
            Some(jst_datetime(
                created_date,
                NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
            )),
            Some(jst_datetime(
                event_date,
                NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
            )),
        ),
        smoke_fixture(
            "blogs",
            Some("content-1"),
            "new",
            None,
            Some("DRAFT"),
            jst_datetime(event_date, NaiveTime::from_hms_opt(11, 0, 0).unwrap()),
            None,
            Some(jst_datetime(
                event_date,
                NaiveTime::from_hms_opt(11, 0, 0).unwrap(),
            )),
            Some(jst_datetime(
                draft_created_date,
                NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
            )),
            None,
            None,
        ),
        smoke_fixture(
            "blogs",
            Some("content-1"),
            "edit",
            Some("PUBLISH"),
            Some("PUBLISH"),
            jst_datetime(event_date, NaiveTime::from_hms_opt(13, 0, 0).unwrap()),
            Some(jst_datetime(
                event_date,
                NaiveTime::from_hms_opt(12, 0, 0).unwrap(),
            )),
            Some(jst_datetime(
                event_date,
                NaiveTime::from_hms_opt(13, 0, 0).unwrap(),
            )),
            None,
            None,
            None,
        ),
        smoke_fixture(
            "blogs",
            None,
            "edit",
            Some("PUBLISH"),
            Some("DRAFT"),
            jst_datetime(event_date, NaiveTime::from_hms_opt(15, 0, 0).unwrap()),
            Some(jst_datetime(
                event_date,
                NaiveTime::from_hms_opt(14, 0, 0).unwrap(),
            )),
            Some(jst_datetime(
                event_date,
                NaiveTime::from_hms_opt(15, 0, 0).unwrap(),
            )),
            None,
            None,
            None,
        ),
        smoke_fixture(
            "blogs",
            None,
            "edit",
            Some("PUBLISH"),
            Some("CLOSED"),
            jst_datetime(event_date, NaiveTime::from_hms_opt(15, 30, 0).unwrap()),
            Some(jst_datetime(
                event_date,
                NaiveTime::from_hms_opt(14, 30, 0).unwrap(),
            )),
            Some(jst_datetime(
                event_date,
                NaiveTime::from_hms_opt(15, 30, 0).unwrap(),
            )),
            None,
            None,
            None,
        ),
        smoke_fixture(
            "blogs",
            None,
            "edit",
            Some("DRAFT"),
            Some("DRAFT"),
            jst_datetime(event_date, NaiveTime::from_hms_opt(16, 0, 0).unwrap()),
            None,
            Some(jst_datetime(
                event_date,
                NaiveTime::from_hms_opt(16, 0, 0).unwrap(),
            )),
            None,
            None,
            None,
        ),
        smoke_fixture(
            "blogs",
            Some("content-2"),
            "new",
            None,
            Some("PUBLISH"),
            jst_datetime(event_date, NaiveTime::from_hms_opt(14, 0, 0).unwrap()),
            None,
            Some(jst_datetime(
                event_date,
                NaiveTime::from_hms_opt(14, 0, 0).unwrap(),
            )),
            None,
            Some(jst_datetime(
                event_date,
                NaiveTime::from_hms_opt(8, 0, 0).unwrap(),
            )),
            Some(jst_datetime(
                event_date,
                NaiveTime::from_hms_opt(14, 0, 0).unwrap(),
            )),
        ),
        smoke_fixture(
            "authors",
            None,
            "delete",
            Some("PUBLISH"),
            None,
            jst_datetime(event_date, NaiveTime::from_hms_opt(15, 0, 0).unwrap()),
            Some(jst_datetime(
                event_date,
                NaiveTime::from_hms_opt(15, 0, 0).unwrap(),
            )),
            None,
            None,
            None,
            None,
        ),
    ]
}

#[allow(clippy::too_many_arguments)]
fn smoke_fixture(
    api: &str,
    content_id: Option<&str>,
    event_type: &str,
    old_status: Option<&str>,
    new_status: Option<&str>,
    received_at: DateTime<Utc>,
    old_updated_at: Option<DateTime<Utc>>,
    new_updated_at: Option<DateTime<Utc>>,
    draft_created_at: Option<DateTime<Utc>>,
    content_created_at: Option<DateTime<Utc>>,
    content_published_at: Option<DateTime<Utc>>,
) -> (String, DateTime<Utc>) {
    let old = old_status.map(|status| {
        format!(
            r#"{{"status":["{status}"],"updatedAt":"{}"}}"#,
            old_updated_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| received_at.to_rfc3339())
        )
    });
    let new = new_status.map(|status| {
        let mut fields = vec![
            format!(r#""status":["{status}"]"#),
            format!(
                r#""updatedAt":"{}""#,
                new_updated_at
                    .map(|value| value.to_rfc3339())
                    .unwrap_or_else(|| received_at.to_rfc3339())
            ),
        ];
        if let Some(value) = draft_created_at {
            fields.push(format!(
                r#""draftValue":{{"createdAt":"{}"}}"#,
                value.to_rfc3339()
            ));
        }
        if content_created_at.is_some() || content_published_at.is_some() {
            let created = content_created_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| received_at.to_rfc3339());
            let published = content_published_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| received_at.to_rfc3339());
            fields.push(format!(
                r#""publishValue":{{"createdAt":"{created}","publishedAt":"{published}"}}"#
            ));
        }
        format!("{{{}}}", fields.join(","))
    });

    let id_field = match content_id {
        Some(content_id) => format!(r#""id": "{content_id}","#),
        None => String::new(),
    };

    let body = format!(
        r#"{{
          "service": "{SERVICE_ID}",
          "api": "{api}",
          {id_field}
          "type": "{event_type}",
          "contents": {{
            "old": {},
            "new": {}
          }}
        }}"#,
        old.as_deref().unwrap_or("null"),
        new.as_deref().unwrap_or("null")
    );

    (body, received_at)
}

#[derive(Debug, Clone, Copy)]
enum BulkTemplate {
    InitialDraft,
    SaveDraft,
    PublishFromDraft,
    InitialPublish,
    UpdatePublished,
    AddDraftToPublished,
    DiscardDraftOnPublished,
    UnpublishToDraft,
    UnpublishToClosed,
    ReopenToDraft,
    RepublishFromClosed,
    DeleteDraft,
    DeletePublished,
    DeleteClosed,
}

#[derive(Debug, Clone, Copy)]
struct BulkEventTiming {
    draft_created_at: Option<DateTime<Utc>>,
    content_created_at: Option<DateTime<Utc>>,
    content_published_at: Option<DateTime<Utc>>,
}

impl BulkEventTiming {
    fn for_filler(_template: BulkTemplate) -> Self {
        Self {
            draft_created_at: None,
            content_created_at: None,
            content_published_at: None,
        }
    }
}

#[derive(Debug, Clone)]
struct BulkDaySchedule {
    days: Vec<WeightedBulkDay>,
}

impl BulkDaySchedule {
    fn latest_date(&self) -> NaiveDate {
        self.days.last().expect("non-empty schedule").date
    }
}

#[derive(Debug, Clone)]
struct WeightedBulkDay {
    date: NaiveDate,
    weight: u32,
}

#[derive(Debug)]
struct BulkDayCandidate {
    date: NaiveDate,
    weight: u32,
    active_score: u32,
}

fn api_publish_lead_days(api: &str, index: u32) -> i64 {
    let base = match api {
        "blogs" => 1,
        "authors" => 2,
        "news" => 4,
        "categories" => 3,
        "pages" => 8,
        "advertisements" => 5,
        "tags" => 1,
        "labels" => 1,
        "papers" => 14,
        "cards" => 4,
        _ => 5,
    };
    base + i64::from(index % 5)
}

fn api_draft_to_publish_days(api: &str, index: u32) -> i64 {
    let base = match api {
        "blogs" => 4,
        "authors" => 2,
        "news" => 10,
        "categories" => 6,
        "pages" => 18,
        "advertisements" => 7,
        "tags" => 1,
        "labels" => 1,
        "papers" => 24,
        "cards" => 5,
        _ => 8,
    };
    base + i64::from(index % 6)
}

fn build_realistic_bulk_day_schedule(
    rng: &mut SeededRng,
    start_date: NaiveDate,
    days: u32,
) -> BulkDaySchedule {
    let mut candidates = Vec::with_capacity(days as usize);
    let end_date = start_date + Duration::days(i64::from(days - 1));
    for offset in 0..days {
        let date = start_date + Duration::days(i64::from(offset));
        let weekly_weight = weekly_activity_weight(date);
        let campaign_weight = campaign_activity_weight(date);
        let noise = rng.next_u32(35);
        let weight = weekly_weight + campaign_weight + noise;
        candidates.push(BulkDayCandidate {
            date,
            weight,
            active_score: weekly_weight + campaign_weight * 2 + rng.next_u32(80),
        });
    }

    let zero_days = ((f64::from(days) * (1.0 - BULK_ACTIVE_DAY_DENSITY)).round() as usize)
        .min(candidates.len().saturating_sub(1));
    candidates.sort_by_key(|day| (day.active_score, day.date));
    let mut active_candidates = candidates.split_off(zero_days);
    if !active_candidates.iter().any(|day| day.date == end_date)
        && let Some(end_index) = candidates.iter().position(|day| day.date == end_date)
    {
        let end_candidate = candidates.remove(end_index);
        active_candidates.push(end_candidate);
        if let Some(remove_index) = active_candidates
            .iter()
            .position(|day| day.date != end_date)
        {
            active_candidates.remove(remove_index);
        }
    }
    active_candidates.sort_by_key(|day| day.date);

    let schedule_days: Vec<WeightedBulkDay> = active_candidates
        .into_iter()
        .map(|day| WeightedBulkDay {
            date: day.date,
            weight: day.weight.max(1),
        })
        .collect();
    BulkDaySchedule {
        days: schedule_days,
    }
}

fn weekly_activity_weight(date: NaiveDate) -> u32 {
    match date.weekday().number_from_monday() {
        1 => 105,
        2 => 120,
        3 => 115,
        4 => 110,
        5 => 90,
        6 => 35,
        _ => 25,
    }
}

fn campaign_activity_weight(date: NaiveDate) -> u32 {
    match date.day() {
        4..=6 | 14..=16 | 24..=26 => 80,
        9 | 10 | 19 | 20 => 35,
        _ => 0,
    }
}

/// Filler templates exclude INITIAL_DRAFT / PUBLISH_FROM_DRAFT pairs so draft-to-publish
/// metrics stay driven by coordinated metric lifecycles only.
fn build_bulk_webhook_body(
    api: &str,
    content_id: &str,
    template: BulkTemplate,
    received_at: DateTime<Utc>,
    timing: &BulkEventTiming,
) -> String {
    let timestamp = received_at.to_rfc3339();
    let day_before = (received_at - Duration::days(1)).to_rfc3339();
    let draft_created_at = timing
        .draft_created_at
        .unwrap_or(received_at - Duration::days(7))
        .to_rfc3339();
    let content_created_at = timing
        .content_created_at
        .unwrap_or(received_at - Duration::days(7))
        .to_rfc3339();
    let content_published_at = timing
        .content_published_at
        .unwrap_or(received_at)
        .to_rfc3339();

    match template {
        BulkTemplate::InitialDraft => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"new",
              "contents":{{
                "old":null,
                "new":{{
                  "status":["DRAFT"],
                  "updatedAt":"{timestamp}",
                  "draftValue":{{"createdAt":"{draft_created_at}"}}
                }}
              }}
            }}"#
        ),
        BulkTemplate::SaveDraft => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"edit",
              "contents":{{
                "old":{{"status":["DRAFT"],"updatedAt":"{day_before}"}},
                "new":{{
                  "status":["DRAFT"],
                  "updatedAt":"{timestamp}",
                  "draftValue":{{"createdAt":"{draft_created_at}"}}
                }}
              }}
            }}"#
        ),
        BulkTemplate::InitialPublish => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"new",
              "contents":{{
                "old":null,
                "new":{{
                  "status":["PUBLISH"],
                  "updatedAt":"{timestamp}",
                  "publishValue":{{"createdAt":"{content_created_at}","publishedAt":"{content_published_at}"}}
                }}
              }}
            }}"#
        ),
        BulkTemplate::PublishFromDraft => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"edit",
              "contents":{{
                "old":{{"status":["DRAFT"],"updatedAt":"{day_before}"}},
                "new":{{
                  "status":["PUBLISH"],
                  "updatedAt":"{timestamp}",
                  "publishValue":{{"createdAt":"{content_created_at}","publishedAt":"{content_published_at}"}}
                }}
              }}
            }}"#
        ),
        BulkTemplate::UpdatePublished => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"edit",
              "contents":{{
                "old":{{"status":["PUBLISH"],"updatedAt":"{day_before}"}},
                "new":{{"status":["PUBLISH"],"updatedAt":"{timestamp}"}}
              }}
            }}"#
        ),
        BulkTemplate::AddDraftToPublished => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"edit",
              "contents":{{
                "old":{{"status":["PUBLISH"],"updatedAt":"{day_before}"}},
                "new":{{
                  "status":["PUBLISH","DRAFT"],
                  "updatedAt":"{timestamp}",
                  "draftKey":"debug-draft-key",
                  "publishValue":{{"createdAt":"{content_created_at}","publishedAt":"{content_published_at}"}},
                  "draftValue":{{"createdAt":"{draft_created_at}"}}
                }}
              }}
            }}"#
        ),
        BulkTemplate::DiscardDraftOnPublished => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"edit",
              "contents":{{
                "old":{{
                  "status":["PUBLISH","DRAFT"],
                  "updatedAt":"{day_before}",
                  "draftKey":"debug-draft-key",
                  "publishValue":{{"createdAt":"{content_created_at}","publishedAt":"{content_published_at}"}},
                  "draftValue":{{"createdAt":"{draft_created_at}"}}
                }},
                "new":{{
                  "status":["PUBLISH"],
                  "updatedAt":"{timestamp}",
                  "draftKey":null,
                  "publishValue":{{"createdAt":"{content_created_at}","publishedAt":"{content_published_at}"}},
                  "draftValue":null
                }}
              }}
            }}"#
        ),
        BulkTemplate::UnpublishToDraft => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"edit",
              "contents":{{
                "old":{{"status":["PUBLISH"],"updatedAt":"{day_before}"}},
                "new":{{
                  "status":["DRAFT"],
                  "updatedAt":"{timestamp}",
                  "draftKey":"debug-draft-key",
                  "publishValue":null,
                  "draftValue":{{"createdAt":"{draft_created_at}"}}
                }}
              }}
            }}"#
        ),
        BulkTemplate::UnpublishToClosed => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"edit",
              "contents":{{
                "old":{{"status":["PUBLISH"],"updatedAt":"{day_before}"}},
                "new":{{
                  "status":["CLOSED"],
                  "updatedAt":"{timestamp}",
                  "draftKey":null,
                  "publishValue":null,
                  "draftValue":null
                }}
              }}
            }}"#
        ),
        BulkTemplate::ReopenToDraft => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"edit",
              "contents":{{
                "old":{{"status":["CLOSED"],"updatedAt":"{day_before}"}},
                "new":{{
                  "status":["DRAFT"],
                  "updatedAt":"{timestamp}",
                  "draftKey":"debug-draft-key",
                  "publishValue":null,
                  "draftValue":{{"createdAt":"{draft_created_at}"}}
                }}
              }}
            }}"#
        ),
        BulkTemplate::RepublishFromClosed => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"edit",
              "contents":{{
                "old":{{"status":["CLOSED"],"updatedAt":"{day_before}"}},
                "new":{{
                  "status":["PUBLISH"],
                  "updatedAt":"{timestamp}",
                  "publishValue":{{"createdAt":"{content_created_at}","publishedAt":"{content_published_at}"}}
                }}
              }}
            }}"#
        ),
        BulkTemplate::DeleteDraft => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"delete",
              "contents":{{
                "old":{{"status":["DRAFT"],"updatedAt":"{timestamp}"}},
                "new":null
              }}
            }}"#
        ),
        BulkTemplate::DeletePublished => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"delete",
              "contents":{{
                "old":{{"status":["PUBLISH"],"updatedAt":"{timestamp}"}},
                "new":null
              }}
            }}"#
        ),
        BulkTemplate::DeleteClosed => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"delete",
              "contents":{{
                "old":{{"status":["CLOSED"],"updatedAt":"{timestamp}"}},
                "new":null
              }}
            }}"#
        ),
    }
}

fn write_single_event_file(path: &Path, event: &NormalizedEvent) -> Result<(), IngestError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| IngestError::Parquet(error.to_string()))?;
    }
    let parquet = event_to_parquet(event)?;
    fs::write(path, parquet).map_err(|error| IngestError::Parquet(error.to_string()))
}

fn write_multi_event_file(path: &Path, events: &[NormalizedEvent]) -> Result<(), IngestError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| IngestError::Parquet(error.to_string()))?;
    }
    let parquet = events_to_parquet(events)?;
    fs::write(path, parquet).map_err(|error| IngestError::Parquet(error.to_string()))
}

fn partition_dir_for_event(event: &NormalizedEvent) -> String {
    let service = event.service.as_deref().unwrap_or("unknown");
    let api = event.api.as_deref().unwrap_or("unknown");
    let key = build_s3_key(EVENT_PREFIX, service, api, event.received_at, "placeholder");
    partition_dir_from_key(&key)
}

fn partition_dir_from_key(key: &str) -> String {
    key.rsplit_once('/')
        .map(|(dir, _)| dir.to_owned())
        .unwrap_or_else(|| key.to_owned())
}

fn track_date(
    event: &NormalizedEvent,
    min_dt: &mut Option<NaiveDate>,
    max_dt: &mut Option<NaiveDate>,
) {
    let date = jst_date(event.received_at);
    *min_dt = Some(min_dt.map_or(date, |current| current.min(date)));
    *max_dt = Some(max_dt.map_or(date, |current| current.max(date)));
}

fn jst_today() -> NaiveDate {
    jst_date(Utc::now())
}

fn jst_date(value: DateTime<Utc>) -> NaiveDate {
    let jst = jst_offset();
    value.with_timezone(&jst).date_naive()
}

fn jst_datetime(date: NaiveDate, time: NaiveTime) -> DateTime<Utc> {
    let jst = jst_offset();
    jst.from_local_datetime(&date.and_time(time))
        .single()
        .expect("valid JST datetime")
        .with_timezone(&Utc)
}

fn jst_offset() -> FixedOffset {
    FixedOffset::east_opt(9 * 60 * 60).expect("valid JST offset")
}

fn random_received_at_on_schedule(
    rng: &mut SeededRng,
    schedule: &BulkDaySchedule,
) -> DateTime<Utc> {
    let date = weighted_random_day(rng, schedule);
    let hour = realistic_event_hour(rng, date);
    let minute = rng.next_u32(60);
    let second = rng.next_u32(60);
    jst_datetime(
        date,
        NaiveTime::from_hms_opt(hour, minute, second).expect("valid time"),
    )
}

fn weighted_random_day(rng: &mut SeededRng, schedule: &BulkDaySchedule) -> NaiveDate {
    let total_weight: u64 = schedule.days.iter().map(|day| u64::from(day.weight)).sum();

    if total_weight == 0 {
        return schedule.latest_date();
    }

    let mut remaining = rng.next_u64() % total_weight;
    for day in &schedule.days {
        let weight = u64::from(day.weight);
        if remaining < weight {
            return day.date;
        }
        remaining -= weight;
    }

    schedule.latest_date()
}

fn realistic_event_hour(rng: &mut SeededRng, date: NaiveDate) -> u32 {
    let sample = rng.next_u32(100);
    let is_weekday = date.weekday().number_from_monday() <= 5;
    if is_weekday {
        match sample {
            0..=74 => 9 + rng.next_u32(9),
            75..=89 => 18 + rng.next_u32(4),
            _ => rng.next_u32(24),
        }
    } else {
        match sample {
            0..=69 => 10 + rng.next_u32(8),
            70..=84 => 18 + rng.next_u32(3),
            _ => rng.next_u32(24),
        }
    }
}

fn pick_content_id(rng: &mut SeededRng, contents: u32) -> String {
    let raw = rng.next_u64() as f64 / u64::MAX as f64;
    let biased = raw * raw;
    let index = (biased * f64::from(contents - 1)).round() as u32;
    format!("content-{index}")
}

struct SeededRng {
    state: u64,
}

impl SeededRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        self.state
    }

    fn next_u32(&mut self, upper_exclusive: u32) -> u32 {
        (self.next_u64() % u64::from(upper_exclusive)) as u32
    }

    fn next_usize(&mut self, upper_exclusive: usize) -> usize {
        (self.next_u64() as usize) % upper_exclusive
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use arrow_array::StringArray;
    use bytes::Bytes;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use tempfile::tempdir;

    use super::*;

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
        assert_eq!(events[0].content_id.as_deref(), Some("content-1"));
        assert_eq!(events[6].content_id.as_deref(), Some("content-2"));
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
    fn bulk_filler_does_not_emit_publish_lifecycle_event_kinds() {
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

        let mut filler_lifecycle_kinds = 0usize;
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
                    if content_id.starts_with("metric-")
                        || content_id.starts_with("activity-draft-")
                    {
                        continue;
                    }
                    let kind = event_kinds.value(row);
                    if matches!(kind, "INITIAL_DRAFT" | "PUBLISH_FROM_DRAFT") {
                        filler_lifecycle_kinds += 1;
                    }
                }
            }
        }

        assert_eq!(
            filler_lifecycle_kinds, 0,
            "filler must not emit publish lifecycle events that skew Grafana metrics"
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
        let targets = compute_activity_targets(50_000, 90);

        for api in BULK_APIS {
            let mut durations = Vec::new();
            for pair_index in 0..targets.publish_from_draft {
                if BULK_APIS[pair_index as usize % BULK_APIS.len()] != *api {
                    continue;
                }
                let content_id = format!("metric-{api}-{pair_index}");
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
                durations.push(
                    (publish.content_published_at.unwrap() - draft.draft_created_at.unwrap())
                        .num_days(),
                );
            }

            let min = *durations.iter().min().unwrap();
            let max = *durations.iter().max().unwrap();
            let base = match *api {
                "blogs" => 4,
                "authors" => 2,
                "news" => 10,
                "categories" => 6,
                "pages" => 18,
                "advertisements" => 7,
                "tags" => 1,
                "labels" => 1,
                "papers" => 24,
                "cards" => 5,
                _ => 8,
            };
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
                Some("PUBLISH_FROM_DRAFT")
                    | Some("INITIAL_PUBLISH")
                    | Some("REPUBLISH_FROM_CLOSED")
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

    fn generate_test_bulk_events(count: u32) -> Vec<NormalizedEvent> {
        generate_test_bulk_events_for_days(count, 90)
    }

    fn count_events(events: &[NormalizedEvent], event_kind: &str) -> usize {
        events
            .iter()
            .filter(|event| event.event_kind.as_deref() == Some(event_kind))
            .count()
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
        let content_id = first_metric_pair_content_id(events, api);
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

    fn first_metric_pair_content_id(events: &[NormalizedEvent], api: &str) -> String {
        events
            .iter()
            .find(|event| {
                event.api.as_deref() == Some(api)
                    && event.event_kind.as_deref() == Some("PUBLISH_FROM_DRAFT")
                    && event
                        .content_id
                        .as_deref()
                        .is_some_and(|content_id| content_id.starts_with("metric-"))
                    && event
                        .content_id
                        .as_deref()
                        .is_some_and(|content_id| !content_id.contains("-direct-"))
            })
            .and_then(|event| event.content_id.clone())
            .unwrap_or_else(|| panic!("metric pair for api={api}"))
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
}
