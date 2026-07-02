use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Duration, FixedOffset, NaiveDate, NaiveTime, TimeZone, Utc};

use crate::{
    IngestError, NormalizedEvent, build_s3_key, event_to_parquet, events_to_parquet,
    normalize_payload,
};

const EVENT_PREFIX: &str = "microcms_events";
const SERVICE_ID: &str = "example-service";
const BULK_APIS: &[&str] = &["blogs", "authors", "news", "categories", "pages"];
/// Share of calendar days that receive filler events. Remaining days stay at zero in heatmap.
const BULK_ACTIVE_DAY_DENSITY: f64 = 0.55;
const METRIC_CONTENTS_PER_API: u32 = 12;
const METRIC_PUBLISH_WINDOW_DAYS: i64 = 30;
const SMOKE_EVENT_IDS: [&str; 7] = [
    "018f1001-0000-7000-8000-000000000001",
    "018f1001-0000-7000-8000-000000000002",
    "018f1001-0000-7000-8000-000000000003",
    "018f1001-0000-7000-8000-000000000004",
    "018f1001-0000-7000-8000-000000000005",
    "018f1001-0000-7000-8000-000000000006",
    "018f1001-0000-7000-8000-000000000007",
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
    fs::create_dir_all(&config.output_dir)
        .map_err(|error| IngestError::Parquet(error.to_string()))?;

    match config.preset {
        DebugSeedPreset::Smoke => generate_smoke_files(config),
        DebugSeedPreset::Bulk => generate_bulk_files(config),
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
        event_count: 7,
        file_count: 7,
        partition_count: partitions.len(),
        min_dt,
        max_dt,
    })
}

fn generate_bulk_files(config: &DebugSeedConfig) -> Result<DebugSeedSummary, IngestError> {
    let end_date = jst_today();
    let start_date = end_date - Duration::days(i64::from(config.days - 1));
    let mut rng = SeededRng::new(config.seed);
    let mut events = Vec::with_capacity(config.count as usize);
    let mut min_dt = None;
    let mut max_dt = None;

    generate_bulk_metric_lifecycle_events(end_date, &mut events, &mut min_dt, &mut max_dt)?;

    let active_days = build_sparse_active_days(&mut rng, start_date, config.days);
    let filler_count = config.count.saturating_sub(events.len() as u32);
    for _ in 0..filler_count {
        let received_at = random_received_at_on_days(&mut rng, &active_days);
        let api = BULK_APIS[rng.next_usize(BULK_APIS.len())];
        let content_id = pick_content_id(&mut rng, config.contents);
        let template = pick_bulk_template(&mut rng);
        let timing = BulkEventTiming::for_filler(template);
        push_bulk_event(
            &mut events,
            &mut min_dt,
            &mut max_dt,
            api,
            &content_id,
            template,
            received_at,
            &timing,
        )?;
    }

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

fn generate_bulk_metric_lifecycle_events(
    end_date: NaiveDate,
    events: &mut Vec<NormalizedEvent>,
    min_dt: &mut Option<NaiveDate>,
    max_dt: &mut Option<NaiveDate>,
) -> Result<(), IngestError> {
    let recent_start = end_date - Duration::days(METRIC_PUBLISH_WINDOW_DAYS - 1);

    for api in BULK_APIS {
        for index in 0..METRIC_CONTENTS_PER_API {
            let content_id = format!("metric-{api}-{index}");
            let publish_lead_days = api_publish_lead_days(api, index);
            let draft_to_publish_days = api_draft_to_publish_days(api, index);
            let publish_day = recent_start
                + Duration::days(
                    (i64::from(index) * METRIC_PUBLISH_WINDOW_DAYS)
                        / i64::from(METRIC_CONTENTS_PER_API),
                );
            let publish_at = jst_datetime(
                publish_day,
                NaiveTime::from_hms_opt(10 + (index % 8), (index * 5) % 60, 0).unwrap(),
            );
            let content_created_at = publish_at - Duration::days(publish_lead_days);
            let draft_created_at = publish_at - Duration::days(draft_to_publish_days);
            let draft_received_at = jst_datetime(
                jst_date(draft_created_at),
                NaiveTime::from_hms_opt(9 + (index % 6), 15, 0).unwrap(),
            );

            push_bulk_event(
                events,
                min_dt,
                max_dt,
                api,
                &content_id,
                BulkTemplate::CreateDraft,
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
                BulkTemplate::FirstPublish,
                publish_at,
                &BulkEventTiming {
                    draft_created_at: None,
                    content_created_at: Some(content_created_at),
                    content_published_at: Some(publish_at),
                },
            )?;

            if index.is_multiple_of(3) {
                let direct_publish_lead = publish_lead_days + i64::from(index % 4) + 1;
                let direct_created_at = publish_at - Duration::days(direct_publish_lead);
                push_bulk_event(
                    events,
                    min_dt,
                    max_dt,
                    api,
                    &format!("{content_id}-direct"),
                    BulkTemplate::CreatePublish,
                    publish_at + Duration::hours(1),
                    &BulkEventTiming {
                        draft_created_at: None,
                        content_created_at: Some(direct_created_at),
                        content_published_at: Some(publish_at + Duration::hours(1)),
                    },
                )?;
            }
        }
    }

    Ok(())
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
    CreateDraft,
    CreatePublish,
    FirstPublish,
    UpdatePublish,
    Unpublish,
    Delete,
    UnclassifiedEdit,
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

fn api_publish_lead_days(api: &str, index: u32) -> i64 {
    let base = match api {
        "blogs" => 1,
        "authors" => 2,
        "news" => 4,
        "categories" => 3,
        "pages" => 8,
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
        _ => 8,
    };
    base + i64::from(index % 6)
}

fn build_sparse_active_days(
    rng: &mut SeededRng,
    start_date: NaiveDate,
    days: u32,
) -> Vec<NaiveDate> {
    let mut active_days = Vec::new();
    for offset in 0..days {
        let threshold = (BULK_ACTIVE_DAY_DENSITY * u64::MAX as f64) as u64;
        if rng.next_u64() <= threshold {
            active_days.push(start_date + Duration::days(i64::from(offset)));
        }
    }
    if active_days.is_empty() {
        active_days.push(start_date);
    }
    active_days
}

/// Filler templates exclude CREATE_DRAFT / FIRST_PUBLISH / CREATE_PUBLISH so Grafana
/// publish-duration metrics stay driven by coordinated metric-lifecycle pairs only.
fn pick_bulk_template(rng: &mut SeededRng) -> BulkTemplate {
    const TEMPLATES: [BulkTemplate; 4] = [
        BulkTemplate::UpdatePublish,
        BulkTemplate::Unpublish,
        BulkTemplate::Delete,
        BulkTemplate::UnclassifiedEdit,
    ];
    TEMPLATES[rng.next_usize(TEMPLATES.len())]
}

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
        BulkTemplate::CreateDraft => format!(
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
        BulkTemplate::CreatePublish => format!(
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
        BulkTemplate::FirstPublish => format!(
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
        BulkTemplate::UpdatePublish => format!(
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
        BulkTemplate::Unpublish => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"edit",
              "contents":{{
                "old":{{"status":["PUBLISH"],"updatedAt":"{day_before}"}},
                "new":{{"status":["DRAFT"],"updatedAt":"{timestamp}"}}
              }}
            }}"#
        ),
        BulkTemplate::Delete => format!(
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
        BulkTemplate::UnclassifiedEdit => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"edit",
              "contents":{{
                "old":{{"status":["DRAFT"],"updatedAt":"{day_before}"}},
                "new":{{"status":["DRAFT"],"updatedAt":"{timestamp}"}}
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

fn random_received_at_on_days(rng: &mut SeededRng, active_days: &[NaiveDate]) -> DateTime<Utc> {
    let date = active_days[rng.next_usize(active_days.len())];
    let hour = rng.next_u32(24);
    let minute = rng.next_u32(60);
    let second = rng.next_u32(60);
    jst_datetime(
        date,
        NaiveTime::from_hms_opt(hour, minute, second).expect("valid time"),
    )
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

        assert_eq!(summary.event_count, 7);
        assert_eq!(summary.file_count, 7);
        assert!(summary.partition_count >= 2);
        assert!(summary.min_dt.is_some());
        assert!(summary.max_dt.is_some());

        let parquet_files: Vec<_> = walk_parquet_files(tempdir.path());
        assert_eq!(parquet_files.len(), 7);
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
        assert_eq!(events[6].content_id, None);
        assert_eq!(events[0].content_id.as_deref(), Some("content-1"));
        assert_eq!(events[5].content_id.as_deref(), Some("content-2"));
        assert_eq!(events[0].event_kind.as_deref(), Some("FIRST_PUBLISH"));
        assert_eq!(events[6].event_kind.as_deref(), Some("DELETE"));
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
            unique_days < 80,
            "expected moderately sparse heatmap days, got {unique_days}"
        );
        assert!(unique_days > 35);
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
                    if content_id.starts_with("metric-") {
                        continue;
                    }
                    let kind = event_kinds.value(row);
                    if matches!(kind, "CREATE_DRAFT" | "FIRST_PUBLISH" | "CREATE_PUBLISH") {
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
    fn metric_lifecycle_draft_to_publish_days_stay_within_api_ranges() {
        let mut events = Vec::new();
        generate_bulk_metric_lifecycle_events(jst_today(), &mut events, &mut None, &mut None)
            .unwrap();

        for api in BULK_APIS {
            let mut durations = Vec::new();
            for index in 0..METRIC_CONTENTS_PER_API {
                let content_id = format!("metric-{api}-{index}");
                let draft = events
                    .iter()
                    .find(|event| {
                        event.content_id.as_deref() == Some(content_id.as_str())
                            && event.event_kind.as_deref() == Some("CREATE_DRAFT")
                    })
                    .expect("draft event");
                let publish = events
                    .iter()
                    .find(|event| {
                        event.content_id.as_deref() == Some(content_id.as_str())
                            && event.event_kind.as_deref() == Some("FIRST_PUBLISH")
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
        let mut events = Vec::new();
        generate_bulk_metric_lifecycle_events(jst_today(), &mut events, &mut None, &mut None)
            .unwrap();

        let blogs_lead = publish_lead_days_for_api(&events, "blogs");
        let pages_lead = publish_lead_days_for_api(&events, "pages");
        assert!(blogs_lead < pages_lead);

        let blogs_draft_lead = draft_to_publish_days_for_api(&events, "blogs");
        let pages_draft_lead = draft_to_publish_days_for_api(&events, "pages");
        assert!(blogs_draft_lead < pages_draft_lead);
    }

    #[test]
    fn build_sparse_active_days_covers_fraction_of_range() {
        let start = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let active_days = build_sparse_active_days(&mut SeededRng::new(42), start, 90);
        assert!(active_days.len() < 75);
        assert!(active_days.len() > 35);
    }

    fn publish_lead_days_for_api(events: &[NormalizedEvent], api: &str) -> i64 {
        let event = events
            .iter()
            .find(|event| {
                event.api.as_deref() == Some(api)
                    && matches!(
                        event.event_kind.as_deref(),
                        Some("FIRST_PUBLISH") | Some("CREATE_PUBLISH")
                    )
                    && event.content_created_at.is_some()
                    && event.content_published_at.is_some()
            })
            .expect("publish event");
        (event.content_published_at.unwrap() - event.content_created_at.unwrap()).num_days()
    }

    fn draft_to_publish_days_for_api(events: &[NormalizedEvent], api: &str) -> i64 {
        let content_id = format!("metric-{api}-0");
        let draft = events
            .iter()
            .find(|event| {
                event.content_id.as_deref() == Some(content_id.as_str())
                    && event.event_kind.as_deref() == Some("CREATE_DRAFT")
            })
            .expect("draft event");
        let publish = events
            .iter()
            .find(|event| {
                event.content_id.as_deref() == Some(content_id.as_str())
                    && event.event_kind.as_deref() == Some("FIRST_PUBLISH")
            })
            .expect("publish event");
        (publish.content_published_at.unwrap() - draft.draft_created_at.unwrap()).num_days()
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
