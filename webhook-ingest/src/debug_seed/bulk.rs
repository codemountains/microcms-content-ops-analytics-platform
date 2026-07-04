pub(super) mod activity;
pub(super) mod schedule;
mod template;
pub(super) mod timing;

use std::collections::BTreeMap;

use chrono::{DateTime, Duration, NaiveDate, NaiveTime, Utc};

use crate::{IngestError, NormalizedEvent, normalize_payload};

use self::activity::{ActivityTargets, compute_activity_targets};
use self::schedule::{
    BulkDaySchedule, build_realistic_bulk_day_schedule, random_received_at_on_schedule,
};
use self::template::{BulkEventTiming, BulkTemplate, build_bulk_webhook_body};
use self::timing::{api_draft_to_publish_days, api_publish_lead_days};
use super::config::{DebugSeedConfig, DebugSeedSummary};
use super::io::{partition_dir_for_event, track_date, write_multi_event_file};
use super::rng::SeededRng;
use super::time::{jst_date, jst_datetime, jst_today};

pub(super) const BULK_APIS: &[&str] = &[
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
pub(super) fn generate_bulk_files(
    config: &DebugSeedConfig,
) -> Result<DebugSeedSummary, IngestError> {
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

pub(super) fn generate_bulk_activity_events(
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

    let filler_template_counts = [
        (BulkTemplate::UpdatePublished, targets.update_published),
        (
            BulkTemplate::AddDraftToPublished,
            targets.add_draft_to_published,
        ),
        (
            BulkTemplate::DiscardDraftOnPublished,
            targets.discard_draft_on_published,
        ),
        (BulkTemplate::UnpublishToDraft, targets.unpublish_to_draft),
        (BulkTemplate::UnpublishToClosed, targets.unpublish_to_closed),
        (BulkTemplate::ReopenToDraft, targets.reopen_to_draft),
        (
            BulkTemplate::RepublishFromClosed,
            targets.republish_from_closed,
        ),
        (BulkTemplate::DeleteDraft, targets.delete_draft),
        (BulkTemplate::DeletePublished, targets.delete_published),
        (BulkTemplate::DeleteClosed, targets.delete_closed),
    ];
    let filler_capacity = filler_template_counts
        .iter()
        .map(|(_, count)| *count as usize)
        .sum();
    let mut filler_templates = Vec::with_capacity(filler_capacity);
    for (template, count) in filler_template_counts {
        filler_templates.extend(std::iter::repeat_n(template, count as usize));
    }
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

fn pick_content_id(rng: &mut SeededRng, contents: u32) -> String {
    let raw = rng.next_u64() as f64 / u64::MAX as f64;
    let biased = raw * raw;
    let index = (biased * f64::from(contents - 1)).round() as u32;
    format!("content-{index}")
}
