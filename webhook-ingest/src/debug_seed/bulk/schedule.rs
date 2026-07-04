use chrono::{DateTime, Datelike, Duration, NaiveDate, NaiveTime, Utc};

use super::super::rng::SeededRng;
use super::super::time::jst_datetime;

const BULK_ACTIVE_DAY_DENSITY: f64 = 0.80;

#[derive(Debug, Clone)]
pub(in crate::debug_seed) struct BulkDaySchedule {
    pub(in crate::debug_seed) days: Vec<WeightedBulkDay>,
}

impl BulkDaySchedule {
    pub(in crate::debug_seed) fn latest_date(&self) -> NaiveDate {
        self.days.last().expect("non-empty schedule").date
    }
}

#[derive(Debug, Clone)]
pub(in crate::debug_seed) struct WeightedBulkDay {
    pub(in crate::debug_seed) date: NaiveDate,
    pub(in crate::debug_seed) weight: u32,
}

#[derive(Debug)]
struct BulkDayCandidate {
    date: NaiveDate,
    weight: u32,
    active_score: u32,
}

pub(in crate::debug_seed) fn build_realistic_bulk_day_schedule(
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

pub(in crate::debug_seed) fn random_received_at_on_schedule(
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
