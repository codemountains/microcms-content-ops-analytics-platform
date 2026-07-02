use crate::ApiError;

pub(crate) fn validate_days(days: Option<u32>) -> Result<u32, ApiError> {
    validate_days_with_default(days, 30)
}

fn validate_days_with_default(days: Option<u32>, default: u32) -> Result<u32, ApiError> {
    let days = days.unwrap_or(default);
    if (1..=3660).contains(&days) {
        Ok(days)
    } else {
        Err(ApiError::InvalidQuery("days must be between 1 and 3660"))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PublishDurationUnit {
    Days,
    Hours,
}

impl PublishDurationUnit {
    pub(crate) fn sql_parts(self) -> (&'static str, &'static str) {
        match self {
            Self::Days => ("86400.0", "avg_days"),
            Self::Hours => ("3600.0", "avg_hours"),
        }
    }

    pub(crate) fn days_value(self, value: f64) -> Option<f64> {
        match self {
            Self::Days => Some(value),
            Self::Hours => None,
        }
    }

    pub(crate) fn hours_value(self, value: f64) -> Option<f64> {
        match self {
            Self::Days => None,
            Self::Hours => Some(value),
        }
    }
}

pub(crate) fn validate_publish_duration_unit(
    unit: Option<&str>,
) -> Result<PublishDurationUnit, ApiError> {
    match unit.unwrap_or("days") {
        "days" => Ok(PublishDurationUnit::Days),
        "hours" => Ok(PublishDurationUnit::Hours),
        _ => Err(ApiError::InvalidQuery("unit must be days or hours")),
    }
}

const MAX_CALENDAR_RANGE_MS: i64 = 3660 * 24 * 60 * 60 * 1000;
const DEFAULT_CALENDAR_RANGE_MS: i64 = 365 * 24 * 60 * 60 * 1000;

pub(crate) fn validate_time_range(
    from: Option<i64>,
    to: Option<i64>,
) -> Result<(i64, i64), ApiError> {
    match (from, to) {
        (Some(from_ms), Some(to_ms)) => {
            if from_ms > to_ms {
                return Err(ApiError::InvalidQuery(
                    "from must be less than or equal to to",
                ));
            }
            if to_ms - from_ms > MAX_CALENDAR_RANGE_MS {
                return Err(ApiError::InvalidQuery(
                    "time range must not exceed 3660 days",
                ));
            }
            Ok((from_ms, to_ms))
        }
        (None, None) => {
            let to_ms = chrono::Utc::now().timestamp_millis();
            Ok((to_ms - DEFAULT_CALENDAR_RANGE_MS, to_ms))
        }
        _ => Err(ApiError::InvalidQuery("from and to must both be provided")),
    }
}

pub(crate) fn validate_limit(limit: Option<u32>) -> Result<u32, ApiError> {
    let limit = limit.unwrap_or(20);
    if (1..=1000).contains(&limit) {
        Ok(limit)
    } else {
        Err(ApiError::InvalidQuery("limit must be between 1 and 1000"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_time_range() {
        let from_ms = 1_000_i64;
        let to_ms = 2_000_i64;
        assert_eq!(
            validate_time_range(Some(from_ms), Some(to_ms)).unwrap(),
            (from_ms, to_ms)
        );
        assert!(validate_time_range(Some(to_ms), Some(from_ms)).is_err());
        assert!(validate_time_range(Some(from_ms), None).is_err());
        assert!(validate_time_range(None, Some(to_ms)).is_err());

        let (default_from, default_to) = validate_time_range(None, None).unwrap();
        assert!(default_to > default_from);
        assert_eq!(default_to - default_from, DEFAULT_CALENDAR_RANGE_MS);
    }

    #[test]
    fn validates_days() {
        assert_eq!(validate_days(None).unwrap(), 30);
        assert_eq!(validate_days_with_default(None, 365).unwrap(), 365);
        assert_eq!(validate_days(Some(1)).unwrap(), 1);
        assert_eq!(validate_days(Some(3660)).unwrap(), 3660);
        assert!(validate_days(Some(0)).is_err());
        assert!(validate_days(Some(3661)).is_err());
    }

    #[test]
    fn validates_publish_duration_unit() {
        assert_eq!(
            validate_publish_duration_unit(None).unwrap(),
            PublishDurationUnit::Days
        );
        assert_eq!(
            validate_publish_duration_unit(Some("days")).unwrap(),
            PublishDurationUnit::Days
        );
        assert_eq!(
            validate_publish_duration_unit(Some("hours")).unwrap(),
            PublishDurationUnit::Hours
        );
        assert!(validate_publish_duration_unit(Some("weeks")).is_err());
    }

    #[test]
    fn validates_limit() {
        assert_eq!(validate_limit(None).unwrap(), 20);
        assert_eq!(validate_limit(Some(1)).unwrap(), 1);
        assert_eq!(validate_limit(Some(1000)).unwrap(), 1000);
        assert!(validate_limit(Some(0)).is_err());
        assert!(validate_limit(Some(1001)).is_err());
    }
}
