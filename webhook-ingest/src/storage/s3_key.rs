use chrono::{DateTime, FixedOffset, Utc};

pub fn build_s3_key(
    prefix: &str,
    service: &str,
    api: &str,
    received_at: DateTime<Utc>,
    event_id: &str,
) -> String {
    format!(
        "{}/service={}/api={}/dt={}/{}.parquet",
        prefix.trim_matches('/'),
        partition_escape(service),
        partition_escape(api),
        jst_partition_date(received_at),
        event_id
    )
}

fn jst_partition_date(received_at: DateTime<Utc>) -> String {
    let jst = FixedOffset::east_opt(9 * 60 * 60).expect("valid JST offset");
    received_at
        .with_timezone(&jst)
        .format("%Y-%m-%d")
        .to_string()
}

fn partition_escape(value: &str) -> String {
    value
        .chars()
        .map(|char| match char {
            '/' | '=' | '?' | '#' | '[' | ']' | ' ' => '_',
            char => char,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Utc};

    use super::*;

    #[test]
    fn builds_partitioned_s3_key() {
        let received_at = DateTime::parse_from_rfc3339("2026-06-29T01:02:03Z")
            .unwrap()
            .with_timezone(&Utc);

        assert_eq!(
            build_s3_key(
                "/microcms_events/",
                "example/service",
                "blogs api",
                received_at,
                "event-id"
            ),
            "microcms_events/service=example_service/api=blogs_api/dt=2026-06-29/event-id.parquet"
        );
    }

    #[test]
    fn builds_partitioned_s3_key_with_jst_calendar_day() {
        let before_jst_midnight = DateTime::parse_from_rfc3339("2026-06-29T14:59:59Z")
            .unwrap()
            .with_timezone(&Utc);
        let after_jst_midnight = DateTime::parse_from_rfc3339("2026-06-29T15:00:00Z")
            .unwrap()
            .with_timezone(&Utc);

        assert_eq!(
            build_s3_key(
                "microcms_events",
                "example-service",
                "blogs",
                before_jst_midnight,
                "event-id"
            ),
            "microcms_events/service=example-service/api=blogs/dt=2026-06-29/event-id.parquet"
        );
        assert_eq!(
            build_s3_key(
                "microcms_events",
                "example-service",
                "blogs",
                after_jst_midnight,
                "event-id"
            ),
            "microcms_events/service=example-service/api=blogs/dt=2026-06-30/event-id.parquet"
        );
    }
}
