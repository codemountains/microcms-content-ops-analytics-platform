use duckdb::Connection;

use super::event_kind;
use super::{AverageTimeToPublishRow, JST_OFFSET_INTERVAL, collect_rows};
use crate::handler::PublishDurationUnit;

pub(crate) fn query_average_time_to_publish_rows(
    connection: &Connection,
    events_sql: &str,
    days: u32,
    unit: PublishDurationUnit,
) -> duckdb::Result<Vec<AverageTimeToPublishRow>> {
    let days_minus_one = days - 1;
    let (divisor, value_column) = unit.sql_parts();
    let publish_from_draft = event_kind::PUBLISH_FROM_DRAFT;
    let initial_publish = event_kind::INITIAL_PUBLISH;
    let republish_from_closed = event_kind::REPUBLISH_FROM_CLOSED;
    let sql = format!(
        r#"
        SELECT
          api,
          AVG(date_diff('second', content_created_at, content_published_at) / {divisor}) AS {value_column}
        FROM {events_sql}
        WHERE
          dt >= CAST(CAST(current_timestamp AS TIMESTAMP) + INTERVAL '{JST_OFFSET_INTERVAL}' AS DATE) - INTERVAL '{days_minus_one} DAY'
          AND event_kind IN ('{publish_from_draft}', '{initial_publish}', '{republish_from_closed}')
          AND content_created_at IS NOT NULL
          AND content_published_at IS NOT NULL
          AND content_published_at >= content_created_at
        GROUP BY api
        ORDER BY {value_column} DESC, api
        "#
    );

    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([], |row| {
        let value = row.get(1)?;
        Ok(AverageTimeToPublishRow {
            api: row.get(0)?,
            avg_days: unit.days_value(value),
            avg_hours: unit.hours_value(value),
        })
    })?;
    collect_rows(rows)
}

#[cfg(test)]
mod tests {
    use duckdb::Connection;
    use serde_json::json;

    use super::*;

    #[test]
    fn queries_average_time_to_publish_by_api_in_selected_unit() {
        let connection = Connection::open_in_memory().unwrap();
        let current_jst_date =
            "CAST(CAST(current_timestamp AS TIMESTAMP) + INTERVAL '9 HOURS' AS DATE)";
        let events_sql = r#"
            (
              SELECT {current_jst_date} AS dt, 'blogs' AS api, 'INITIAL_PUBLISH' AS event_kind,
                     TIMESTAMP '2026-06-28 00:00:00' AS content_created_at,
                     TIMESTAMP '2026-06-29 12:00:00' AS content_published_at
              UNION ALL
              SELECT {current_jst_date}, 'blogs', 'PUBLISH_FROM_DRAFT',
                     TIMESTAMP '2026-06-29 00:00:00',
                     TIMESTAMP '2026-06-29 12:00:00'
              UNION ALL
              SELECT {current_jst_date}, 'blogs', 'REPUBLISH_FROM_CLOSED',
                     TIMESTAMP '2026-06-27 00:00:00',
                     TIMESTAMP '2026-06-29 12:00:00'
              UNION ALL
              SELECT {current_jst_date}, 'blogs', 'PUBLISH_FROM_DRAFT',
                     TIMESTAMP '2026-06-30 00:00:00',
                     TIMESTAMP '2026-06-29 12:00:00'
              UNION ALL
              SELECT {current_jst_date}, 'blogs', 'PUBLISH_FROM_DRAFT',
                     NULL::TIMESTAMP,
                     TIMESTAMP '2026-06-29 12:00:00'
              UNION ALL
              SELECT {current_jst_date}, 'authors', 'INITIAL_PUBLISH',
                     TIMESTAMP '2026-06-29 00:00:00',
                     TIMESTAMP '2026-06-29 12:00:00'
            )
        "#
        .replace("{current_jst_date}", current_jst_date);

        let day_rows = query_average_time_to_publish_rows(
            &connection,
            &events_sql,
            30,
            PublishDurationUnit::Days,
        )
        .unwrap();
        let hour_rows = query_average_time_to_publish_rows(
            &connection,
            &events_sql,
            30,
            PublishDurationUnit::Hours,
        )
        .unwrap();

        assert_eq!(day_rows.len(), 2);
        assert_eq!(day_rows[0].api.as_deref(), Some("blogs"));
        assert_eq!(day_rows[0].avg_days, Some(1.5));
        assert_eq!(day_rows[0].avg_hours, None);
        assert_eq!(
            serde_json::to_value(&day_rows[0]).unwrap(),
            json!({
                "api": "blogs",
                "avg_days": 1.5,
                "avg_hours": null
            })
        );
        assert_eq!(day_rows[1].api.as_deref(), Some("authors"));
        assert_eq!(day_rows[1].avg_days, Some(0.5));
        assert_eq!(day_rows[1].avg_hours, None);

        assert_eq!(hour_rows.len(), 2);
        assert_eq!(hour_rows[0].api.as_deref(), Some("blogs"));
        assert_eq!(hour_rows[0].avg_days, None);
        assert_eq!(hour_rows[0].avg_hours, Some(36.0));
        assert_eq!(
            serde_json::to_value(&hour_rows[0]).unwrap(),
            json!({
                "api": "blogs",
                "avg_days": null,
                "avg_hours": 36.0
            })
        );
        assert_eq!(hour_rows[1].api.as_deref(), Some("authors"));
        assert_eq!(hour_rows[1].avg_days, None);
        assert_eq!(hour_rows[1].avg_hours, Some(12.0));
    }
}
