use duckdb::Connection;

use super::event_kind;
use super::{AverageDraftToPublishRow, collect_rows, time_range_bounds_cte};
use crate::handler::PublishDurationUnit;

pub(crate) fn query_average_draft_to_publish_rows(
    connection: &Connection,
    events_sql: &str,
    from_ms: i64,
    to_ms: i64,
    unit: PublishDurationUnit,
) -> duckdb::Result<Vec<AverageDraftToPublishRow>> {
    let bounds = time_range_bounds_cte(from_ms, to_ms);
    let (divisor, value_column) = unit.sql_parts();
    let initial_draft = event_kind::INITIAL_DRAFT;
    let publish_from_draft = event_kind::PUBLISH_FROM_DRAFT;
    let sql = format!(
        r#"
        WITH {bounds},
        drafts AS (
          SELECT
            api,
            content_id,
            MIN(draft_created_at) AS draft_at
          FROM {events_sql}
          WHERE
            event_kind = '{initial_draft}'
            AND content_id IS NOT NULL
            AND draft_created_at IS NOT NULL
          GROUP BY api, content_id
        ),
        first_publishes AS (
          SELECT
            api,
            content_id,
            MIN(content_published_at) AS published_at
          FROM {events_sql}
          WHERE
            dt >= (SELECT start_date FROM bounds)
            AND dt <= (SELECT end_date FROM bounds)
            AND event_kind = '{publish_from_draft}'
            AND content_id IS NOT NULL
            AND content_published_at IS NOT NULL
          GROUP BY api, content_id
        )
        SELECT
          drafts.api,
          AVG(date_diff('second', drafts.draft_at, first_publishes.published_at) / {divisor}) AS {value_column},
          COUNT(*) AS sample_count
        FROM drafts
        INNER JOIN first_publishes
          ON drafts.api = first_publishes.api
          AND drafts.content_id = first_publishes.content_id
        WHERE first_publishes.published_at >= drafts.draft_at
        GROUP BY drafts.api
        ORDER BY {value_column} DESC, drafts.api
        "#
    );

    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([], |row| {
        let value = row.get(1)?;
        Ok(AverageDraftToPublishRow {
            api: row.get(0)?,
            avg_days: unit.days_value(value),
            avg_hours: unit.hours_value(value),
            sample_count: row.get(2)?,
        })
    })?;
    collect_rows(rows)
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use duckdb::Connection;

    use super::*;

    #[test]
    fn queries_average_draft_to_publish_by_api_from_first_publish_period() {
        let connection = Connection::open_in_memory().unwrap();
        let from_ms = DateTime::parse_from_rfc3339("2026-06-30T00:00:00+09:00")
            .unwrap()
            .timestamp_millis();
        let to_ms = DateTime::parse_from_rfc3339("2026-06-30T23:59:59+09:00")
            .unwrap()
            .timestamp_millis();
        let events_sql = r#"
            (
              SELECT DATE '2026-06-30' AS dt, 'blogs' AS api, 'content-1' AS content_id,
                     'INITIAL_DRAFT' AS event_kind, TIMESTAMP '2026-06-24 00:00:00' AS draft_created_at,
                     NULL::TIMESTAMP AS content_published_at
              UNION ALL
              SELECT DATE '2026-06-30', 'blogs', 'content-1',
                     'PUBLISH_FROM_DRAFT', NULL::TIMESTAMP, TIMESTAMP '2026-06-29 00:00:00'
              UNION ALL
              SELECT DATE '2026-06-30', 'blogs', 'content-2',
                     'INITIAL_DRAFT', TIMESTAMP '2026-06-27 00:00:00', NULL::TIMESTAMP
              UNION ALL
              SELECT DATE '2026-06-30', 'blogs', 'content-2',
                     'PUBLISH_FROM_DRAFT', NULL::TIMESTAMP, TIMESTAMP '2026-06-29 00:00:00'
              UNION ALL
              SELECT DATE '2026-06-30', 'blogs', 'draft-only',
                     'INITIAL_DRAFT', TIMESTAMP '2026-06-26 00:00:00', NULL::TIMESTAMP
              UNION ALL
              SELECT DATE '2026-06-30', 'blogs', 'saved-draft-only',
                     'SAVE_DRAFT', TIMESTAMP '2026-06-26 00:00:00', NULL::TIMESTAMP
              UNION ALL
              SELECT DATE '2026-06-30', 'blogs', 'instant-publish',
                     'INITIAL_PUBLISH', NULL::TIMESTAMP, TIMESTAMP '2026-06-29 00:00:00'
              UNION ALL
              SELECT DATE '2026-06-29',
                     'blogs', 'outside-period', 'PUBLISH_FROM_DRAFT', NULL::TIMESTAMP, TIMESTAMP '2026-06-29 00:00:00'
              UNION ALL
              SELECT DATE '2026-06-30', 'blogs', 'outside-period',
                     'INITIAL_DRAFT', TIMESTAMP '2026-06-27 00:00:00', NULL::TIMESTAMP
              UNION ALL
              SELECT DATE '2026-06-30', 'blogs', 'negative-lead',
                     'INITIAL_DRAFT', TIMESTAMP '2026-06-30 00:00:00', NULL::TIMESTAMP
              UNION ALL
              SELECT DATE '2026-06-30', 'blogs', 'negative-lead',
                     'PUBLISH_FROM_DRAFT', NULL::TIMESTAMP, TIMESTAMP '2026-06-29 00:00:00'
              UNION ALL
              SELECT DATE '2026-06-30', 'authors', 'author-1',
                     'INITIAL_DRAFT', TIMESTAMP '2026-06-28 00:00:00', NULL::TIMESTAMP
              UNION ALL
              SELECT DATE '2026-06-30', 'authors', 'author-1',
                     'PUBLISH_FROM_DRAFT', NULL::TIMESTAMP, TIMESTAMP '2026-06-29 00:00:00'
            )
        "#;

        let day_rows = query_average_draft_to_publish_rows(
            &connection,
            events_sql,
            from_ms,
            to_ms,
            PublishDurationUnit::Days,
        )
        .unwrap();
        let hour_rows = query_average_draft_to_publish_rows(
            &connection,
            events_sql,
            from_ms,
            to_ms,
            PublishDurationUnit::Hours,
        )
        .unwrap();

        assert_eq!(day_rows.len(), 2);
        assert_eq!(day_rows[0].api.as_deref(), Some("blogs"));
        assert_eq!(day_rows[0].avg_days, Some(3.5));
        assert_eq!(day_rows[0].avg_hours, None);
        assert_eq!(day_rows[0].sample_count, 2);
        assert_eq!(day_rows[1].api.as_deref(), Some("authors"));
        assert_eq!(day_rows[1].avg_days, Some(1.0));
        assert_eq!(day_rows[1].avg_hours, None);
        assert_eq!(day_rows[1].sample_count, 1);

        assert_eq!(hour_rows.len(), 2);
        assert_eq!(hour_rows[0].api.as_deref(), Some("blogs"));
        assert_eq!(hour_rows[0].avg_days, None);
        assert_eq!(hour_rows[0].avg_hours, Some(84.0));
        assert_eq!(hour_rows[0].sample_count, 2);
        assert_eq!(hour_rows[1].api.as_deref(), Some("authors"));
        assert_eq!(hour_rows[1].avg_days, None);
        assert_eq!(hour_rows[1].avg_hours, Some(24.0));
        assert_eq!(hour_rows[1].sample_count, 1);
    }
}
