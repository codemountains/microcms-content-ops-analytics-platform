use duckdb::Connection;

use super::event_kind;
use super::{AverageDraftToPublishRow, JST_OFFSET_INTERVAL, collect_rows};
use crate::handler::PublishDurationUnit;

pub(crate) fn query_average_draft_to_publish_rows(
    connection: &Connection,
    events_sql: &str,
    days: u32,
    unit: PublishDurationUnit,
) -> duckdb::Result<Vec<AverageDraftToPublishRow>> {
    let days_minus_one = days - 1;
    let (divisor, value_column) = unit.sql_parts();
    let create_draft = event_kind::CREATE_DRAFT;
    let first_publish = event_kind::FIRST_PUBLISH;
    let sql = format!(
        r#"
        WITH drafts AS (
          SELECT
            api,
            content_id,
            MIN(draft_created_at) AS draft_at
          FROM {events_sql}
          WHERE
            event_kind = '{create_draft}'
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
            dt >= CAST(CAST(current_timestamp AS TIMESTAMP) + INTERVAL '{JST_OFFSET_INTERVAL}' AS DATE) - INTERVAL '{days_minus_one} DAY'
            AND event_kind = '{first_publish}'
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
    use duckdb::Connection;

    use super::*;

    #[test]
    fn queries_average_draft_to_publish_by_api_from_first_publish_period() {
        let connection = Connection::open_in_memory().unwrap();
        let current_jst_date =
            "CAST(CAST(current_timestamp AS TIMESTAMP) + INTERVAL '9 HOURS' AS DATE)";
        let events_sql = r#"
            (
              SELECT {current_jst_date} AS dt, 'blogs' AS api, 'content-1' AS content_id,
                     'CREATE_DRAFT' AS event_kind, TIMESTAMP '2026-06-24 00:00:00' AS draft_created_at,
                     NULL::TIMESTAMP AS content_published_at
              UNION ALL
              SELECT {current_jst_date}, 'blogs', 'content-1',
                     'FIRST_PUBLISH', NULL::TIMESTAMP, TIMESTAMP '2026-06-29 00:00:00'
              UNION ALL
              SELECT {current_jst_date}, 'blogs', 'content-2',
                     'CREATE_DRAFT', TIMESTAMP '2026-06-27 00:00:00', NULL::TIMESTAMP
              UNION ALL
              SELECT {current_jst_date}, 'blogs', 'content-2',
                     'FIRST_PUBLISH', NULL::TIMESTAMP, TIMESTAMP '2026-06-29 00:00:00'
              UNION ALL
              SELECT {current_jst_date}, 'blogs', 'draft-only',
                     'CREATE_DRAFT', TIMESTAMP '2026-06-26 00:00:00', NULL::TIMESTAMP
              UNION ALL
              SELECT {current_jst_date}, 'blogs', 'instant-publish',
                     'CREATE_PUBLISH', NULL::TIMESTAMP, TIMESTAMP '2026-06-29 00:00:00'
              UNION ALL
              SELECT {current_jst_date} - INTERVAL '40 DAY',
                     'blogs', 'outside-period', 'FIRST_PUBLISH', NULL::TIMESTAMP, TIMESTAMP '2026-06-29 00:00:00'
              UNION ALL
              SELECT {current_jst_date}, 'blogs', 'outside-period',
                     'CREATE_DRAFT', TIMESTAMP '2026-06-27 00:00:00', NULL::TIMESTAMP
              UNION ALL
              SELECT {current_jst_date}, 'blogs', 'negative-lead',
                     'CREATE_DRAFT', TIMESTAMP '2026-06-30 00:00:00', NULL::TIMESTAMP
              UNION ALL
              SELECT {current_jst_date}, 'blogs', 'negative-lead',
                     'FIRST_PUBLISH', NULL::TIMESTAMP, TIMESTAMP '2026-06-29 00:00:00'
              UNION ALL
              SELECT {current_jst_date}, 'authors', 'author-1',
                     'CREATE_DRAFT', TIMESTAMP '2026-06-28 00:00:00', NULL::TIMESTAMP
              UNION ALL
              SELECT {current_jst_date}, 'authors', 'author-1',
                     'FIRST_PUBLISH', NULL::TIMESTAMP, TIMESTAMP '2026-06-29 00:00:00'
            )
        "#
        .replace("{current_jst_date}", current_jst_date);

        let day_rows = query_average_draft_to_publish_rows(
            &connection,
            &events_sql,
            30,
            PublishDurationUnit::Days,
        )
        .unwrap();
        let hour_rows = query_average_draft_to_publish_rows(
            &connection,
            &events_sql,
            30,
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
