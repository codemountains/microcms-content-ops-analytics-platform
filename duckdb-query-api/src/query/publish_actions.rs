use duckdb::Connection;

use super::event_kind;
use super::{
    PARTITION_TIME_JST_SUFFIX, PublishActionSummaryRow, PublishActionTrendRow, collect_rows,
    time_range_bounds_cte,
};

pub(crate) fn query_publish_action_summary_rows(
    connection: &Connection,
    events_sql: &str,
    from_ms: i64,
    to_ms: i64,
) -> duckdb::Result<Vec<PublishActionSummaryRow>> {
    let bounds = time_range_bounds_cte(from_ms, to_ms);
    let initial_draft = event_kind::INITIAL_DRAFT;
    let save_draft = event_kind::SAVE_DRAFT;
    let publish_from_draft = event_kind::PUBLISH_FROM_DRAFT;
    let initial_publish = event_kind::INITIAL_PUBLISH;
    let update_published = event_kind::UPDATE_PUBLISHED;
    let unpublish_to_draft = event_kind::UNPUBLISH_TO_DRAFT;
    let unpublish_to_closed = event_kind::UNPUBLISH_TO_CLOSED;
    let reopen_to_draft = event_kind::REOPEN_TO_DRAFT;
    let republish_from_closed = event_kind::REPUBLISH_FROM_CLOSED;
    let sql = format!(
        r#"
        WITH {bounds},
        summary AS (
          SELECT
            COALESCE(SUM(CASE WHEN event_kind IN ('{publish_from_draft}', '{initial_publish}', '{republish_from_closed}') THEN 1 ELSE 0 END), 0) AS publish_count,
            COALESCE(SUM(CASE WHEN event_kind IN ('{publish_from_draft}', '{initial_publish}', '{update_published}', '{republish_from_closed}') THEN 1 ELSE 0 END), 0) AS published_state_count,
            COALESCE(SUM(CASE WHEN event_kind IN ('{initial_draft}', '{save_draft}', '{publish_from_draft}', '{initial_publish}', '{update_published}', '{unpublish_to_draft}', '{unpublish_to_closed}', '{reopen_to_draft}', '{republish_from_closed}') THEN 1 ELSE 0 END), 0) AS state_arrival_count
          FROM {events_sql}
          WHERE
            dt >= (SELECT start_date FROM bounds)
            AND dt <= (SELECT end_date FROM bounds)
        )
        SELECT
          publish_count,
          published_state_count,
          state_arrival_count,
          CASE
            WHEN state_arrival_count = 0 THEN NULL
            ELSE published_state_count::DOUBLE / state_arrival_count
          END AS published_state_rate
        FROM summary
        "#
    );

    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([], |row| {
        Ok(PublishActionSummaryRow {
            publish_count: row.get(0)?,
            published_state_count: row.get(1)?,
            state_arrival_count: row.get(2)?,
            published_state_rate: row.get(3)?,
        })
    })?;
    collect_rows(rows)
}

pub(crate) fn query_publish_action_trend_rows(
    connection: &Connection,
    events_sql: &str,
    from_ms: i64,
    to_ms: i64,
) -> duckdb::Result<Vec<PublishActionTrendRow>> {
    let bounds = time_range_bounds_cte(from_ms, to_ms);
    let publish_from_draft = event_kind::PUBLISH_FROM_DRAFT;
    let initial_publish = event_kind::INITIAL_PUBLISH;
    let republish_from_closed = event_kind::REPUBLISH_FROM_CLOSED;
    let sql = format!(
        r#"
        WITH {bounds},
        calendar AS (
          SELECT CAST(day AS DATE) AS dt
          FROM generate_series(
            (SELECT start_date FROM bounds),
            (SELECT end_date FROM bounds),
            INTERVAL 1 DAY
          ) AS series(day)
        ),
        daily AS (
          SELECT
            dt,
            SUM(CASE WHEN event_kind = '{publish_from_draft}' THEN 1 ELSE 0 END) AS publish_from_draft_count,
            SUM(CASE WHEN event_kind = '{initial_publish}' THEN 1 ELSE 0 END) AS initial_publish_count,
            SUM(CASE WHEN event_kind = '{republish_from_closed}' THEN 1 ELSE 0 END) AS republish_from_closed_count
          FROM {events_sql}
          WHERE
            dt >= (SELECT start_date FROM bounds)
            AND dt <= (SELECT end_date FROM bounds)
          GROUP BY dt
        )
        SELECT
          CONCAT(CAST(calendar.dt AS VARCHAR), '{PARTITION_TIME_JST_SUFFIX}') AS time,
          COALESCE(daily.publish_from_draft_count, 0) AS publish_from_draft_count,
          COALESCE(daily.initial_publish_count, 0) AS initial_publish_count,
          COALESCE(daily.republish_from_closed_count, 0) AS republish_from_closed_count,
          COALESCE(daily.publish_from_draft_count, 0) + COALESCE(daily.initial_publish_count, 0) + COALESCE(daily.republish_from_closed_count, 0) AS publish_count
        FROM calendar
        LEFT JOIN daily ON daily.dt = calendar.dt
        ORDER BY calendar.dt
        "#
    );

    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([], |row| {
        Ok(PublishActionTrendRow {
            time: row.get(0)?,
            publish_from_draft_count: row.get(1)?,
            initial_publish_count: row.get(2)?,
            republish_from_closed_count: row.get(3)?,
            publish_count: row.get(4)?,
        })
    })?;
    collect_rows(rows)
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use serde_json::json;

    use super::*;

    #[test]
    fn summarizes_publish_actions_and_published_state_rate_for_selected_time_range() {
        let connection = Connection::open_in_memory().unwrap();
        let from_ms = DateTime::parse_from_rfc3339("2026-06-30T00:00:00+09:00")
            .unwrap()
            .timestamp_millis();
        let to_ms = DateTime::parse_from_rfc3339("2026-06-30T23:59:59+09:00")
            .unwrap()
            .timestamp_millis();
        let events_sql = r#"
            (
              SELECT DATE '2026-06-30' AS dt, 'PUBLISH_FROM_DRAFT' AS event_kind
              UNION ALL
              SELECT DATE '2026-06-30', 'INITIAL_PUBLISH'
              UNION ALL
              SELECT DATE '2026-06-30', 'REPUBLISH_FROM_CLOSED'
              UNION ALL
              SELECT DATE '2026-06-30', 'UPDATE_PUBLISHED'
              UNION ALL
              SELECT DATE '2026-06-30', 'INITIAL_DRAFT'
              UNION ALL
              SELECT DATE '2026-06-30', 'SAVE_DRAFT'
              UNION ALL
              SELECT DATE '2026-06-30', 'UNPUBLISH_TO_DRAFT'
              UNION ALL
              SELECT DATE '2026-06-30', 'UNPUBLISH_TO_CLOSED'
              UNION ALL
              SELECT DATE '2026-06-30', 'REOPEN_TO_DRAFT'
              UNION ALL
              SELECT DATE '2026-06-30', 'ADD_DRAFT_TO_PUBLISHED'
              UNION ALL
              SELECT DATE '2026-06-30', 'DISCARD_DRAFT_ON_PUBLISHED'
              UNION ALL
              SELECT DATE '2026-06-30', 'DELETE_PUBLISHED'
              UNION ALL
              SELECT DATE '2026-06-30', NULL
              UNION ALL
              SELECT DATE '2026-06-29', 'PUBLISH_FROM_DRAFT'
              UNION ALL
              SELECT DATE '2026-07-01', 'INITIAL_PUBLISH'
            )
        "#;

        let rows =
            query_publish_action_summary_rows(&connection, events_sql, from_ms, to_ms).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].publish_count, 3);
        assert_eq!(rows[0].published_state_count, 4);
        assert_eq!(rows[0].state_arrival_count, 9);
        assert_eq!(rows[0].published_state_rate, Some(4.0 / 9.0));
        assert_eq!(
            serde_json::to_value(&rows[0]).unwrap(),
            json!({
                "publish_count": 3,
                "published_state_count": 4,
                "state_arrival_count": 9,
                "published_state_rate": 4.0 / 9.0
            })
        );
    }

    #[test]
    fn returns_null_published_state_rate_when_no_state_arrivals_exist() {
        let connection = Connection::open_in_memory().unwrap();
        let from_ms = DateTime::parse_from_rfc3339("2026-06-30T00:00:00+09:00")
            .unwrap()
            .timestamp_millis();
        let to_ms = DateTime::parse_from_rfc3339("2026-06-30T23:59:59+09:00")
            .unwrap()
            .timestamp_millis();
        let events_sql = r#"
            (
              SELECT
                DATE '2026-06-30' AS dt,
                'ADD_DRAFT_TO_PUBLISHED' AS event_kind
              UNION ALL
              SELECT
                DATE '2026-06-30',
                'DISCARD_DRAFT_ON_PUBLISHED'
              UNION ALL
              SELECT
                DATE '2026-06-30',
                'DELETE_PUBLISHED'
            )
        "#;

        let rows =
            query_publish_action_summary_rows(&connection, events_sql, from_ms, to_ms).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].publish_count, 0);
        assert_eq!(rows[0].published_state_count, 0);
        assert_eq!(rows[0].state_arrival_count, 0);
        assert_eq!(rows[0].published_state_rate, None);
    }

    #[test]
    fn queries_publish_action_trend_with_zero_filled_jst_days() {
        let connection = Connection::open_in_memory().unwrap();
        let from_ms = DateTime::parse_from_rfc3339("2026-06-29T00:00:00+09:00")
            .unwrap()
            .timestamp_millis();
        let to_ms = DateTime::parse_from_rfc3339("2026-07-01T23:59:59+09:00")
            .unwrap()
            .timestamp_millis();
        let events_sql = r#"
            (
              SELECT DATE '2026-06-29' AS dt, 'PUBLISH_FROM_DRAFT' AS event_kind
              UNION ALL
              SELECT DATE '2026-06-29', 'INITIAL_PUBLISH'
              UNION ALL
              SELECT DATE '2026-07-01', 'REPUBLISH_FROM_CLOSED'
              UNION ALL
              SELECT DATE '2026-07-02', 'PUBLISH_FROM_DRAFT'
            )
        "#;

        let rows =
            query_publish_action_trend_rows(&connection, events_sql, from_ms, to_ms).unwrap();

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].time, "2026-06-29T00:00:00+09:00");
        assert_eq!(rows[0].publish_from_draft_count, 1);
        assert_eq!(rows[0].initial_publish_count, 1);
        assert_eq!(rows[0].republish_from_closed_count, 0);
        assert_eq!(rows[0].publish_count, 2);
        assert_eq!(rows[1].time, "2026-06-30T00:00:00+09:00");
        assert_eq!(rows[1].publish_from_draft_count, 0);
        assert_eq!(rows[1].initial_publish_count, 0);
        assert_eq!(rows[1].republish_from_closed_count, 0);
        assert_eq!(rows[1].publish_count, 0);
        assert_eq!(rows[2].time, "2026-07-01T00:00:00+09:00");
        assert_eq!(rows[2].publish_from_draft_count, 0);
        assert_eq!(rows[2].initial_publish_count, 0);
        assert_eq!(rows[2].republish_from_closed_count, 1);
        assert_eq!(rows[2].publish_count, 1);
        assert!(rows.iter().all(|row| row.time.ends_with("T00:00:00+09:00")));
    }
}
