use duckdb::Connection;

use super::event_kind;
use super::{
    JST_OFFSET_INTERVAL, PARTITION_TIME_JST_SUFFIX, PublishActionSummaryRow, PublishActionTrendRow,
    collect_rows,
};

pub(crate) fn query_publish_action_summary_rows(
    connection: &Connection,
    events_sql: &str,
    days: u32,
) -> duckdb::Result<Vec<PublishActionSummaryRow>> {
    let days_minus_one = days - 1;
    let publish_from_draft = event_kind::PUBLISH_FROM_DRAFT;
    let initial_publish = event_kind::INITIAL_PUBLISH;
    let republish_from_closed = event_kind::REPUBLISH_FROM_CLOSED;
    let sql = format!(
        r#"
        WITH bounds AS (
          SELECT
            CAST(CAST(current_timestamp AS TIMESTAMP) + INTERVAL '{JST_OFFSET_INTERVAL}' AS DATE) - INTERVAL '{days_minus_one} DAY' AS start_date,
            CAST(CAST(current_timestamp AS TIMESTAMP) + INTERVAL '{JST_OFFSET_INTERVAL}' AS DATE) AS end_date
        )
        SELECT
          COALESCE(SUM(CASE WHEN event_kind IN ('{publish_from_draft}', '{initial_publish}', '{republish_from_closed}') THEN 1 ELSE 0 END), 0) AS publish_count,
          COUNT(*) AS total_count,
          CASE
            WHEN COUNT(*) = 0 THEN NULL
            ELSE COALESCE(SUM(CASE WHEN event_kind IN ('{publish_from_draft}', '{initial_publish}', '{republish_from_closed}') THEN 1 ELSE 0 END), 0)::DOUBLE / COUNT(*)
          END AS publish_rate
        FROM {events_sql}
        WHERE
          dt >= (SELECT start_date FROM bounds)
          AND dt <= (SELECT end_date FROM bounds)
        "#
    );

    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([], |row| {
        Ok(PublishActionSummaryRow {
            publish_count: row.get(0)?,
            total_count: row.get(1)?,
            publish_rate: row.get(2)?,
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
    let publish_from_draft = event_kind::PUBLISH_FROM_DRAFT;
    let initial_publish = event_kind::INITIAL_PUBLISH;
    let republish_from_closed = event_kind::REPUBLISH_FROM_CLOSED;
    let sql = format!(
        r#"
        WITH bounds AS (
          SELECT
            CAST(epoch_ms({from_ms}) + INTERVAL '{JST_OFFSET_INTERVAL}' AS DATE) AS start_date,
            CAST(epoch_ms({to_ms}) + INTERVAL '{JST_OFFSET_INTERVAL}' AS DATE) AS end_date
        ),
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
    fn summarizes_publish_actions_and_rate_for_selected_days() {
        let connection = Connection::open_in_memory().unwrap();
        let current_jst_date =
            "CAST(CAST(current_timestamp AS TIMESTAMP) + INTERVAL '9 HOURS' AS DATE)";
        let events_sql = r#"
            (
              SELECT {current_jst_date} AS dt, 'PUBLISH_FROM_DRAFT' AS event_kind
              UNION ALL
              SELECT {current_jst_date}, 'INITIAL_PUBLISH'
              UNION ALL
              SELECT {current_jst_date}, 'REPUBLISH_FROM_CLOSED'
              UNION ALL
              SELECT {current_jst_date}, 'UPDATE_PUBLISHED'
              UNION ALL
              SELECT {current_jst_date} - INTERVAL 1 DAY, 'PUBLISH_FROM_DRAFT'
              UNION ALL
              SELECT {current_jst_date} + INTERVAL 1 DAY, 'INITIAL_PUBLISH'
            )
        "#
        .replace("{current_jst_date}", current_jst_date);

        let rows = query_publish_action_summary_rows(&connection, &events_sql, 1).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].publish_count, 3);
        assert_eq!(rows[0].total_count, 4);
        assert_eq!(rows[0].publish_rate, Some(3.0 / 4.0));
        assert_eq!(
            serde_json::to_value(&rows[0]).unwrap(),
            json!({
                "publish_count": 3,
                "total_count": 4,
                "publish_rate": 3.0 / 4.0
            })
        );
    }

    #[test]
    fn returns_null_publish_rate_when_no_events_exist() {
        let connection = Connection::open_in_memory().unwrap();
        let events_sql = r#"
            (
              SELECT
                CAST(CAST(current_timestamp AS TIMESTAMP) + INTERVAL '9 HOURS' AS DATE) AS dt,
                'INITIAL_PUBLISH' AS event_kind
              WHERE FALSE
            )
        "#;

        let rows = query_publish_action_summary_rows(&connection, events_sql, 1).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].publish_count, 0);
        assert_eq!(rows[0].total_count, 0);
        assert_eq!(rows[0].publish_rate, None);
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
