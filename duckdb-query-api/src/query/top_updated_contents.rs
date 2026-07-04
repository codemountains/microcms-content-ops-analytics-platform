use duckdb::Connection;

use super::{TopUpdatedContentRow, collect_rows, time_range_bounds_cte};

pub(crate) fn query_top_updated_contents_rows(
    connection: &Connection,
    events_sql: &str,
    from_ms: i64,
    to_ms: i64,
    limit: u32,
) -> duckdb::Result<Vec<TopUpdatedContentRow>> {
    let bounds = time_range_bounds_cte(from_ms, to_ms);
    let sql = format!(
        r#"
        WITH {bounds}
        SELECT
          api,
          content_id,
          COUNT(*) AS count,
          strftime(MAX(received_at), '%Y-%m-%dT%H:%M:%SZ') AS last_event_at
        FROM {events_sql}
        WHERE
          dt >= (SELECT start_date FROM bounds)
          AND dt <= (SELECT end_date FROM bounds)
          AND event_type IN ('new', 'edit')
          AND content_id IS NOT NULL
        GROUP BY api, content_id
        ORDER BY count DESC, MAX(received_at) DESC
        LIMIT {limit}
        "#
    );

    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([], |row| {
        Ok(TopUpdatedContentRow {
            api: row.get(0)?,
            content_id: row.get(1)?,
            count: row.get(2)?,
            last_event_at: row.get(3)?,
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
    fn formats_last_event_at_as_rfc3339_for_grafana_timestamp_fields() {
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
                TIMESTAMP '2026-06-29 12:00:00' AS received_at,
                'blogs' AS api,
                'content-1' AS content_id,
                'edit' AS event_type
              UNION ALL
              SELECT
                DATE '2026-06-30',
                TIMESTAMP '2026-06-29 13:30:45',
                'blogs',
                'content-1',
                'edit'
              UNION ALL
              SELECT
                DATE '2026-06-29',
                TIMESTAMP '2026-06-29 14:30:45',
                'blogs',
                'content-1',
                'edit'
            )
        "#;

        let rows =
            query_top_updated_contents_rows(&connection, events_sql, from_ms, to_ms, 20).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].count, 2);
        assert_eq!(
            rows[0].last_event_at.as_deref(),
            Some("2026-06-29T13:30:45Z")
        );
    }

    #[test]
    fn orders_tied_counts_by_full_precision_last_received_at() {
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
                TIMESTAMP '2026-06-29 13:30:45.100000' AS received_at,
                'blogs' AS api,
                'content-older' AS content_id,
                'edit' AS event_type
              UNION ALL
              SELECT
                DATE '2026-06-30',
                TIMESTAMP '2026-06-29 13:30:45.900000',
                'blogs',
                'content-newer',
                'edit'
            )
        "#;

        let rows =
            query_top_updated_contents_rows(&connection, events_sql, from_ms, to_ms, 20).unwrap();

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].content_id.as_deref(), Some("content-newer"));
        assert_eq!(rows[1].content_id.as_deref(), Some("content-older"));
        assert_eq!(
            rows[0].last_event_at.as_deref(),
            rows[1].last_event_at.as_deref()
        );
    }
}
