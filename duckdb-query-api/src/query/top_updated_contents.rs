use duckdb::Connection;

use super::{JST_OFFSET_INTERVAL, TopUpdatedContentRow, collect_rows};

pub(crate) fn query_top_updated_contents_rows(
    connection: &Connection,
    events_sql: &str,
    days: u32,
    limit: u32,
) -> duckdb::Result<Vec<TopUpdatedContentRow>> {
    let days_minus_one = days - 1;
    let sql = format!(
        r#"
        SELECT
          api,
          content_id,
          COUNT(*) AS count,
          strftime(MAX(received_at), '%Y-%m-%dT%H:%M:%SZ') AS last_event_at
        FROM {events_sql}
        WHERE
          dt >= CAST(CAST(current_timestamp AS TIMESTAMP) + INTERVAL '{JST_OFFSET_INTERVAL}' AS DATE) - INTERVAL '{days_minus_one} DAY'
          AND event_type IN ('new', 'edit')
          AND content_id IS NOT NULL
        GROUP BY api, content_id
        ORDER BY count DESC, last_event_at DESC
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
    use duckdb::Connection;

    use super::*;

    #[test]
    fn formats_last_event_at_as_rfc3339_for_grafana_timestamp_fields() {
        let connection = Connection::open_in_memory().unwrap();
        let events_sql = r#"
            (
              SELECT
                CAST(CAST(current_timestamp AS TIMESTAMP) + INTERVAL '9 HOURS' AS DATE) AS dt,
                TIMESTAMP '2026-06-29 12:00:00' AS received_at,
                'blogs' AS api,
                'content-1' AS content_id,
                'edit' AS event_type
              UNION ALL
              SELECT
                CAST(CAST(current_timestamp AS TIMESTAMP) + INTERVAL '9 HOURS' AS DATE),
                TIMESTAMP '2026-06-29 13:30:45',
                'blogs',
                'content-1',
                'edit'
            )
        "#;

        let rows = query_top_updated_contents_rows(&connection, events_sql, 1, 20).unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].last_event_at.as_deref(),
            Some("2026-06-29T13:30:45Z")
        );
    }
}
