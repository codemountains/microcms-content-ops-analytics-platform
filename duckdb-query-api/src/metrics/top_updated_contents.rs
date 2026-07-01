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
          CAST(MAX(received_at) AS VARCHAR) AS last_event_at
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
