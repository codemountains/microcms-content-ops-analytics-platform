use duckdb::Connection;

use super::event_kind;
use super::{ApiActivityRow, JST_OFFSET_INTERVAL, collect_rows};

pub(crate) fn query_api_activity_rows(
    connection: &Connection,
    events_sql: &str,
    days: u32,
) -> duckdb::Result<Vec<ApiActivityRow>> {
    let days_minus_one = days - 1;
    let create_draft = event_kind::CREATE_DRAFT;
    let create_publish = event_kind::CREATE_PUBLISH;
    let first_publish = event_kind::FIRST_PUBLISH;
    let update_publish = event_kind::UPDATE_PUBLISH;
    let unpublish = event_kind::UNPUBLISH;
    let delete = event_kind::DELETE;
    let sql = format!(
        r#"
        SELECT
          api,
          SUM(CASE WHEN event_kind = '{create_draft}' THEN 1 ELSE 0 END) AS create_draft_count,
          SUM(CASE WHEN event_kind = '{create_publish}' THEN 1 ELSE 0 END) AS create_publish_count,
          SUM(CASE WHEN event_kind = '{first_publish}' THEN 1 ELSE 0 END) AS first_publish_count,
          SUM(CASE WHEN event_kind = '{update_publish}' THEN 1 ELSE 0 END) AS update_publish_count,
          SUM(CASE WHEN event_kind = '{unpublish}' THEN 1 ELSE 0 END) AS unpublish_count,
          SUM(CASE WHEN event_kind = '{delete}' THEN 1 ELSE 0 END) AS delete_count,
          COUNT(*) AS total_count
        FROM {events_sql}
        WHERE dt >= CAST(CAST(current_timestamp AS TIMESTAMP) + INTERVAL '{JST_OFFSET_INTERVAL}' AS DATE) - INTERVAL '{days_minus_one} DAY'
        GROUP BY api
        ORDER BY total_count DESC, api
        "#
    );

    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([], |row| {
        Ok(ApiActivityRow {
            api: row.get(0)?,
            create_draft_count: row.get(1)?,
            create_publish_count: row.get(2)?,
            first_publish_count: row.get(3)?,
            update_publish_count: row.get(4)?,
            unpublish_count: row.get(5)?,
            delete_count: row.get(6)?,
            total_count: row.get(7)?,
        })
    })?;
    collect_rows(rows)
}
