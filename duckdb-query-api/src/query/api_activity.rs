use duckdb::Connection;

use super::event_kind;
use super::{ApiActivityRow, JST_OFFSET_INTERVAL, collect_rows};

pub(crate) fn query_api_activity_rows(
    connection: &Connection,
    events_sql: &str,
    days: u32,
) -> duckdb::Result<Vec<ApiActivityRow>> {
    let days_minus_one = days - 1;
    let initial_draft = event_kind::INITIAL_DRAFT;
    let save_draft = event_kind::SAVE_DRAFT;
    let publish_from_draft = event_kind::PUBLISH_FROM_DRAFT;
    let initial_publish = event_kind::INITIAL_PUBLISH;
    let update_published = event_kind::UPDATE_PUBLISHED;
    let add_draft_to_published = event_kind::ADD_DRAFT_TO_PUBLISHED;
    let discard_draft_on_published = event_kind::DISCARD_DRAFT_ON_PUBLISHED;
    let unpublish_to_draft = event_kind::UNPUBLISH_TO_DRAFT;
    let unpublish_to_closed = event_kind::UNPUBLISH_TO_CLOSED;
    let reopen_to_draft = event_kind::REOPEN_TO_DRAFT;
    let republish_from_closed = event_kind::REPUBLISH_FROM_CLOSED;
    let delete_draft = event_kind::DELETE_DRAFT;
    let delete_published = event_kind::DELETE_PUBLISHED;
    let delete_closed = event_kind::DELETE_CLOSED;
    let sql = format!(
        r#"
        SELECT
          api,
          SUM(CASE WHEN event_kind = '{initial_draft}' THEN 1 ELSE 0 END) AS initial_draft_count,
          SUM(CASE WHEN event_kind = '{save_draft}' THEN 1 ELSE 0 END) AS save_draft_count,
          SUM(CASE WHEN event_kind = '{publish_from_draft}' THEN 1 ELSE 0 END) AS publish_from_draft_count,
          SUM(CASE WHEN event_kind = '{initial_publish}' THEN 1 ELSE 0 END) AS initial_publish_count,
          SUM(CASE WHEN event_kind = '{update_published}' THEN 1 ELSE 0 END) AS update_published_count,
          SUM(CASE WHEN event_kind = '{add_draft_to_published}' THEN 1 ELSE 0 END) AS add_draft_to_published_count,
          SUM(CASE WHEN event_kind = '{discard_draft_on_published}' THEN 1 ELSE 0 END) AS discard_draft_on_published_count,
          SUM(CASE WHEN event_kind = '{unpublish_to_draft}' THEN 1 ELSE 0 END) AS unpublish_to_draft_count,
          SUM(CASE WHEN event_kind = '{unpublish_to_closed}' THEN 1 ELSE 0 END) AS unpublish_to_closed_count,
          SUM(CASE WHEN event_kind = '{reopen_to_draft}' THEN 1 ELSE 0 END) AS reopen_to_draft_count,
          SUM(CASE WHEN event_kind = '{republish_from_closed}' THEN 1 ELSE 0 END) AS republish_from_closed_count,
          SUM(CASE WHEN event_kind = '{delete_draft}' THEN 1 ELSE 0 END) AS delete_draft_count,
          SUM(CASE WHEN event_kind = '{delete_published}' THEN 1 ELSE 0 END) AS delete_published_count,
          SUM(CASE WHEN event_kind = '{delete_closed}' THEN 1 ELSE 0 END) AS delete_closed_count,
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
            initial_draft_count: row.get(1)?,
            save_draft_count: row.get(2)?,
            publish_from_draft_count: row.get(3)?,
            initial_publish_count: row.get(4)?,
            update_published_count: row.get(5)?,
            add_draft_to_published_count: row.get(6)?,
            discard_draft_on_published_count: row.get(7)?,
            unpublish_to_draft_count: row.get(8)?,
            unpublish_to_closed_count: row.get(9)?,
            reopen_to_draft_count: row.get(10)?,
            republish_from_closed_count: row.get(11)?,
            delete_draft_count: row.get(12)?,
            delete_published_count: row.get(13)?,
            delete_closed_count: row.get(14)?,
            total_count: row.get(15)?,
        })
    })?;
    collect_rows(rows)
}
