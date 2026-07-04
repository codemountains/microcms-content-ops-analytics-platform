use duckdb::Connection;

use super::{CalendarHeatmapRow, PARTITION_TIME_JST_SUFFIX, collect_rows, time_range_bounds_cte};

pub(crate) fn query_calendar_heatmap_rows(
    connection: &Connection,
    events_sql: &str,
    from_ms: i64,
    to_ms: i64,
) -> duckdb::Result<Vec<CalendarHeatmapRow>> {
    let bounds = time_range_bounds_cte(from_ms, to_ms);
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
            COUNT(*) AS event_count
          FROM {events_sql}
          WHERE
            dt >= (SELECT start_date FROM bounds)
            AND dt <= (SELECT end_date FROM bounds)
          GROUP BY dt
        )
        SELECT
          CONCAT(CAST(calendar.dt AS VARCHAR), '{PARTITION_TIME_JST_SUFFIX}') AS time,
          COALESCE(daily.event_count, 0) AS value
        FROM calendar
        LEFT JOIN daily ON daily.dt = calendar.dt
        ORDER BY calendar.dt
        "#
    );

    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([], |row| {
        Ok(CalendarHeatmapRow {
            time: row.get(0)?,
            value: row.get(1)?,
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
    fn queries_calendar_heatmap_with_jst_day_bounds() {
        let connection = Connection::open_in_memory().unwrap();
        let from_ms = DateTime::parse_from_rfc3339("2026-06-30T00:00:00+09:00")
            .unwrap()
            .timestamp_millis();
        let to_ms = DateTime::parse_from_rfc3339("2026-06-30T23:59:59+09:00")
            .unwrap()
            .timestamp_millis();

        let rows = query_calendar_heatmap_rows(
            &connection,
            "(SELECT DATE '2026-06-30' AS dt UNION ALL SELECT DATE '2026-06-30' AS dt)",
            from_ms,
            to_ms,
        )
        .unwrap();

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].time, "2026-06-30T00:00:00+09:00");
        assert_eq!(rows[0].value, 2);
    }
}
