use std::fs;

use duckdb::{Connection, params};

pub(super) fn configure_connection(
    connection: &Connection,
    aws_region: &str,
    events_path: &str,
    extension_directory: &str,
    s3_endpoint: Option<&str>,
    s3_url_style: &str,
    s3_use_ssl: bool,
) -> duckdb::Result<()> {
    let _ = fs::create_dir_all(extension_directory);
    connection.execute_batch(&format!(
        "SET extension_directory = '{}';",
        sql_string_literal(extension_directory)
    ))?;

    if events_path.starts_with("s3://") {
        connection.execute_batch(
            r#"
            INSTALL httpfs;
            LOAD httpfs;
            "#,
        )?;
        connection.execute("SET s3_region = ?1", params![aws_region])?;
        connection.execute("SET s3_url_style = ?1", params![s3_url_style])?;
        connection.execute("SET s3_use_ssl = ?1", params![s3_use_ssl])?;
        if let Some(endpoint) = s3_endpoint {
            connection.execute(
                "SET s3_endpoint = ?1",
                params![normalize_duckdb_s3_endpoint(endpoint)],
            )?;
        }

        let endpoint_clause = s3_endpoint
            .map(|endpoint| {
                format!(
                    ",\n              ENDPOINT '{}'",
                    sql_string_literal(&normalize_duckdb_s3_endpoint(endpoint))
                )
            })
            .unwrap_or_default();
        connection.execute_batch(&format!(
            r#"
            CREATE OR REPLACE SECRET microcms_events_s3 (
              TYPE S3,
              PROVIDER CREDENTIAL_CHAIN,
              REGION '{}',
              URL_STYLE '{}',
              USE_SSL {}{}
            );
            "#,
            sql_string_literal(aws_region),
            sql_string_literal(s3_url_style),
            s3_use_ssl,
            endpoint_clause
        ))?;
    }

    Ok(())
}

fn normalize_duckdb_s3_endpoint(endpoint: &str) -> String {
    endpoint
        .trim()
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .trim_end_matches('/')
        .to_owned()
}

pub(super) fn read_parquet_sql(events_path: &str) -> String {
    format!(
        "read_parquet('{}', hive_partitioning = true, union_by_name = true)",
        sql_string_literal(events_path)
    )
}

pub(crate) fn sql_string_literal(value: &str) -> String {
    value.replace('\'', "''")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_events_path_for_read_parquet_sql() {
        assert_eq!(
            read_parquet_sql("s3://bucket/path/**/*.parquet"),
            "read_parquet('s3://bucket/path/**/*.parquet', hive_partitioning = true, union_by_name = true)"
        );
        assert_eq!(
            read_parquet_sql("s3://bucket/it's/**/*.parquet"),
            "read_parquet('s3://bucket/it''s/**/*.parquet', hive_partitioning = true, union_by_name = true)"
        );
    }

    #[test]
    fn normalizes_duckdb_s3_endpoint() {
        assert_eq!(
            normalize_duckdb_s3_endpoint("http://floci:4566/"),
            "floci:4566"
        );
        assert_eq!(
            normalize_duckdb_s3_endpoint("https://localhost:4566"),
            "localhost:4566"
        );
    }
}
