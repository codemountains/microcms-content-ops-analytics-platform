use std::io::Cursor;
use std::sync::{Arc, LazyLock};

use arrow_array::{
    ArrayRef, RecordBatch,
    builder::{StringBuilder, TimestampMicrosecondBuilder},
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::{DateTime, Utc};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use crate::{IngestError, NormalizedEvent};

static EVENT_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new(
            "received_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        ),
        Field::new("service", DataType::Utf8, true),
        Field::new("api", DataType::Utf8, true),
        Field::new("content_id", DataType::Utf8, true),
        Field::new("event_type", DataType::Utf8, true),
        Field::new("event_kind", DataType::Utf8, true),
        Field::new("old_status", DataType::Utf8, true),
        Field::new("new_status", DataType::Utf8, true),
        Field::new(
            "old_updated_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "new_updated_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "draft_created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "content_created_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new(
            "content_published_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
        Field::new("raw_payload", DataType::Utf8, false),
    ]))
});

pub fn event_to_parquet(event: &NormalizedEvent) -> Result<Vec<u8>, IngestError> {
    let schema = Arc::clone(&*EVENT_SCHEMA);
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            timestamp_required(event.received_at),
            string_optional(event.service.as_deref()),
            string_optional(event.api.as_deref()),
            string_optional(event.content_id.as_deref()),
            string_optional(event.event_type.as_deref()),
            string_optional(event.event_kind.as_deref()),
            string_optional(event.old_status.as_deref()),
            string_optional(event.new_status.as_deref()),
            timestamp_optional(event.old_updated_at),
            timestamp_optional(event.new_updated_at),
            timestamp_optional(event.draft_created_at),
            timestamp_optional(event.content_created_at),
            timestamp_optional(event.content_published_at),
            string_required(&event.raw_payload),
        ],
    )
    .map_err(|error| IngestError::Parquet(error.to_string()))?;

    let mut buffer = Cursor::new(Vec::new());
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props))
        .map_err(|error| IngestError::Parquet(error.to_string()))?;
    writer
        .write(&batch)
        .map_err(|error| IngestError::Parquet(error.to_string()))?;
    writer
        .close()
        .map_err(|error| IngestError::Parquet(error.to_string()))?;

    Ok(buffer.into_inner())
}

fn string_required(value: &str) -> ArrayRef {
    let mut builder = StringBuilder::with_capacity(1, value.len());
    builder.append_value(value);
    Arc::new(builder.finish())
}

fn string_optional(value: Option<&str>) -> ArrayRef {
    let mut builder = StringBuilder::with_capacity(1, value.map(str::len).unwrap_or_default());
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
    Arc::new(builder.finish())
}

fn timestamp_required(value: DateTime<Utc>) -> ArrayRef {
    let mut builder = TimestampMicrosecondBuilder::with_capacity(1).with_timezone("UTC");
    builder.append_value(value.timestamp_micros());
    Arc::new(builder.finish())
}

fn timestamp_optional(value: Option<DateTime<Utc>>) -> ArrayRef {
    let mut builder = TimestampMicrosecondBuilder::with_capacity(1).with_timezone("UTC");
    match value {
        Some(value) => builder.append_value(value.timestamp_micros()),
        None => builder.append_null(),
    }
    Arc::new(builder.finish())
}

#[cfg(test)]
mod tests {
    use arrow_array::{Array, StringArray, TimestampMicrosecondArray};
    use bytes::Bytes;
    use chrono::{DateTime, Utc};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;
    use crate::normalize_payload;

    fn sample_body() -> &'static [u8] {
        br#"{
          "service": "example-service",
          "api": "blogs",
          "id": "content-id",
          "type": "edit",
          "contents": {
            "old": {"status": "DRAFT", "updatedAt": "2026-06-28T12:00:00Z"},
            "new": {"status": "PUBLISH", "updatedAt": "2026-06-29T12:00:00Z"}
          }
        }"#
    }

    #[test]
    fn writes_single_event_parquet() {
        let received_at = DateTime::parse_from_rfc3339("2026-06-29T01:02:03Z")
            .unwrap()
            .with_timezone(&Utc);
        let event = normalize_payload(sample_body(), received_at).unwrap();
        let parquet = event_to_parquet(&event).unwrap();
        assert!(!parquet.is_empty());

        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(parquet)).unwrap();
        let mut reader = builder.build().unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert!(
            !batch
                .schema()
                .fields()
                .iter()
                .any(|field| field.name() == "title")
        );

        let received_at = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(received_at.value(0), event.received_at.timestamp_micros());

        let api = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(api.value(0), "blogs");

        let event_kind = batch
            .column(5)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(event_kind.value(0), "FIRST_PUBLISH");
        assert!(
            batch
                .schema()
                .fields()
                .iter()
                .any(|field| field.name() == "draft_created_at")
        );
        assert_eq!(batch.num_rows(), 1);
    }
}
