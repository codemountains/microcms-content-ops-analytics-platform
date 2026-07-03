use std::io::Cursor;
use std::sync::{Arc, LazyLock};

use arrow_array::{
    RecordBatch,
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
    events_to_parquet(std::slice::from_ref(event))
}

pub fn events_to_parquet(events: &[NormalizedEvent]) -> Result<Vec<u8>, IngestError> {
    if events.is_empty() {
        return Err(IngestError::Parquet("no events to write".to_owned()));
    }

    let schema = Arc::clone(&*EVENT_SCHEMA);
    let batch = events_record_batch(&schema, events)?;
    write_record_batch(schema, &batch)
}

fn events_record_batch(
    schema: &Arc<Schema>,
    events: &[NormalizedEvent],
) -> Result<RecordBatch, IngestError> {
    let len = events.len();
    let mut received_at = TimestampMicrosecondBuilder::with_capacity(len).with_timezone("UTC");
    let mut service = StringBuilder::with_capacity(len, len * 16);
    let mut api = StringBuilder::with_capacity(len, len * 8);
    let mut content_id = StringBuilder::with_capacity(len, len * 12);
    let mut event_type = StringBuilder::with_capacity(len, len * 8);
    let mut event_kind = StringBuilder::with_capacity(len, len * 16);
    let mut old_status = StringBuilder::with_capacity(len, len * 8);
    let mut new_status = StringBuilder::with_capacity(len, len * 8);
    let mut old_updated_at = TimestampMicrosecondBuilder::with_capacity(len).with_timezone("UTC");
    let mut new_updated_at = TimestampMicrosecondBuilder::with_capacity(len).with_timezone("UTC");
    let mut draft_created_at = TimestampMicrosecondBuilder::with_capacity(len).with_timezone("UTC");
    let mut content_created_at =
        TimestampMicrosecondBuilder::with_capacity(len).with_timezone("UTC");
    let mut content_published_at =
        TimestampMicrosecondBuilder::with_capacity(len).with_timezone("UTC");
    let mut raw_payload = StringBuilder::with_capacity(len, len * 32);

    for event in events {
        received_at.append_value(event.received_at.timestamp_micros());
        append_optional_string(&mut service, event.service.as_deref());
        append_optional_string(&mut api, event.api.as_deref());
        append_optional_string(&mut content_id, event.content_id.as_deref());
        append_optional_string(&mut event_type, event.event_type.as_deref());
        append_optional_string(&mut event_kind, event.event_kind.as_deref());
        append_optional_string(&mut old_status, event.old_status.as_deref());
        append_optional_string(&mut new_status, event.new_status.as_deref());
        append_optional_timestamp(&mut old_updated_at, event.old_updated_at);
        append_optional_timestamp(&mut new_updated_at, event.new_updated_at);
        append_optional_timestamp(&mut draft_created_at, event.draft_created_at);
        append_optional_timestamp(&mut content_created_at, event.content_created_at);
        append_optional_timestamp(&mut content_published_at, event.content_published_at);
        raw_payload.append_value(&event.raw_payload);
    }

    RecordBatch::try_new(
        Arc::clone(schema),
        vec![
            Arc::new(received_at.finish()),
            Arc::new(service.finish()),
            Arc::new(api.finish()),
            Arc::new(content_id.finish()),
            Arc::new(event_type.finish()),
            Arc::new(event_kind.finish()),
            Arc::new(old_status.finish()),
            Arc::new(new_status.finish()),
            Arc::new(old_updated_at.finish()),
            Arc::new(new_updated_at.finish()),
            Arc::new(draft_created_at.finish()),
            Arc::new(content_created_at.finish()),
            Arc::new(content_published_at.finish()),
            Arc::new(raw_payload.finish()),
        ],
    )
    .map_err(|error| IngestError::Parquet(error.to_string()))
}

fn write_record_batch(schema: Arc<Schema>, batch: &RecordBatch) -> Result<Vec<u8>, IngestError> {
    let mut buffer = Cursor::new(Vec::new());
    let props = WriterProperties::builder().build();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props))
        .map_err(|error| IngestError::Parquet(error.to_string()))?;
    writer
        .write(batch)
        .map_err(|error| IngestError::Parquet(error.to_string()))?;
    writer
        .close()
        .map_err(|error| IngestError::Parquet(error.to_string()))?;

    Ok(buffer.into_inner())
}

fn append_optional_string(builder: &mut StringBuilder, value: Option<&str>) {
    match value {
        Some(value) => builder.append_value(value),
        None => builder.append_null(),
    }
}

fn append_optional_timestamp(
    builder: &mut TimestampMicrosecondBuilder,
    value: Option<DateTime<Utc>>,
) {
    match value {
        Some(value) => builder.append_value(value.timestamp_micros()),
        None => builder.append_null(),
    }
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
        assert_eq!(event_kind.value(0), "PUBLISH_FROM_DRAFT");
        assert!(
            batch
                .schema()
                .fields()
                .iter()
                .any(|field| field.name() == "draft_created_at")
        );
        assert_eq!(batch.num_rows(), 1);
    }

    #[test]
    fn writes_multiple_events_to_single_parquet_file() {
        let received_at = DateTime::parse_from_rfc3339("2026-06-29T01:02:03Z")
            .unwrap()
            .with_timezone(&Utc);
        let first = normalize_payload(sample_body(), received_at).unwrap();
        let second = normalize_payload(sample_body(), received_at).unwrap();
        let parquet = events_to_parquet(&[first, second]).unwrap();

        let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(parquet)).unwrap();
        let mut reader = builder.build().unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert!(reader.next().is_none());
    }
}
