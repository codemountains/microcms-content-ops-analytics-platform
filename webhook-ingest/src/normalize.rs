use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::IngestError;

pub(crate) const EVENT_KIND_CREATE_DRAFT: &str = "CREATE_DRAFT";
pub(crate) const EVENT_KIND_CREATE_PUBLISH: &str = "CREATE_PUBLISH";
pub(crate) const EVENT_KIND_FIRST_PUBLISH: &str = "FIRST_PUBLISH";
pub(crate) const EVENT_KIND_UPDATE_PUBLISH: &str = "UPDATE_PUBLISH";
pub(crate) const EVENT_KIND_UNPUBLISH: &str = "UNPUBLISH";
pub(crate) const EVENT_KIND_DELETE: &str = "DELETE";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NormalizedEvent {
    pub received_at: DateTime<Utc>,
    pub service: Option<String>,
    pub api: Option<String>,
    pub content_id: Option<String>,
    pub event_type: Option<String>,
    pub event_kind: Option<String>,
    pub old_status: Option<String>,
    pub new_status: Option<String>,
    pub old_updated_at: Option<DateTime<Utc>>,
    pub new_updated_at: Option<DateTime<Utc>>,
    pub draft_created_at: Option<DateTime<Utc>>,
    pub content_created_at: Option<DateTime<Utc>>,
    pub content_published_at: Option<DateTime<Utc>>,
    pub raw_payload: String,
}

#[derive(Debug, Deserialize)]
struct MicrocmsWebhook {
    service: Option<String>,
    api: Option<String>,
    id: Option<String>,
    #[serde(rename = "type")]
    event_type: Option<String>,
    contents: Option<WebhookContents>,
}

#[derive(Debug, Deserialize)]
struct WebhookContents {
    old: Option<Value>,
    new: Option<Value>,
}

pub fn normalize_payload(
    body: &[u8],
    received_at: DateTime<Utc>,
) -> Result<NormalizedEvent, IngestError> {
    let raw_payload = std::str::from_utf8(body)
        .map_err(|_| IngestError::InvalidBody)?
        .to_owned();
    let payload: MicrocmsWebhook = serde_json::from_str(&raw_payload)?;
    let old = payload
        .contents
        .as_ref()
        .and_then(|contents| contents.old.as_ref());
    let new = payload
        .contents
        .as_ref()
        .and_then(|contents| contents.new.as_ref());

    let old_statuses = extract_statuses(old);
    let new_statuses = extract_statuses(new);
    let event_kind = derive_event_kind(payload.event_type.as_deref(), &old_statuses, &new_statuses);

    Ok(NormalizedEvent {
        received_at,
        service: payload.service,
        api: payload.api,
        content_id: payload.id,
        event_type: payload.event_type,
        event_kind,
        old_status: status_csv(&old_statuses),
        new_status: status_csv(&new_statuses),
        old_updated_at: extract_timestamp(old, &["updatedAt", "updated_at"]),
        new_updated_at: extract_timestamp(new, &["updatedAt", "updated_at"]),
        draft_created_at: extract_nested_timestamp(new, &["draftValue", "createdAt"]),
        content_created_at: extract_nested_timestamp(new, &["publishValue", "createdAt"]),
        content_published_at: extract_nested_timestamp(new, &["publishValue", "publishedAt"]),
        raw_payload,
    })
}

fn derive_event_kind(
    event_type: Option<&str>,
    old_statuses: &[String],
    new_statuses: &[String],
) -> Option<String> {
    let old_published = has_status(old_statuses, "PUBLISH");
    let new_published = has_status(new_statuses, "PUBLISH");
    let new_draft = has_status(new_statuses, "DRAFT");

    let event_kind = match event_type {
        Some("delete") => EVENT_KIND_DELETE,
        Some("new") if new_published => EVENT_KIND_CREATE_PUBLISH,
        Some("new") if new_draft => EVENT_KIND_CREATE_DRAFT,
        Some("edit") if !old_published && new_published => EVENT_KIND_FIRST_PUBLISH,
        Some("edit") if old_published && new_published => EVENT_KIND_UPDATE_PUBLISH,
        Some("edit") if old_published && !new_published => EVENT_KIND_UNPUBLISH,
        _ => return None,
    };

    Some(event_kind.to_owned())
}

fn has_status(statuses: &[String], expected: &str) -> bool {
    statuses.iter().any(|status| status == expected)
}

fn extract_statuses(value: Option<&Value>) -> Vec<String> {
    let Some(status) = value
        .and_then(|value| value.as_object())
        .and_then(|object| object.get("status"))
    else {
        return Vec::new();
    };

    let mut statuses = match status {
        Value::String(status) => vec![status.to_owned()],
        Value::Array(statuses) => statuses
            .iter()
            .filter_map(|status| status.as_str().map(ToOwned::to_owned))
            .collect(),
        _ => Vec::new(),
    };
    statuses.sort();
    statuses.dedup();
    statuses
}

fn status_csv(statuses: &[String]) -> Option<String> {
    if statuses.is_empty() {
        None
    } else {
        Some(statuses.join(","))
    }
}

fn extract_string(value: Option<&Value>, keys: &[&str]) -> Option<String> {
    let object = value?.as_object()?;
    keys.iter()
        .filter_map(|key| object.get(*key))
        .find_map(|value| value.as_str().map(ToOwned::to_owned))
}

fn extract_timestamp(value: Option<&Value>, keys: &[&str]) -> Option<DateTime<Utc>> {
    extract_string(value, keys)
        .and_then(|value| DateTime::parse_from_rfc3339(&value).ok())
        .map(|value| value.with_timezone(&Utc))
}

fn extract_nested_string(value: Option<&Value>, path: &[&str]) -> Option<String> {
    let mut current = value?;
    for key in path {
        current = current.as_object()?.get(*key)?;
    }
    current.as_str().map(ToOwned::to_owned)
}

fn extract_nested_timestamp(value: Option<&Value>, path: &[&str]) -> Option<DateTime<Utc>> {
    extract_nested_string(value, path)
        .and_then(|value| DateTime::parse_from_rfc3339(&value).ok())
        .map(|value| value.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn body_with_statuses(
        event_type: &str,
        old_status: Option<&str>,
        new_status: Option<&str>,
    ) -> Vec<u8> {
        let old = old_status
            .map(|status| format!(r#"{{"status": {status}}}"#))
            .unwrap_or_else(|| "null".to_owned());
        let new = new_status
            .map(|status| {
                format!(
                    r#"{{
                      "status": {status},
                      "publishValue": {{
                        "createdAt": "2026-06-27T12:00:00Z",
                        "publishedAt": "2026-06-29T12:00:00Z"
                      }}
                    }}"#
                )
            })
            .unwrap_or_else(|| "null".to_owned());

        format!(
            r#"{{
              "service": "example-service",
              "api": "blogs",
              "id": "content-id",
              "type": "{event_type}",
              "contents": {{
                "old": {old},
                "new": {new}
              }}
            }}"#
        )
        .into_bytes()
    }

    #[test]
    fn normalizes_microcms_payload() {
        let received_at = DateTime::parse_from_rfc3339("2026-06-29T01:02:03Z")
            .unwrap()
            .with_timezone(&Utc);
        let event = normalize_payload(sample_body(), received_at).unwrap();

        assert_eq!(event.service.as_deref(), Some("example-service"));
        assert_eq!(event.api.as_deref(), Some("blogs"));
        assert_eq!(event.content_id.as_deref(), Some("content-id"));
        assert_eq!(event.event_type.as_deref(), Some("edit"));
        assert_eq!(event.old_status.as_deref(), Some("DRAFT"));
        assert_eq!(event.new_status.as_deref(), Some("PUBLISH"));
        assert_eq!(
            event.new_updated_at.unwrap().to_rfc3339(),
            "2026-06-29T12:00:00+00:00"
        );
    }

    #[test]
    fn normalizes_status_arrays_event_kind_and_publish_timestamps() {
        let received_at = DateTime::parse_from_rfc3339("2026-06-29T01:02:03Z")
            .unwrap()
            .with_timezone(&Utc);
        let body = body_with_statuses("edit", Some(r#"["DRAFT"]"#), Some(r#"["PUBLISH"]"#));
        let event = normalize_payload(&body, received_at).unwrap();

        assert_eq!(event.old_status.as_deref(), Some("DRAFT"));
        assert_eq!(event.new_status.as_deref(), Some("PUBLISH"));
        assert_eq!(event.event_kind.as_deref(), Some(EVENT_KIND_FIRST_PUBLISH));
        assert_eq!(
            event.content_created_at.unwrap().to_rfc3339(),
            "2026-06-27T12:00:00+00:00"
        );
        assert_eq!(
            event.content_published_at.unwrap().to_rfc3339(),
            "2026-06-29T12:00:00+00:00"
        );
    }

    #[test]
    fn normalizes_draft_created_at_from_new_draft_value() {
        let received_at = DateTime::parse_from_rfc3339("2026-06-29T01:02:03Z")
            .unwrap()
            .with_timezone(&Utc);
        let body = br#"{
          "service": "example-service",
          "api": "blogs",
          "id": "content-id",
          "type": "new",
          "contents": {
            "old": null,
            "new": {
              "status": ["DRAFT"],
              "updatedAt": "2026-06-29T12:00:00Z",
              "publishValue": null,
              "draftValue": {
                "createdAt": "2026-06-27T12:00:00Z",
                "updatedAt": "2026-06-29T12:00:00Z"
              }
            }
          }
        }"#;
        let event = normalize_payload(body, received_at).unwrap();

        assert_eq!(event.event_kind.as_deref(), Some(EVENT_KIND_CREATE_DRAFT));
        assert_eq!(
            event.draft_created_at.unwrap().to_rfc3339(),
            "2026-06-27T12:00:00+00:00"
        );
    }

    #[test]
    fn derives_event_kind_for_supported_content_transitions() {
        let received_at = DateTime::parse_from_rfc3339("2026-06-29T01:02:03Z")
            .unwrap()
            .with_timezone(&Utc);
        let cases = [
            (
                "new",
                None,
                Some(r#"["DRAFT"]"#),
                Some(EVENT_KIND_CREATE_DRAFT),
            ),
            (
                "new",
                None,
                Some(r#"["PUBLISH"]"#),
                Some(EVENT_KIND_CREATE_PUBLISH),
            ),
            (
                "edit",
                Some(r#"["DRAFT"]"#),
                Some(r#"["PUBLISH"]"#),
                Some(EVENT_KIND_FIRST_PUBLISH),
            ),
            (
                "edit",
                Some(r#"["PUBLISH"]"#),
                Some(r#"["PUBLISH"]"#),
                Some(EVENT_KIND_UPDATE_PUBLISH),
            ),
            (
                "edit",
                Some(r#"["PUBLISH"]"#),
                Some(r#"["DRAFT"]"#),
                Some(EVENT_KIND_UNPUBLISH),
            ),
            (
                "delete",
                Some(r#"["PUBLISH"]"#),
                None,
                Some(EVENT_KIND_DELETE),
            ),
            ("edit", Some(r#"["DRAFT"]"#), Some(r#"["DRAFT"]"#), None),
        ];

        for (event_type, old_status, new_status, expected) in cases {
            let body = body_with_statuses(event_type, old_status, new_status);
            let event = normalize_payload(&body, received_at).unwrap();
            assert_eq!(event.event_kind.as_deref(), expected);
        }
    }
}
