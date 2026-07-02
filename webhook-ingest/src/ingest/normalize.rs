mod event_kind;
mod extract;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use self::event_kind::derive_event_kind;
use self::extract::{extract_nested_timestamp, extract_statuses, extract_timestamp, status_csv};
use crate::IngestError;

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

#[cfg(test)]
mod tests {
    use super::event_kind::{EVENT_KIND_CREATE_DRAFT, EVENT_KIND_FIRST_PUBLISH};
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
            ("new", None, Some(r#"["DRAFT"]"#), Some("CREATE_DRAFT")),
            ("new", None, Some(r#"["PUBLISH"]"#), Some("CREATE_PUBLISH")),
            (
                "edit",
                Some(r#"["DRAFT"]"#),
                Some(r#"["PUBLISH"]"#),
                Some("FIRST_PUBLISH"),
            ),
            (
                "edit",
                Some(r#"["PUBLISH"]"#),
                Some(r#"["PUBLISH"]"#),
                Some("UPDATE_PUBLISH"),
            ),
            (
                "edit",
                Some(r#"["PUBLISH"]"#),
                Some(r#"["DRAFT"]"#),
                Some("UNPUBLISH"),
            ),
            ("delete", Some(r#"["PUBLISH"]"#), None, Some("DELETE")),
            ("edit", Some(r#"["DRAFT"]"#), Some(r#"["DRAFT"]"#), None),
        ];

        for (event_type, old_status, new_status, expected) in cases {
            let body = body_with_statuses(event_type, old_status, new_status);
            let event = normalize_payload(&body, received_at).unwrap();
            assert_eq!(event.event_kind.as_deref(), expected);
        }
    }
}
