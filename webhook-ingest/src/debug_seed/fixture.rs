use chrono::{DateTime, Utc};

use super::SERVICE_ID;

#[allow(clippy::too_many_arguments)]
pub(super) fn smoke_fixture(
    api: &str,
    content_id: Option<&str>,
    event_type: &str,
    old_status: Option<&str>,
    new_status: Option<&str>,
    received_at: DateTime<Utc>,
    old_updated_at: Option<DateTime<Utc>>,
    new_updated_at: Option<DateTime<Utc>>,
    draft_created_at: Option<DateTime<Utc>>,
    content_created_at: Option<DateTime<Utc>>,
    content_published_at: Option<DateTime<Utc>>,
) -> (String, DateTime<Utc>) {
    let old = old_status.map(|status| {
        format!(
            r#"{{"status":["{status}"],"updatedAt":"{}"}}"#,
            old_updated_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| received_at.to_rfc3339())
        )
    });
    let new = new_status.map(|status| {
        let mut fields = vec![
            format!(r#""status":["{status}"]"#),
            format!(
                r#""updatedAt":"{}""#,
                new_updated_at
                    .map(|value| value.to_rfc3339())
                    .unwrap_or_else(|| received_at.to_rfc3339())
            ),
        ];
        if let Some(value) = draft_created_at {
            fields.push(format!(
                r#""draftValue":{{"createdAt":"{}"}}"#,
                value.to_rfc3339()
            ));
        }
        if content_created_at.is_some() || content_published_at.is_some() {
            let created = content_created_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| received_at.to_rfc3339());
            let published = content_published_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| received_at.to_rfc3339());
            fields.push(format!(
                r#""publishValue":{{"createdAt":"{created}","publishedAt":"{published}"}}"#
            ));
        }
        format!("{{{}}}", fields.join(","))
    });

    let id_field = match content_id {
        Some(content_id) => format!(r#""id": "{content_id}","#),
        None => String::new(),
    };

    let body = format!(
        r#"{{
          "service": "{SERVICE_ID}",
          "api": "{api}",
          {id_field}
          "type": "{event_type}",
          "contents": {{
            "old": {},
            "new": {}
          }}
        }}"#,
        old.as_deref().unwrap_or("null"),
        new.as_deref().unwrap_or("null")
    );

    (body, received_at)
}
