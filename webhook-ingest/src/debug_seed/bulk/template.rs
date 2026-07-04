use chrono::{DateTime, Duration, Utc};

use super::super::SERVICE_ID;

#[derive(Debug, Clone, Copy)]
pub(super) enum BulkTemplate {
    InitialDraft,
    SaveDraft,
    PublishFromDraft,
    InitialPublish,
    UpdatePublished,
    AddDraftToPublished,
    DiscardDraftOnPublished,
    UnpublishToDraft,
    UnpublishToClosed,
    ReopenToDraft,
    RepublishFromClosed,
    DeleteDraft,
    DeletePublished,
    DeleteClosed,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct BulkEventTiming {
    pub(super) draft_created_at: Option<DateTime<Utc>>,
    pub(super) content_created_at: Option<DateTime<Utc>>,
    pub(super) content_published_at: Option<DateTime<Utc>>,
}

impl BulkEventTiming {
    pub(super) fn for_filler(_template: BulkTemplate) -> Self {
        Self {
            draft_created_at: None,
            content_created_at: None,
            content_published_at: None,
        }
    }
}

/// Filler templates exclude INITIAL_DRAFT / PUBLISH_FROM_DRAFT pairs so draft-to-publish
/// metrics stay driven by coordinated metric lifecycles only.
pub(super) fn build_bulk_webhook_body(
    api: &str,
    content_id: &str,
    template: BulkTemplate,
    received_at: DateTime<Utc>,
    timing: &BulkEventTiming,
) -> String {
    let timestamp = received_at.to_rfc3339();
    let day_before = (received_at - Duration::days(1)).to_rfc3339();
    let draft_created_at = timing
        .draft_created_at
        .unwrap_or(received_at - Duration::days(7))
        .to_rfc3339();
    let content_created_at = timing
        .content_created_at
        .unwrap_or(received_at - Duration::days(7))
        .to_rfc3339();
    let content_published_at = timing
        .content_published_at
        .unwrap_or(received_at)
        .to_rfc3339();

    match template {
        BulkTemplate::InitialDraft => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"new",
              "contents":{{
                "old":null,
                "new":{{
                  "status":["DRAFT"],
                  "updatedAt":"{timestamp}",
                  "draftValue":{{"createdAt":"{draft_created_at}"}}
                }}
              }}
            }}"#
        ),
        BulkTemplate::SaveDraft => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"edit",
              "contents":{{
                "old":{{"status":["DRAFT"],"updatedAt":"{day_before}"}},
                "new":{{
                  "status":["DRAFT"],
                  "updatedAt":"{timestamp}",
                  "draftValue":{{"createdAt":"{draft_created_at}"}}
                }}
              }}
            }}"#
        ),
        BulkTemplate::InitialPublish => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"new",
              "contents":{{
                "old":null,
                "new":{{
                  "status":["PUBLISH"],
                  "updatedAt":"{timestamp}",
                  "publishValue":{{"createdAt":"{content_created_at}","publishedAt":"{content_published_at}"}}
                }}
              }}
            }}"#
        ),
        BulkTemplate::PublishFromDraft => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"edit",
              "contents":{{
                "old":{{"status":["DRAFT"],"updatedAt":"{day_before}"}},
                "new":{{
                  "status":["PUBLISH"],
                  "updatedAt":"{timestamp}",
                  "publishValue":{{"createdAt":"{content_created_at}","publishedAt":"{content_published_at}"}}
                }}
              }}
            }}"#
        ),
        BulkTemplate::UpdatePublished => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"edit",
              "contents":{{
                "old":{{"status":["PUBLISH"],"updatedAt":"{day_before}"}},
                "new":{{"status":["PUBLISH"],"updatedAt":"{timestamp}"}}
              }}
            }}"#
        ),
        BulkTemplate::AddDraftToPublished => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"edit",
              "contents":{{
                "old":{{"status":["PUBLISH"],"updatedAt":"{day_before}"}},
                "new":{{
                  "status":["PUBLISH","DRAFT"],
                  "updatedAt":"{timestamp}",
                  "draftKey":"debug-draft-key",
                  "publishValue":{{"createdAt":"{content_created_at}","publishedAt":"{content_published_at}"}},
                  "draftValue":{{"createdAt":"{draft_created_at}"}}
                }}
              }}
            }}"#
        ),
        BulkTemplate::DiscardDraftOnPublished => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"edit",
              "contents":{{
                "old":{{
                  "status":["PUBLISH","DRAFT"],
                  "updatedAt":"{day_before}",
                  "draftKey":"debug-draft-key",
                  "publishValue":{{"createdAt":"{content_created_at}","publishedAt":"{content_published_at}"}},
                  "draftValue":{{"createdAt":"{draft_created_at}"}}
                }},
                "new":{{
                  "status":["PUBLISH"],
                  "updatedAt":"{timestamp}",
                  "draftKey":null,
                  "publishValue":{{"createdAt":"{content_created_at}","publishedAt":"{content_published_at}"}},
                  "draftValue":null
                }}
              }}
            }}"#
        ),
        BulkTemplate::UnpublishToDraft => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"edit",
              "contents":{{
                "old":{{"status":["PUBLISH"],"updatedAt":"{day_before}"}},
                "new":{{
                  "status":["DRAFT"],
                  "updatedAt":"{timestamp}",
                  "draftKey":"debug-draft-key",
                  "publishValue":null,
                  "draftValue":{{"createdAt":"{draft_created_at}"}}
                }}
              }}
            }}"#
        ),
        BulkTemplate::UnpublishToClosed => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"edit",
              "contents":{{
                "old":{{"status":["PUBLISH"],"updatedAt":"{day_before}"}},
                "new":{{
                  "status":["CLOSED"],
                  "updatedAt":"{timestamp}",
                  "draftKey":null,
                  "publishValue":null,
                  "draftValue":null
                }}
              }}
            }}"#
        ),
        BulkTemplate::ReopenToDraft => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"edit",
              "contents":{{
                "old":{{"status":["CLOSED"],"updatedAt":"{day_before}"}},
                "new":{{
                  "status":["DRAFT"],
                  "updatedAt":"{timestamp}",
                  "draftKey":"debug-draft-key",
                  "publishValue":null,
                  "draftValue":{{"createdAt":"{draft_created_at}"}}
                }}
              }}
            }}"#
        ),
        BulkTemplate::RepublishFromClosed => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"edit",
              "contents":{{
                "old":{{"status":["CLOSED"],"updatedAt":"{day_before}"}},
                "new":{{
                  "status":["PUBLISH"],
                  "updatedAt":"{timestamp}",
                  "publishValue":{{"createdAt":"{content_created_at}","publishedAt":"{content_published_at}"}}
                }}
              }}
            }}"#
        ),
        BulkTemplate::DeleteDraft => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"delete",
              "contents":{{
                "old":{{"status":["DRAFT"],"updatedAt":"{timestamp}"}},
                "new":null
              }}
            }}"#
        ),
        BulkTemplate::DeletePublished => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"delete",
              "contents":{{
                "old":{{"status":["PUBLISH"],"updatedAt":"{timestamp}"}},
                "new":null
              }}
            }}"#
        ),
        BulkTemplate::DeleteClosed => format!(
            r#"{{
              "service":"{SERVICE_ID}",
              "api":"{api}",
              "id":"{content_id}",
              "type":"delete",
              "contents":{{
                "old":{{"status":["CLOSED"],"updatedAt":"{timestamp}"}},
                "new":null
              }}
            }}"#
        ),
    }
}
