pub(super) const EVENT_KIND_INITIAL_DRAFT: &str = "INITIAL_DRAFT";
pub(super) const EVENT_KIND_SAVE_DRAFT: &str = "SAVE_DRAFT";
pub(super) const EVENT_KIND_PUBLISH_FROM_DRAFT: &str = "PUBLISH_FROM_DRAFT";
pub(super) const EVENT_KIND_INITIAL_PUBLISH: &str = "INITIAL_PUBLISH";
pub(super) const EVENT_KIND_UPDATE_PUBLISHED: &str = "UPDATE_PUBLISHED";
pub(super) const EVENT_KIND_ADD_DRAFT_TO_PUBLISHED: &str = "ADD_DRAFT_TO_PUBLISHED";
pub(super) const EVENT_KIND_DISCARD_DRAFT_ON_PUBLISHED: &str = "DISCARD_DRAFT_ON_PUBLISHED";
pub(super) const EVENT_KIND_UNPUBLISH_TO_DRAFT: &str = "UNPUBLISH_TO_DRAFT";
pub(super) const EVENT_KIND_UNPUBLISH_TO_CLOSED: &str = "UNPUBLISH_TO_CLOSED";
pub(super) const EVENT_KIND_REOPEN_TO_DRAFT: &str = "REOPEN_TO_DRAFT";
pub(super) const EVENT_KIND_REPUBLISH_FROM_CLOSED: &str = "REPUBLISH_FROM_CLOSED";
pub(super) const EVENT_KIND_DELETE_DRAFT: &str = "DELETE_DRAFT";
pub(super) const EVENT_KIND_DELETE_PUBLISHED: &str = "DELETE_PUBLISHED";
pub(super) const EVENT_KIND_DELETE_CLOSED: &str = "DELETE_CLOSED";

pub(super) fn derive_event_kind(
    event_type: Option<&str>,
    old_statuses: &[String],
    new_statuses: &[String],
) -> Option<String> {
    let old_state = canonical_status(old_statuses);
    let new_state = canonical_status(new_statuses);
    let old_published = old_state == Some(ContentStatus::Publish);
    let old_draft = has_status(old_statuses, "DRAFT");
    let new_published = new_state == Some(ContentStatus::Publish);
    let new_draft = has_status(new_statuses, "DRAFT");

    let event_kind = match (event_type, old_state, new_state) {
        (Some("new"), None, Some(ContentStatus::Draft)) => EVENT_KIND_INITIAL_DRAFT,
        (Some("new"), None, Some(ContentStatus::Publish)) => EVENT_KIND_INITIAL_PUBLISH,
        (Some("edit"), Some(ContentStatus::Draft), Some(ContentStatus::Draft)) => {
            EVENT_KIND_SAVE_DRAFT
        }
        (Some("edit"), Some(ContentStatus::Draft), Some(ContentStatus::Publish)) => {
            EVENT_KIND_PUBLISH_FROM_DRAFT
        }
        (Some("edit"), Some(ContentStatus::Publish), Some(ContentStatus::Publish))
            if old_published && !old_draft && new_published && new_draft =>
        {
            EVENT_KIND_ADD_DRAFT_TO_PUBLISHED
        }
        (Some("edit"), Some(ContentStatus::Publish), Some(ContentStatus::Publish))
            if old_published && old_draft && new_published && !new_draft =>
        {
            EVENT_KIND_DISCARD_DRAFT_ON_PUBLISHED
        }
        (Some("edit"), Some(ContentStatus::Publish), Some(ContentStatus::Publish)) => {
            EVENT_KIND_UPDATE_PUBLISHED
        }
        (Some("edit"), Some(ContentStatus::Publish), Some(ContentStatus::Draft)) => {
            EVENT_KIND_UNPUBLISH_TO_DRAFT
        }
        (Some("edit"), Some(ContentStatus::Publish), Some(ContentStatus::Closed)) => {
            EVENT_KIND_UNPUBLISH_TO_CLOSED
        }
        (Some("edit"), Some(ContentStatus::Closed), Some(ContentStatus::Draft)) => {
            EVENT_KIND_REOPEN_TO_DRAFT
        }
        (Some("edit"), Some(ContentStatus::Closed), Some(ContentStatus::Publish)) => {
            EVENT_KIND_REPUBLISH_FROM_CLOSED
        }
        (Some("delete"), Some(ContentStatus::Draft), None) => EVENT_KIND_DELETE_DRAFT,
        (Some("delete"), Some(ContentStatus::Publish), None) => EVENT_KIND_DELETE_PUBLISHED,
        (Some("delete"), Some(ContentStatus::Closed), None) => EVENT_KIND_DELETE_CLOSED,
        _ => return None,
    };

    Some(event_kind.to_owned())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContentStatus {
    Draft,
    Publish,
    Closed,
}

fn canonical_status(statuses: &[String]) -> Option<ContentStatus> {
    if has_status(statuses, "PUBLISH") {
        Some(ContentStatus::Publish)
    } else if has_status(statuses, "DRAFT") && statuses.len() == 1 {
        Some(ContentStatus::Draft)
    } else if has_status(statuses, "CLOSED") && statuses.len() == 1 {
        Some(ContentStatus::Closed)
    } else {
        None
    }
}

fn has_status(statuses: &[String], expected: &str) -> bool {
    statuses.iter().any(|status| status == expected)
}
