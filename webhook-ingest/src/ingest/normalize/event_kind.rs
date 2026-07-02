pub(super) const EVENT_KIND_CREATE_DRAFT: &str = "CREATE_DRAFT";
pub(super) const EVENT_KIND_CREATE_PUBLISH: &str = "CREATE_PUBLISH";
pub(super) const EVENT_KIND_FIRST_PUBLISH: &str = "FIRST_PUBLISH";
pub(super) const EVENT_KIND_UPDATE_PUBLISH: &str = "UPDATE_PUBLISH";
pub(super) const EVENT_KIND_UNPUBLISH: &str = "UNPUBLISH";
pub(super) const EVENT_KIND_DELETE: &str = "DELETE";

pub(super) fn derive_event_kind(
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
