use chrono::{DateTime, Utc};
use serde_json::Value;

pub(super) fn extract_statuses(value: Option<&Value>) -> Vec<String> {
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

pub(super) fn status_csv(statuses: &[String]) -> Option<String> {
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

pub(super) fn extract_timestamp(value: Option<&Value>, keys: &[&str]) -> Option<DateTime<Utc>> {
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

pub(super) fn extract_nested_timestamp(
    value: Option<&Value>,
    path: &[&str],
) -> Option<DateTime<Utc>> {
    extract_nested_string(value, path)
        .and_then(|value| DateTime::parse_from_rfc3339(&value).ok())
        .map(|value| value.with_timezone(&Utc))
}
