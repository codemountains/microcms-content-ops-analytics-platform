use std::env;

use crate::IngestError;

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub(crate) bucket: String,
    pub(crate) prefix: String,
    pub(crate) secret: String,
}

impl Config {
    pub(crate) fn from_env() -> Result<Self, IngestError> {
        let bucket = required_env("EVENT_BUCKET")?;
        let prefix = env::var("EVENT_PREFIX").unwrap_or_else(|_| "microcms_events".to_owned());
        let secret = required_env("MICROCMS_WEBHOOK_SECRET")?;

        Ok(Self {
            bucket,
            prefix: prefix.trim_matches('/').to_owned(),
            secret,
        })
    }
}

pub(crate) fn required_env(key: &'static str) -> Result<String, IngestError> {
    let value = env::var(key).map_err(|_| IngestError::MissingEnv(key))?;
    if value.trim().is_empty() {
        return Err(IngestError::MissingEnv(key));
    }

    Ok(value)
}

pub(crate) fn env_bool(key: &str) -> bool {
    env::var(key)
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_empty_required_env_value() {
        const KEY: &str = "__WEBHOOK_INGEST_EMPTY_REQUIRED_ENV_TEST";

        unsafe {
            env::set_var(KEY, "   ");
        }
        let result = required_env(KEY);
        unsafe {
            env::remove_var(KEY);
        }

        assert!(matches!(result, Err(IngestError::MissingEnv(KEY))));
    }
}
