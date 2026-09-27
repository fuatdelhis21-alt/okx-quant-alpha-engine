use serde::Serialize;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone, Serialize)]
pub struct MonitoringConfig {
    pub env_filter: String,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            env_filter: "info".to_string(),
        }
    }
}

pub fn init_tracing(config: &MonitoringConfig) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.env_filter));

    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(true)
        .with_line_number(true)
        .without_time()
        .try_init();
}

#[must_use]
pub fn redact_secret(value: &str) -> String {
    if value.is_empty() {
        return "<redacted>".to_string();
    }

    let visible = value.chars().take(2).collect::<String>();
    format!("{}***", visible)
}

#[derive(Clone)]
pub struct SecretString(String);

impl SecretString {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SecretString(<redacted>)")
    }
}

impl std::fmt::Display for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<redacted>")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_secret_value() {
        assert_eq!(redact_secret("abcdef"), "ab***");
    }

    #[test]
    fn secret_string_debug_hides_value() {
        let secret = SecretString::new("super-secret");
        assert!(!format!("{secret:?}").contains("super-secret"));
    }
}
