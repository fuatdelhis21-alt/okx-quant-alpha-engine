use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeMode {
    #[default]
    Paper,
    Simulation,
    Live,
}

impl RuntimeMode {
    #[must_use]
    pub fn is_live(self) -> bool {
        matches!(self, Self::Live)
    }
}

impl Display for RuntimeMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Paper => write!(f, "paper"),
            Self::Simulation => write!(f, "simulation"),
            Self::Live => write!(f, "live"),
        }
    }
}

impl FromStr for RuntimeMode {
    type Err = ParseRuntimeModeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "paper" => Ok(Self::Paper),
            "simulation" | "sim" => Ok(Self::Simulation),
            "live" => Ok(Self::Live),
            _ => Err(ParseRuntimeModeError(value.to_string())),
        }
    }
}

#[derive(Debug, Error)]
#[error("unsupported runtime mode: {0}")]
pub struct ParseRuntimeModeError(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RequestPriority {
    Low,
    #[default]
    Normal,
    High,
    Critical,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstrumentId(String);

impl InstrumentId {
    pub fn new(raw: impl Into<String>) -> Result<Self, InstrumentIdError> {
        let value = raw.into();
        let normalized = value.trim();
        if normalized.is_empty() {
            return Err(InstrumentIdError::Empty);
        }

        Ok(Self(normalized.to_owned()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for InstrumentId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum InstrumentIdError {
    #[error("instrument identifier cannot be empty")]
    Empty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComponentStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub component: &'static str,
    pub status: ComponentStatus,
    pub details: Option<String>,
    pub observed_at: DateTime<Utc>,
}

impl HealthStatus {
    #[must_use]
    pub fn new(
        component: &'static str,
        status: ComponentStatus,
        details: Option<String>,
        observed_at: DateTime<Utc>,
    ) -> Self {
        Self {
            component,
            status,
            details,
            observed_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_defaults_to_paper() {
        assert_eq!(RuntimeMode::default(), RuntimeMode::Paper);
        assert!(!RuntimeMode::default().is_live());
    }

    #[test]
    fn runtime_mode_parses_variants() {
        assert_eq!("paper".parse::<RuntimeMode>().ok(), Some(RuntimeMode::Paper));
        assert_eq!("sim".parse::<RuntimeMode>().ok(), Some(RuntimeMode::Simulation));
        assert_eq!("live".parse::<RuntimeMode>().ok(), Some(RuntimeMode::Live));
    }

    #[test]
    fn instrument_rejects_empty() {
        assert_eq!(InstrumentId::new("   ").err(), Some(InstrumentIdError::Empty));
    }
}
