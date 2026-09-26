use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};
use thiserror::Error;

pub trait WallClock: Send + Sync {
    fn now_utc(&self) -> DateTime<Utc>;
}

#[derive(Debug, Default)]
pub struct SystemWallClock;

impl WallClock for SystemWallClock {
    fn now_utc(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExchangeTimestamp(i64);

impl ExchangeTimestamp {
    #[must_use]
    pub fn from_millis(value: i64) -> Self {
        Self(value)
    }

    #[must_use]
    pub fn as_millis(self) -> i64 {
        self.0
    }

    pub fn to_datetime(self) -> Result<DateTime<Utc>, TimeError> {
        Utc.timestamp_millis_opt(self.0)
            .single()
            .ok_or(TimeError::InvalidTimestamp(self.0))
    }

    #[must_use]
    pub fn from_datetime(value: DateTime<Utc>) -> Self {
        Self(value.timestamp_millis())
    }
}

#[derive(Debug, Error)]
pub enum TimeError {
    #[error("invalid exchange timestamp: {0}")]
    InvalidTimestamp(i64),
}

#[derive(Debug, Clone)]
pub struct MonotonicClock {
    start: Instant,
}

impl Default for MonotonicClock {
    fn default() -> Self {
        Self::new()
    }
}

impl MonotonicClock {
    #[must_use]
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    #[must_use]
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

#[derive(Debug, Default)]
pub struct ClockOffsetState {
    offset_millis: AtomicI64,
    sampled_at_millis: AtomicI64,
}

impl ClockOffsetState {
    #[must_use]
    pub fn current_offset_millis(&self) -> i64 {
        self.offset_millis.load(Ordering::Relaxed)
    }

    pub fn update_offset(&self, offset_millis: i64, sampled_at: DateTime<Utc>) {
        self.offset_millis.store(offset_millis, Ordering::Relaxed);
        self.sampled_at_millis
            .store(sampled_at.timestamp_millis(), Ordering::Relaxed);
    }

    #[must_use]
    pub fn sampled_at(&self) -> Option<ExchangeTimestamp> {
        let raw = self.sampled_at_millis.load(Ordering::Relaxed);
        (raw > 0).then_some(ExchangeTimestamp::from_millis(raw))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_exchange_timestamp() {
        let ts = ExchangeTimestamp::from_millis(1_700_000_000_000);
        let dt = ts.to_datetime().expect("timestamp should parse");
        assert_eq!(ExchangeTimestamp::from_datetime(dt), ts);
    }

    #[test]
    fn tracks_clock_offset() {
        let offset = ClockOffsetState::default();
        offset.update_offset(25, Utc::now());
        assert_eq!(offset.current_offset_millis(), 25);
        assert!(offset.sampled_at().is_some());
    }
}
