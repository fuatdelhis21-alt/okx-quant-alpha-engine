use qae_config::AppConfig;
use qae_core::RuntimeMode;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Reconnecting,
    Stopped,
}

#[derive(Debug, Clone)]
pub struct ReconnectPolicy {
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub max_attempts: u32,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self {
            base_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(10),
            max_attempts: 8,
        }
    }
}

impl ReconnectPolicy {
    #[must_use]
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let exponent = attempt.saturating_sub(1).min(16);
        let factor = 2u32.saturating_pow(exponent);
        let candidate = self.base_delay.saturating_mul(factor);
        candidate.min(self.max_delay)
    }

    #[must_use]
    pub fn should_retry(&self, attempt: u32) -> bool {
        attempt <= self.max_attempts
    }
}

#[derive(Debug, Clone)]
pub struct HeartbeatPolicy {
    pub heartbeat_interval: Duration,
    pub heartbeat_timeout: Duration,
}

impl Default for HeartbeatPolicy {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(15),
            heartbeat_timeout: Duration::from_secs(45),
        }
    }
}

#[derive(Debug)]
pub struct HeartbeatSupervisor {
    policy: HeartbeatPolicy,
    last_heartbeat: Instant,
}

impl HeartbeatSupervisor {
    #[must_use]
    pub fn new(policy: HeartbeatPolicy) -> Self {
        Self {
            policy,
            last_heartbeat: Instant::now(),
        }
    }

    pub fn mark_heartbeat(&mut self) {
        self.last_heartbeat = Instant::now();
    }

    #[must_use]
    pub fn is_timed_out(&self) -> bool {
        self.last_heartbeat.elapsed() >= self.policy.heartbeat_timeout
    }
}

#[derive(Debug, Default, Clone)]
pub struct WsHealthMetrics {
    pub reconnect_count: u64,
    pub missed_heartbeats: u64,
    pub last_error: Option<String>,
}

#[derive(Debug)]
pub struct WebsocketEngine {
    pub state: ConnectionState,
    pub reconnect_policy: ReconnectPolicy,
    pub heartbeat_policy: HeartbeatPolicy,
    pub health: WsHealthMetrics,
    _send_tx: mpsc::Sender<String>,
    _send_rx: mpsc::Receiver<String>,
}

impl WebsocketEngine {
    pub fn new(config: &AppConfig) -> Result<Self, WebsocketEngineError> {
        if config.mode == RuntimeMode::Live && !config.live_trading {
            return Err(WebsocketEngineError::LiveModeGuard);
        }

        let capacity = 1_024;
        let (send_tx, send_rx) = mpsc::channel(capacity);

        Ok(Self {
            state: ConnectionState::Disconnected,
            reconnect_policy: ReconnectPolicy {
                max_attempts: config.network.reconnect_max_attempts,
                ..ReconnectPolicy::default()
            },
            heartbeat_policy: HeartbeatPolicy::default(),
            health: WsHealthMetrics::default(),
            _send_tx: send_tx,
            _send_rx: send_rx,
        })
    }

    pub fn begin_connecting(&mut self) {
        self.state = ConnectionState::Connecting;
    }

    pub fn mark_connected(&mut self) {
        self.state = ConnectionState::Connected;
    }

    pub fn register_disconnect(&mut self, err: impl Into<String>) {
        self.state = ConnectionState::Reconnecting;
        self.health.reconnect_count += 1;
        self.health.last_error = Some(err.into());
    }
}

#[derive(Debug, Error)]
pub enum WebsocketEngineError {
    #[error("live mode guard rejected websocket engine startup")]
    LiveModeGuard,
}

#[cfg(test)]
mod tests {
    use super::*;
    use qae_config::AppConfig;

    #[test]
    fn reconnect_backoff_grows_until_cap() {
        let policy = ReconnectPolicy {
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(500),
            max_attempts: 5,
        };

        assert_eq!(policy.delay_for_attempt(1), Duration::from_millis(100));
        assert_eq!(policy.delay_for_attempt(2), Duration::from_millis(200));
        assert_eq!(policy.delay_for_attempt(3), Duration::from_millis(400));
        assert_eq!(policy.delay_for_attempt(4), Duration::from_millis(500));
    }

    #[test]
    fn defaults_to_non_live_safe_start() {
        let cfg = AppConfig::default();
        let engine = WebsocketEngine::new(&cfg).expect("safe defaults should start");
        assert_eq!(engine.state, ConnectionState::Disconnected);
    }
}
