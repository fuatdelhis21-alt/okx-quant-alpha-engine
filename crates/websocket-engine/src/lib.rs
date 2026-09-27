use qae_config::AppConfig;
use qae_core::{InstrumentId, RuntimeMode};
use qae_market_data::{
    MarketDataError, MarketDataUpdate, MarketSnapshot, OrderBookLevel, OrderBookSnapshot, Ticker,
};
use qae_time::ExchangeTimestamp;
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarketDataChannel {
    Tickers,
    OrderBook,
}

impl MarketDataChannel {
    #[must_use]
    pub fn as_okx_channel(self, depth: u32) -> &'static str {
        match self {
            Self::Tickers => "tickers",
            Self::OrderBook if depth <= 5 => "books5",
            Self::OrderBook => "books",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MarketDataSubscription {
    pub instrument_id: InstrumentId,
    pub channel: MarketDataChannel,
}

#[derive(Debug)]
pub struct WebsocketEngine {
    pub state: ConnectionState,
    pub reconnect_policy: ReconnectPolicy,
    pub heartbeat_policy: HeartbeatPolicy,
    pub health: WsHealthMetrics,
    subscriptions: Vec<MarketDataSubscription>,
    snapshots: HashMap<InstrumentId, MarketSnapshot>,
    order_book_depth: u32,
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
        let subscriptions = build_subscriptions(config)?;

        Ok(Self {
            state: ConnectionState::Disconnected,
            reconnect_policy: ReconnectPolicy {
                max_attempts: config.network.reconnect_max_attempts,
                ..ReconnectPolicy::default()
            },
            heartbeat_policy: HeartbeatPolicy::default(),
            health: WsHealthMetrics::default(),
            subscriptions,
            snapshots: HashMap::new(),
            order_book_depth: config.market_data.order_book_depth,
            _send_tx: send_tx,
            _send_rx: send_rx,
        })
    }

    #[must_use]
    pub fn subscriptions(&self) -> &[MarketDataSubscription] {
        &self.subscriptions
    }

    #[must_use]
    pub fn subscription_requests(&self) -> Vec<String> {
        let args = self
            .subscriptions
            .iter()
            .map(|subscription| {
                format!(
                    r#"{{"channel":"{}","instId":"{}"}}"#,
                    subscription.channel.as_okx_channel(self.order_book_depth),
                    subscription.instrument_id.as_str()
                )
            })
            .collect::<Vec<_>>();

        vec![format!(
            r#"{{"op":"subscribe","args":[{}]}}"#,
            args.join(",")
        )]
    }

    pub fn ingest_text_frame(
        &mut self,
        payload: &str,
    ) -> Result<Vec<MarketDataUpdate>, WebsocketEngineError> {
        let frame: IncomingFrame = serde_json::from_str(payload)
            .map_err(|source| WebsocketEngineError::InvalidFrame(source.to_string()))?;

        if frame.event.is_some() && frame.data.is_none() {
            return Ok(Vec::new());
        }

        let arg = frame.arg.ok_or(WebsocketEngineError::MissingFrameArg)?;
        let inst_id = arg
            .inst_id
            .ok_or(WebsocketEngineError::MissingInstrumentId)?;
        let instrument_id =
            InstrumentId::new(inst_id).map_err(|_| WebsocketEngineError::MissingInstrumentId)?;
        let channel = arg.channel;
        let mut updates = Vec::new();

        for entry in frame.data.unwrap_or_default() {
            let update = match channel.as_str() {
                "tickers" => MarketDataUpdate::Ticker(parse_ticker(&instrument_id, entry)?),
                "books" | "books5" => {
                    MarketDataUpdate::OrderBook(parse_order_book(&instrument_id, entry)?)
                }
                other => return Err(WebsocketEngineError::UnsupportedChannel(other.to_string())),
            };

            self.apply_update(update.clone());
            updates.push(update);
        }

        Ok(updates)
    }

    #[must_use]
    pub fn latest_snapshot(&self, instrument_id: &InstrumentId) -> Option<&MarketSnapshot> {
        self.snapshots.get(instrument_id)
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

    fn apply_update(&mut self, update: MarketDataUpdate) {
        match update {
            MarketDataUpdate::Ticker(ticker) => {
                let instrument_id = ticker.instrument_id.clone();
                self.snapshots.entry(instrument_id).or_default().ticker = Some(ticker);
            }
            MarketDataUpdate::OrderBook(order_book) => {
                let instrument_id = order_book.instrument_id.clone();
                self.snapshots.entry(instrument_id).or_default().order_book = Some(order_book);
            }
        }
    }
}

fn build_subscriptions(
    config: &AppConfig,
) -> Result<Vec<MarketDataSubscription>, WebsocketEngineError> {
    let mut subscriptions = Vec::new();

    for instrument in &config.market_data.instruments {
        let instrument_id = InstrumentId::new(instrument.clone())
            .map_err(|_| WebsocketEngineError::InvalidInstrument(instrument.clone()))?;

        if config.market_data.subscribe_tickers {
            subscriptions.push(MarketDataSubscription {
                instrument_id: instrument_id.clone(),
                channel: MarketDataChannel::Tickers,
            });
        }

        if config.market_data.subscribe_order_book {
            subscriptions.push(MarketDataSubscription {
                instrument_id,
                channel: MarketDataChannel::OrderBook,
            });
        }
    }

    Ok(subscriptions)
}

fn parse_ticker(
    instrument_id: &InstrumentId,
    value: serde_json::Value,
) -> Result<Ticker, WebsocketEngineError> {
    let payload: TickerPayload = serde_json::from_value(value)
        .map_err(|source| WebsocketEngineError::InvalidFrame(source.to_string()))?;
    let ts = parse_i64(&payload.ts)?;

    Ticker::new(
        instrument_id.clone(),
        parse_f64(&payload.last)?,
        parse_f64(&payload.bid_px)?,
        parse_f64(&payload.ask_px)?,
        ExchangeTimestamp::from_millis(ts),
    )
    .map_err(WebsocketEngineError::MarketData)
}

fn parse_order_book(
    instrument_id: &InstrumentId,
    value: serde_json::Value,
) -> Result<OrderBookSnapshot, WebsocketEngineError> {
    let payload: OrderBookPayload = serde_json::from_value(value)
        .map_err(|source| WebsocketEngineError::InvalidFrame(source.to_string()))?;
    let ts = parse_i64(&payload.ts)?;
    let bids = payload
        .bids
        .into_iter()
        .map(|level| {
            let price = parse_f64(&level[0])?;
            let quantity = parse_f64(&level[1])?;
            OrderBookLevel::new(price, quantity).map_err(WebsocketEngineError::MarketData)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let asks = payload
        .asks
        .into_iter()
        .map(|level| {
            let price = parse_f64(&level[0])?;
            let quantity = parse_f64(&level[1])?;
            OrderBookLevel::new(price, quantity).map_err(WebsocketEngineError::MarketData)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let checksum = payload
        .checksum
        .map(|value| parse_i64(&value))
        .transpose()?;

    OrderBookSnapshot::new(
        instrument_id.clone(),
        bids,
        asks,
        checksum,
        ExchangeTimestamp::from_millis(ts),
    )
    .map_err(WebsocketEngineError::MarketData)
}

fn parse_f64(value: &str) -> Result<f64, WebsocketEngineError> {
    f64::from_str(value).map_err(|_| WebsocketEngineError::InvalidNumber(value.to_string()))
}

fn parse_i64(value: &str) -> Result<i64, WebsocketEngineError> {
    i64::from_str(value).map_err(|_| WebsocketEngineError::InvalidNumber(value.to_string()))
}

#[derive(Debug, Deserialize)]
struct IncomingFrame {
    event: Option<String>,
    arg: Option<FrameArg>,
    data: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, Deserialize)]
struct FrameArg {
    channel: String,
    #[serde(rename = "instId")]
    inst_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TickerPayload {
    last: String,
    #[serde(rename = "bidPx")]
    bid_px: String,
    #[serde(rename = "askPx")]
    ask_px: String,
    ts: String,
}

#[derive(Debug, Deserialize)]
struct OrderBookPayload {
    bids: Vec<[String; 4]>,
    asks: Vec<[String; 4]>,
    ts: String,
    checksum: Option<String>,
}

#[derive(Debug, Error)]
pub enum WebsocketEngineError {
    #[error("live mode guard rejected websocket engine startup")]
    LiveModeGuard,
    #[error("unsupported websocket channel: {0}")]
    UnsupportedChannel(String),
    #[error("invalid websocket frame: {0}")]
    InvalidFrame(String),
    #[error("websocket frame is missing arg")]
    MissingFrameArg,
    #[error("websocket frame is missing instId")]
    MissingInstrumentId,
    #[error("invalid numeric field: {0}")]
    InvalidNumber(String),
    #[error("invalid instrument id: {0}")]
    InvalidInstrument(String),
    #[error("{0}")]
    MarketData(#[from] MarketDataError),
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
        assert_eq!(engine.subscriptions().len(), 2);
    }

    #[test]
    fn emits_subscription_request_payloads() {
        let cfg = AppConfig::default();
        let engine = WebsocketEngine::new(&cfg).expect("engine");
        let mut requests = engine.subscription_requests();
        let request = requests.pop().expect("request");

        assert!(request.contains(r#""channel":"tickers""#));
        assert!(request.contains(r#""channel":"books5""#));
        assert!(request.contains(r#""instId":"BTC-USDT-SWAP""#));
    }

    #[test]
    fn ingests_ticker_frames_into_snapshot_cache() {
        let cfg = AppConfig::default();
        let mut engine = WebsocketEngine::new(&cfg).expect("engine");
        let updates = engine
            .ingest_text_frame(
                r#"{"arg":{"channel":"tickers","instId":"BTC-USDT-SWAP"},"data":[{"last":"101.0","bidPx":"100.5","askPx":"101.5","ts":"1700000000000"}]}"#,
            )
            .expect("ticker frame");

        assert_eq!(updates.len(), 1);
        let snapshot = engine
            .latest_snapshot(&InstrumentId::new("BTC-USDT-SWAP").expect("instrument"))
            .expect("snapshot");
        assert_eq!(
            snapshot.ticker.as_ref().map(|ticker| ticker.last_price),
            Some(101.0)
        );
    }

    #[test]
    fn ingests_order_book_frames_into_snapshot_cache() {
        let cfg = AppConfig::default();
        let mut engine = WebsocketEngine::new(&cfg).expect("engine");
        engine
            .ingest_text_frame(
                r#"{"arg":{"channel":"books5","instId":"BTC-USDT-SWAP"},"data":[{"asks":[["101.5","2.0","0","1"]],"bids":[["101.0","1.5","0","1"]],"ts":"1700000000001","checksum":"7"}]}"#,
            )
            .expect("book frame");

        let snapshot = engine
            .latest_snapshot(&InstrumentId::new("BTC-USDT-SWAP").expect("instrument"))
            .expect("snapshot");
        assert_eq!(
            snapshot
                .order_book
                .as_ref()
                .and_then(|book| book.best_ask())
                .map(|level| level.price),
            Some(101.5)
        );
    }
}
