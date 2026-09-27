use futures_util::{SinkExt, StreamExt};
use qae_config::AppConfig;
use qae_core::{InstrumentId, RuntimeMode};
use qae_market_data::{
    MarketDataError, MarketDataUpdate, MarketSnapshot, OrderBookLevel, OrderBookSnapshot, Ticker,
};
use qae_time::ExchangeTimestamp;
use serde::de::IgnoredAny;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::time::{Duration, Instant};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::time;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::{Error as TungsteniteError, Message};

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
            .map(|subscription| SubscriptionArg {
                channel: subscription.channel.as_okx_channel(self.order_book_depth),
                inst_id: subscription.instrument_id.as_str(),
            })
            .collect::<Vec<_>>();

        vec![
            serde_json::to_string(&SubscriptionRequest {
                op: "subscribe",
                args,
            })
            .expect("subscription serialization must succeed"),
        ]
    }

    pub async fn connect_public_stream(
        &mut self,
        endpoint: &str,
        max_updates: usize,
        max_duration: Duration,
    ) -> Result<Vec<MarketDataUpdate>, WebsocketEngineError> {
        if max_updates == 0 || max_duration.is_zero() {
            self.state = ConnectionState::Stopped;
            return Ok(Vec::new());
        }

        self.begin_connecting();

        let (mut socket, _) = connect_async(endpoint)
            .await
            .map_err(|source| self.register_transport_error(source))?;

        self.mark_connected();
        self.send_subscriptions(&mut socket).await?;
        let updates = self
            .collect_updates(&mut socket, max_updates, max_duration)
            .await?;

        if matches!(
            self.state,
            ConnectionState::Connected | ConnectionState::Connecting
        ) {
            self.state = ConnectionState::Stopped;
        }

        Ok(updates)
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
        let instrument_id = InstrumentId::new(inst_id.clone())
            .map_err(|_| WebsocketEngineError::InvalidInstrument(inst_id))?;
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

    async fn send_subscriptions<S>(&mut self, socket: &mut S) -> Result<(), WebsocketEngineError>
    where
        S: futures_util::Sink<Message, Error = TungsteniteError> + Unpin,
    {
        for request in self.subscription_requests() {
            socket
                .send(Message::Text(request.into()))
                .await
                .map_err(|source| self.register_transport_error(source))?;
        }

        Ok(())
    }

    async fn collect_updates<S>(
        &mut self,
        socket: &mut S,
        max_updates: usize,
        max_duration: Duration,
    ) -> Result<Vec<MarketDataUpdate>, WebsocketEngineError>
    where
        S: futures_util::Stream<Item = Result<Message, TungsteniteError>>
            + futures_util::Sink<Message, Error = TungsteniteError>
            + Unpin,
    {
        let mut updates = Vec::new();
        let mut heartbeat = HeartbeatSupervisor::new(self.heartbeat_policy.clone());
        let deadline = time::Instant::now() + max_duration;

        while updates.len() < max_updates {
            let now = time::Instant::now();
            if now >= deadline {
                break;
            }

            let remaining = deadline.saturating_duration_since(now);
            let wait_for = remaining.min(self.heartbeat_policy.heartbeat_interval);

            match time::timeout(wait_for, socket.next()).await {
                Ok(Some(Ok(Message::Text(payload)))) => {
                    heartbeat.mark_heartbeat();
                    updates.extend(self.ingest_text_frame(payload.as_str())?);
                }
                Ok(Some(Ok(Message::Ping(payload)))) => {
                    heartbeat.mark_heartbeat();
                    socket
                        .send(Message::Pong(payload))
                        .await
                        .map_err(|source| self.register_transport_error(source))?;
                }
                Ok(Some(Ok(Message::Pong(_)))) => {
                    heartbeat.mark_heartbeat();
                }
                Ok(Some(Ok(Message::Binary(_)))) => {
                    heartbeat.mark_heartbeat();
                }
                Ok(Some(Ok(Message::Close(_)))) => {
                    self.state = ConnectionState::Stopped;
                    break;
                }
                Ok(Some(Ok(Message::Frame(_)))) => {}
                Ok(Some(Err(source))) => return Err(self.register_transport_error(source)),
                Ok(None) => {
                    self.state = ConnectionState::Stopped;
                    break;
                }
                Err(_) => {
                    if heartbeat.is_timed_out() {
                        self.health.missed_heartbeats += 1;
                        let error = WebsocketEngineError::HeartbeatTimedOut;
                        self.register_disconnect(error.to_string());
                        return Err(error);
                    }

                    socket
                        .send(Message::Ping(Vec::new().into()))
                        .await
                        .map_err(|source| self.register_transport_error(source))?;
                    heartbeat.mark_heartbeat();
                }
            }
        }

        Ok(updates)
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

    fn register_transport_error(&mut self, source: impl std::fmt::Display) -> WebsocketEngineError {
        let error = WebsocketEngineError::Transport(source.to_string());
        self.register_disconnect(error.to_string());
        error
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
            let price = parse_f64(&level.0)?;
            let quantity = parse_f64(&level.1)?;
            OrderBookLevel::new(price, quantity).map_err(WebsocketEngineError::MarketData)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let asks = payload
        .asks
        .into_iter()
        .map(|level| {
            let price = parse_f64(&level.0)?;
            let quantity = parse_f64(&level.1)?;
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

#[derive(Debug, Serialize)]
struct SubscriptionRequest<'a> {
    op: &'static str,
    args: Vec<SubscriptionArg<'a>>,
}

#[derive(Debug, Serialize)]
struct SubscriptionArg<'a> {
    channel: &'static str,
    #[serde(rename = "instId")]
    inst_id: &'a str,
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
    bids: Vec<RawOrderBookLevel>,
    asks: Vec<RawOrderBookLevel>,
    ts: String,
    checksum: Option<String>,
}

type RawOrderBookLevel = (String, String, IgnoredAny, IgnoredAny);

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
    #[error("websocket transport failed: {0}")]
    Transport(String),
    #[error("websocket heartbeat timed out")]
    HeartbeatTimedOut,
    #[error("{0}")]
    MarketData(#[from] MarketDataError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use qae_config::AppConfig;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;

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

    #[tokio::test(flavor = "current_thread")]
    async fn connects_to_local_server_and_collects_updates() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut socket = accept_async(stream).await.expect("handshake");

            let first_message = socket
                .next()
                .await
                .expect("message")
                .expect("socket message");
            let payload = first_message.into_text().expect("text");
            assert!(payload.contains(r#""op":"subscribe""#));

            socket
                .send(Message::Text(
                    r#"{"event":"subscribe","arg":{"channel":"tickers","instId":"BTC-USDT-SWAP"}}"#
                        .into(),
                ))
                .await
                .expect("ack");
            socket
                .send(Message::Text(
                    r#"{"arg":{"channel":"tickers","instId":"BTC-USDT-SWAP"},"data":[{"last":"101.0","bidPx":"100.5","askPx":"101.5","ts":"1700000000000"}]}"#
                        .into(),
                ))
                .await
                .expect("ticker");
            socket
                .send(Message::Text(
                    r#"{"arg":{"channel":"books5","instId":"BTC-USDT-SWAP"},"data":[{"asks":[["101.5","2.0","0","1"]],"bids":[["101.0","1.5","0","1"]],"ts":"1700000000001","checksum":"7"}]}"#
                        .into(),
                ))
                .await
                .expect("book");
        });

        let cfg = AppConfig::default();
        let mut engine = WebsocketEngine::new(&cfg).expect("engine");
        let updates = engine
            .connect_public_stream(&format!("ws://{addr}"), 2, Duration::from_secs(2))
            .await
            .expect("updates");

        assert_eq!(updates.len(), 2);
        assert_eq!(engine.state, ConnectionState::Stopped);

        server.await.expect("server");
    }
}
