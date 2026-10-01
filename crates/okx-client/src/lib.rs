use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hmac::{Hmac, Mac};
use qae_config::{AppConfig, WsEndpointKind};
use qae_core::{
    CancelOrderRequest, ExecutionReport, OrderRequest, OrderSide, OrderStatus, OrderType,
    RequestPriority, RuntimeMode,
};
use qae_time::{ExchangeTimestamp, WallClock};
use reqwest::blocking::Client;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::borrow::Cow;
use std::collections::BTreeMap;
use thiserror::Error;
use url::Url;

pub const OKX_ACCESS_KEY: &str = "OK-ACCESS-KEY";
pub const OKX_ACCESS_SIGN: &str = "OK-ACCESS-SIGN";
pub const OKX_ACCESS_TIMESTAMP: &str = "OK-ACCESS-TIMESTAMP";
pub const OKX_ACCESS_PASSPHRASE: &str = "OK-ACCESS-PASSPHRASE";
pub const OKX_SIMULATED_TRADING: &str = "x-simulated-trading";
pub const OKX_WS_LOGIN_PATH: &str = "/users/self/verify";
pub const OKX_ORDER_PATH: &str = "/api/v5/trade/order";
pub const OKX_CANCEL_ORDER_PATH: &str = "/api/v5/trade/cancel-order";

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct OkxClient {
    config: AppConfig,
}

impl OkxClient {
    #[must_use]
    pub fn new(config: AppConfig) -> Self {
        Self { config }
    }

    #[must_use]
    pub fn rest_endpoint(&self) -> &Url {
        &self.config.okx.endpoints.rest
    }

    #[must_use]
    pub fn websocket_endpoint(&self, endpoint: WsEndpointKind) -> &Url {
        self.config.okx.endpoints.ws_endpoint(endpoint)
    }

    pub fn build_authenticated_request<C: WallClock>(
        &self,
        signer: &impl RequestSigner,
        request: RestRequest,
        clock: &C,
    ) -> Result<SignedRequest, OkxClientError> {
        let (api_key, api_secret, passphrase) = self.credentials()?;

        let timestamp = clock
            .now_utc()
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
        let signature_payload =
            signature_payload(&timestamp, &request.method, &request.path, &request.body);
        let signature = signer.sign(api_secret, &signature_payload)?;

        let mut headers = BTreeMap::new();
        headers.insert(OKX_ACCESS_KEY.to_string(), api_key.to_string());
        headers.insert(OKX_ACCESS_SIGN.to_string(), signature);
        headers.insert(OKX_ACCESS_TIMESTAMP.to_string(), timestamp);
        headers.insert(OKX_ACCESS_PASSPHRASE.to_string(), passphrase.to_string());

        if self.config.okx.demo_trading {
            headers.insert(OKX_SIMULATED_TRADING.to_string(), "1".to_string());
        }

        Ok(SignedRequest { request, headers })
    }

    pub fn build_ws_login_command<C: WallClock>(
        &self,
        signer: &impl RequestSigner,
        clock: &C,
    ) -> Result<String, OkxClientError> {
        let (api_key, api_secret, passphrase) = self.credentials()?;
        let timestamp = clock.now_utc().timestamp().to_string();
        let sign = signer.sign(
            api_secret,
            &signature_payload(&timestamp, "GET", OKX_WS_LOGIN_PATH, ""),
        )?;

        serde_json::to_string(&WsLoginCommand {
            op: "login",
            args: vec![WsLoginArg {
                api_key,
                passphrase,
                timestamp: &timestamp,
                sign: &sign,
            }],
        })
        .map_err(OkxClientError::SerializePayload)
    }

    pub fn build_order_request(&self, order: &OrderRequest) -> Result<RestRequest, OkxClientError> {
        order
            .validate()
            .map_err(|error| OkxClientError::InvalidOrder(error.to_string()))?;

        let body = serde_json::to_string(&OkxOrderBody::from(order))
            .map_err(OkxClientError::SerializePayload)?;

        Ok(RestRequest::new("POST", OKX_ORDER_PATH)
            .with_priority(RequestPriority::High)
            .with_body(body))
    }

    pub fn build_signed_order_request<C: WallClock>(
        &self,
        signer: &impl RequestSigner,
        order: &OrderRequest,
        clock: &C,
    ) -> Result<SignedRequest, OkxClientError> {
        let request = self.build_order_request(order)?;
        self.build_authenticated_request(signer, request, clock)
    }

    pub fn build_cancel_order_request(
        &self,
        cancel: &CancelOrderRequest,
    ) -> Result<RestRequest, OkxClientError> {
        cancel
            .validate()
            .map_err(|error| OkxClientError::InvalidCancelOrder(error.to_string()))?;

        let body = serde_json::to_string(&OkxCancelOrderBody::from(cancel))
            .map_err(OkxClientError::SerializePayload)?;

        Ok(RestRequest::new("POST", OKX_CANCEL_ORDER_PATH)
            .with_priority(RequestPriority::High)
            .with_body(body))
    }

    pub fn build_signed_cancel_order_request<C: WallClock>(
        &self,
        signer: &impl RequestSigner,
        cancel: &CancelOrderRequest,
        clock: &C,
    ) -> Result<SignedRequest, OkxClientError> {
        let request = self.build_cancel_order_request(cancel)?;
        self.build_authenticated_request(signer, request, clock)
    }

    pub fn submit_live_order<C: WallClock, T: LiveOrderTransport>(
        &self,
        transport: &T,
        signer: &impl RequestSigner,
        order: &OrderRequest,
        clock: &C,
    ) -> Result<ExecutionReport, OkxClientError> {
        if !self.config.live_trading || self.config.mode != RuntimeMode::Live {
            return Err(OkxClientError::LiveTradingDisabled);
        }

        let request = self.build_signed_order_request(signer, order, clock)?;
        let body = transport.execute(
            self.rest_endpoint(),
            &request,
            self.config.network.request_timeout_ms,
        )?;
        parse_order_submission_response(&body, order)
    }

    pub fn cancel_live_order<C: WallClock, T: LiveOrderTransport>(
        &self,
        transport: &T,
        signer: &impl RequestSigner,
        cancel: &CancelOrderRequest,
        existing_report: Option<&ExecutionReport>,
        clock: &C,
    ) -> Result<ExecutionReport, OkxClientError> {
        if !self.config.live_trading || self.config.mode != RuntimeMode::Live {
            return Err(OkxClientError::LiveTradingDisabled);
        }

        let request = self.build_signed_cancel_order_request(signer, cancel, clock)?;
        let body = transport.execute(
            self.rest_endpoint(),
            &request,
            self.config.network.request_timeout_ms,
        )?;
        parse_cancel_order_response(&body, cancel, existing_report)
    }

    pub fn request_withdrawal_stub(&self) -> Result<(), OkxClientError> {
        Err(OkxClientError::UnsupportedLiveOperation(
            "withdrawals are not available",
        ))
    }

    fn credentials(&self) -> Result<(&str, &str, &str), OkxClientError> {
        let credentials = &self.config.okx.credentials;
        match (
            credentials.api_key.as_deref(),
            credentials.api_secret.as_deref(),
            credentials.passphrase.as_deref(),
        ) {
            (Some(k), Some(s), Some(p)) => Ok((k, s, p)),
            _ => Err(OkxClientError::MissingCredentials),
        }
    }
}

#[derive(Debug, Clone)]
pub struct RestRequest {
    pub method: String,
    pub path: String,
    pub body: String,
    pub priority: RequestPriority,
}

impl RestRequest {
    #[must_use]
    pub fn new(method: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            method: method.into(),
            path: path.into(),
            body: String::new(),
            priority: RequestPriority::Normal,
        }
    }

    #[must_use]
    pub fn with_body(mut self, body: impl Into<String>) -> Self {
        self.body = body.into();
        self
    }

    #[must_use]
    pub fn with_priority(mut self, priority: RequestPriority) -> Self {
        self.priority = priority;
        self
    }
}

#[derive(Debug, Clone)]
pub struct SignedRequest {
    pub request: RestRequest,
    pub headers: BTreeMap<String, String>,
}

pub trait LiveOrderTransport {
    fn execute(
        &self,
        endpoint: &Url,
        request: &SignedRequest,
        timeout_ms: u64,
    ) -> Result<String, OkxClientError>;
}

#[derive(Debug, Clone)]
pub struct ReqwestLiveOrderTransport {
    client: Client,
}

impl ReqwestLiveOrderTransport {
    pub fn new() -> Result<Self, OkxClientError> {
        let client = Client::builder()
            .build()
            .map_err(OkxClientError::TransportBuild)?;
        Ok(Self { client })
    }
}

impl Default for ReqwestLiveOrderTransport {
    fn default() -> Self {
        Self::new().expect("reqwest transport should build")
    }
}

impl LiveOrderTransport for ReqwestLiveOrderTransport {
    fn execute(
        &self,
        endpoint: &Url,
        request: &SignedRequest,
        timeout_ms: u64,
    ) -> Result<String, OkxClientError> {
        let url = endpoint
            .join(request.request.path.trim_start_matches('/'))
            .map_err(OkxClientError::JoinUrl)?;
        let mut builder =
            self.client
                .request(
                    request.request.method.parse().map_err(|_| {
                        OkxClientError::InvalidMethod(request.request.method.clone())
                    })?,
                    url,
                )
                .timeout(std::time::Duration::from_millis(timeout_ms))
                .body(request.request.body.clone());

        for (key, value) in &request.headers {
            builder = builder.header(key, value);
        }

        let response = builder.send().map_err(OkxClientError::Transport)?;
        let status = response.status();
        let body = response.text().map_err(OkxClientError::ReadResponse)?;
        if !status.is_success() {
            return Err(OkxClientError::ApiRejected {
                code: status.as_u16().to_string(),
                message: body,
            });
        }

        Ok(body)
    }
}

#[derive(Debug, Serialize)]
struct WsLoginCommand<'a> {
    op: &'static str,
    args: Vec<WsLoginArg<'a>>,
}

#[derive(Debug, Serialize)]
struct WsLoginArg<'a> {
    #[serde(rename = "apiKey")]
    api_key: &'a str,
    passphrase: &'a str,
    timestamp: &'a str,
    sign: &'a str,
}

#[derive(Debug, Serialize)]
struct OkxOrderBody<'a> {
    #[serde(rename = "instId")]
    inst_id: &'a str,
    side: &'static str,
    #[serde(rename = "ordType")]
    ord_type: &'static str,
    sz: Cow<'a, str>,
    #[serde(rename = "clOrdId", skip_serializing_if = "Option::is_none")]
    cl_ord_id: Option<&'a str>,
    #[serde(rename = "px", skip_serializing_if = "Option::is_none")]
    px: Option<Cow<'a, str>>,
}

impl<'a> From<&'a OrderRequest> for OkxOrderBody<'a> {
    fn from(order: &'a OrderRequest) -> Self {
        Self {
            inst_id: order.instrument_id.as_str(),
            side: match order.side {
                OrderSide::Buy => "buy",
                OrderSide::Sell => "sell",
            },
            ord_type: match order.order_type {
                OrderType::Market => "market",
                OrderType::Limit => "limit",
            },
            sz: Cow::Owned(order.quantity.to_string()),
            cl_ord_id: order.client_order_id.as_deref(),
            px: order.limit_price.map(|value| Cow::Owned(value.to_string())),
        }
    }
}

#[derive(Debug, Serialize)]
struct OkxCancelOrderBody<'a> {
    #[serde(rename = "instId")]
    inst_id: &'a str,
    #[serde(rename = "ordId", skip_serializing_if = "Option::is_none")]
    ord_id: Option<&'a str>,
    #[serde(rename = "clOrdId", skip_serializing_if = "Option::is_none")]
    cl_ord_id: Option<&'a str>,
}

impl<'a> From<&'a CancelOrderRequest> for OkxCancelOrderBody<'a> {
    fn from(cancel: &'a CancelOrderRequest) -> Self {
        Self {
            inst_id: cancel.instrument_id.as_str(),
            ord_id: cancel.order_id.as_deref(),
            cl_ord_id: cancel.client_order_id.as_deref(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct OkxEnvelope<T> {
    code: String,
    msg: String,
    data: Vec<T>,
}

#[derive(Debug, Deserialize)]
struct OkxOrderAck {
    #[serde(rename = "ordId")]
    ord_id: Option<String>,
    #[serde(rename = "clOrdId")]
    cl_ord_id: Option<String>,
    #[serde(rename = "sCode")]
    status_code: Option<String>,
    #[serde(rename = "sMsg")]
    status_message: Option<String>,
}

pub trait RequestSigner {
    fn sign(&self, secret: &str, payload: &str) -> Result<String, OkxClientError>;
}

#[derive(Debug, Default)]
pub struct HmacSha256Signer;

impl RequestSigner for HmacSha256Signer {
    fn sign(&self, secret: &str, payload: &str) -> Result<String, OkxClientError> {
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
            .map_err(|_| OkxClientError::InvalidSigningSecret)?;
        mac.update(payload.as_bytes());

        Ok(STANDARD.encode(mac.finalize().into_bytes()))
    }
}

#[must_use]
pub fn signature_payload(timestamp: &str, method: &str, path: &str, body: &str) -> String {
    format!("{}{}{}{}", timestamp, method.to_uppercase(), path, body)
}

#[must_use]
pub fn exchange_timestamp_ms<C: WallClock>(clock: &C) -> ExchangeTimestamp {
    ExchangeTimestamp::from_datetime(clock.now_utc())
}

fn parse_order_submission_response(
    body: &str,
    order: &OrderRequest,
) -> Result<ExecutionReport, OkxClientError> {
    let envelope: OkxEnvelope<OkxOrderAck> =
        serde_json::from_str(body).map_err(OkxClientError::DeserializePayload)?;
    if envelope.code != "0" {
        return Err(OkxClientError::ApiRejected {
            code: envelope.code,
            message: envelope.msg,
        });
    }

    let ack = envelope
        .data
        .into_iter()
        .next()
        .ok_or(OkxClientError::MissingOrderAck)?;
    if ack.status_code.as_deref().unwrap_or("0") != "0" {
        return Err(OkxClientError::ApiRejected {
            code: ack.status_code.unwrap_or_else(|| "unknown".to_string()),
            message: ack
                .status_message
                .unwrap_or_else(|| "order rejected".to_string()),
        });
    }

    Ok(ExecutionReport {
        order_id: ack
            .ord_id
            .clone()
            .or_else(|| ack.cl_ord_id.clone())
            .unwrap_or_else(|| order.instrument_id.to_string()),
        client_order_id: ack.cl_ord_id.or_else(|| order.client_order_id.clone()),
        venue_order_id: ack.ord_id,
        instrument_id: order.instrument_id.clone(),
        status: OrderStatus::Accepted,
        filled_quantity: 0.0,
        remaining_quantity: order.quantity,
        average_fill_price: None,
        rejection_reason: None,
    })
}

fn parse_cancel_order_response(
    body: &str,
    cancel: &CancelOrderRequest,
    existing_report: Option<&ExecutionReport>,
) -> Result<ExecutionReport, OkxClientError> {
    let envelope: OkxEnvelope<OkxOrderAck> =
        serde_json::from_str(body).map_err(OkxClientError::DeserializePayload)?;
    if envelope.code != "0" {
        return Err(OkxClientError::ApiRejected {
            code: envelope.code,
            message: envelope.msg,
        });
    }

    let ack = envelope
        .data
        .into_iter()
        .next()
        .ok_or(OkxClientError::MissingOrderAck)?;
    if ack.status_code.as_deref().unwrap_or("0") != "0" {
        return Err(OkxClientError::ApiRejected {
            code: ack.status_code.unwrap_or_else(|| "unknown".to_string()),
            message: ack
                .status_message
                .unwrap_or_else(|| "order cancellation rejected".to_string()),
        });
    }

    Ok(ExecutionReport {
        order_id: ack
            .ord_id
            .clone()
            .or_else(|| existing_report.map(|report| report.order_id.clone()))
            .or_else(|| cancel.order_id.clone())
            .or_else(|| ack.cl_ord_id.clone())
            .or_else(|| cancel.client_order_id.clone())
            .unwrap_or_else(|| cancel.instrument_id.to_string()),
        client_order_id: ack
            .cl_ord_id
            .or_else(|| existing_report.and_then(|report| report.client_order_id.clone()))
            .or_else(|| cancel.client_order_id.clone()),
        venue_order_id: ack
            .ord_id
            .or_else(|| existing_report.and_then(|report| report.venue_order_id.clone()))
            .or_else(|| cancel.order_id.clone()),
        instrument_id: cancel.instrument_id.clone(),
        status: OrderStatus::Cancelled,
        filled_quantity: existing_report.map_or(0.0, |report| report.filled_quantity),
        remaining_quantity: existing_report.map_or(0.0, |report| report.remaining_quantity),
        average_fill_price: existing_report.and_then(|report| report.average_fill_price),
        rejection_reason: None,
    })
}

#[derive(Debug, Error)]
pub enum OkxClientError {
    #[error("api credentials are required for authenticated requests")]
    MissingCredentials,
    #[error("invalid signing secret")]
    InvalidSigningSecret,
    #[error("failed to sign request")]
    Signing,
    #[error("failed to serialize OKX payload: {0}")]
    SerializePayload(serde_json::Error),
    #[error("failed to deserialize OKX payload: {0}")]
    DeserializePayload(serde_json::Error),
    #[error("failed to build HTTP transport: {0}")]
    TransportBuild(reqwest::Error),
    #[error("failed to execute HTTP request: {0}")]
    Transport(reqwest::Error),
    #[error("failed to read HTTP response: {0}")]
    ReadResponse(reqwest::Error),
    #[error("invalid HTTP method: {0}")]
    InvalidMethod(String),
    #[error("failed to join endpoint URL: {0}")]
    JoinUrl(url::ParseError),
    #[error("live trading is disabled by runtime configuration")]
    LiveTradingDisabled,
    #[error("unsupported live operation: {0}")]
    UnsupportedLiveOperation(&'static str),
    #[error("invalid order request: {0}")]
    InvalidOrder(String),
    #[error("invalid cancel order request: {0}")]
    InvalidCancelOrder(String),
    #[error("OKX API rejected request ({code}): {message}")]
    ApiRejected { code: String, message: String },
    #[error("order submission response did not include an acknowledgement")]
    MissingOrderAck,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, TimeZone, Utc};
    use qae_config::ApiCredentials;
    use qae_core::InstrumentId;

    struct FixedClock(DateTime<Utc>);

    impl WallClock for FixedClock {
        fn now_utc(&self) -> DateTime<Utc> {
            self.0
        }
    }

    #[derive(Debug, Default)]
    struct MockTransport {
        response: String,
    }

    impl LiveOrderTransport for MockTransport {
        fn execute(
            &self,
            _endpoint: &Url,
            _request: &SignedRequest,
            _timeout_ms: u64,
        ) -> Result<String, OkxClientError> {
            Ok(self.response.clone())
        }
    }

    #[test]
    fn produces_signing_payload() {
        let payload = signature_payload("t", "post", "/api/v5/trade/order", "{}");
        assert_eq!(payload, "tPOST/api/v5/trade/order{}");
    }

    #[test]
    fn adds_demo_header_when_enabled() {
        let mut cfg = AppConfig::default();
        cfg.okx.credentials = ApiCredentials {
            api_key: Some("k".into()),
            api_secret: Some("s".into()),
            passphrase: Some("p".into()),
        };
        cfg.okx.demo_trading = true;

        let client = OkxClient::new(cfg);
        let request = RestRequest::new("GET", "/api/v5/account/balance");
        let signed = client
            .build_authenticated_request(
                &HmacSha256Signer,
                request,
                &FixedClock(
                    Utc.timestamp_millis_opt(1_700_000_000_000)
                        .single()
                        .expect("valid ts"),
                ),
            )
            .expect("signing should succeed");

        assert_eq!(
            signed
                .headers
                .get(OKX_SIMULATED_TRADING)
                .map(String::as_str),
            Some("1")
        );
    }

    #[test]
    fn builds_websocket_login_command() {
        let mut cfg = AppConfig::default();
        cfg.okx.credentials = ApiCredentials {
            api_key: Some("k".into()),
            api_secret: Some("s".into()),
            passphrase: Some("p".into()),
        };

        let command = OkxClient::new(cfg)
            .build_ws_login_command(
                &HmacSha256Signer,
                &FixedClock(
                    Utc.timestamp_millis_opt(1_700_000_000_000)
                        .single()
                        .expect("valid ts"),
                ),
            )
            .expect("login command");

        assert!(command.contains(r#""op":"login""#));
        assert!(command.contains(r#""apiKey":"k""#));
        assert!(command.contains(r#""timestamp":"1700000000""#));
    }

    #[test]
    fn builds_signed_order_request() {
        let mut cfg = AppConfig::default();
        cfg.okx.credentials = ApiCredentials {
            api_key: Some("k".into()),
            api_secret: Some("s".into()),
            passphrase: Some("p".into()),
        };

        let order = OrderRequest::new(
            InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
            OrderSide::Buy,
            OrderType::Limit,
            2.0,
        )
        .and_then(|order| order.with_limit_price(100.5))
        .expect("order")
        .with_client_order_id("cid-1");

        let signed = OkxClient::new(cfg)
            .build_signed_order_request(
                &HmacSha256Signer,
                &order,
                &FixedClock(
                    Utc.timestamp_millis_opt(1_700_000_000_000)
                        .single()
                        .expect("valid ts"),
                ),
            )
            .expect("signed request");

        assert_eq!(signed.request.path, OKX_ORDER_PATH);
        assert!(signed.request.body.contains(r#""instId":"BTC-USDT-SWAP""#));
        assert!(signed.request.body.contains(r#""clOrdId":"cid-1""#));
        assert_eq!(
            signed.headers.get(OKX_ACCESS_KEY).map(String::as_str),
            Some("k")
        );
    }

    #[test]
    fn builds_signed_cancel_order_request() {
        let mut cfg = AppConfig::default();
        cfg.okx.credentials = ApiCredentials {
            api_key: Some("k".into()),
            api_secret: Some("s".into()),
            passphrase: Some("p".into()),
        };

        let cancel =
            CancelOrderRequest::new(InstrumentId::new("BTC-USDT-SWAP").expect("instrument"))
                .with_client_order_id("cid-1");

        let signed = OkxClient::new(cfg)
            .build_signed_cancel_order_request(
                &HmacSha256Signer,
                &cancel,
                &FixedClock(
                    Utc.timestamp_millis_opt(1_700_000_000_000)
                        .single()
                        .expect("valid ts"),
                ),
            )
            .expect("signed request");

        assert_eq!(signed.request.path, OKX_CANCEL_ORDER_PATH);
        assert!(signed.request.body.contains(r#""instId":"BTC-USDT-SWAP""#));
        assert!(signed.request.body.contains(r#""clOrdId":"cid-1""#));
    }

    #[test]
    fn submits_live_order_and_parses_ack() {
        let mut cfg = AppConfig {
            mode: RuntimeMode::Live,
            live_trading: true,
            ..AppConfig::default()
        };
        cfg.okx.credentials = ApiCredentials {
            api_key: Some("k".into()),
            api_secret: Some("s".into()),
            passphrase: Some("p".into()),
        };

        let order = OrderRequest::new(
            InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
            OrderSide::Buy,
            OrderType::Limit,
            1.0,
        )
        .and_then(|order| order.with_limit_price(100.0))
        .expect("order")
        .with_client_order_id("cid-7");

        let report = OkxClient::new(cfg)
            .submit_live_order(
                &MockTransport {
                    response: r#"{"code":"0","msg":"","data":[{"ordId":"42","clOrdId":"cid-7","sCode":"0","sMsg":""}]}"#.to_string(),
                },
                &HmacSha256Signer,
                &order,
                &FixedClock(
                    Utc.timestamp_millis_opt(1_700_000_000_000)
                        .single()
                        .expect("valid ts"),
                ),
            )
            .expect("report");

        assert_eq!(report.order_id, "42");
        assert_eq!(report.client_order_id.as_deref(), Some("cid-7"));
        assert_eq!(report.venue_order_id.as_deref(), Some("42"));
        assert_eq!(report.status, OrderStatus::Accepted);
    }

    #[test]
    fn cancels_live_order_and_preserves_existing_quantities() {
        let mut cfg = AppConfig {
            mode: RuntimeMode::Live,
            live_trading: true,
            ..AppConfig::default()
        };
        cfg.okx.credentials = ApiCredentials {
            api_key: Some("k".into()),
            api_secret: Some("s".into()),
            passphrase: Some("p".into()),
        };

        let cancel =
            CancelOrderRequest::new(InstrumentId::new("BTC-USDT-SWAP").expect("instrument"))
                .with_order_id("42")
                .with_client_order_id("cid-7");
        let existing = ExecutionReport {
            order_id: "42".to_string(),
            client_order_id: Some("cid-7".to_string()),
            venue_order_id: Some("42".to_string()),
            instrument_id: InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
            status: OrderStatus::PartiallyFilled,
            filled_quantity: 0.5,
            remaining_quantity: 0.5,
            average_fill_price: Some(100.0),
            rejection_reason: None,
        };

        let report = OkxClient::new(cfg)
            .cancel_live_order(
                &MockTransport {
                    response: r#"{"code":"0","msg":"","data":[{"ordId":"42","clOrdId":"cid-7","sCode":"0","sMsg":""}]}"#.to_string(),
                },
                &HmacSha256Signer,
                &cancel,
                Some(&existing),
                &FixedClock(
                    Utc.timestamp_millis_opt(1_700_000_000_000)
                        .single()
                        .expect("valid ts"),
                ),
            )
            .expect("report");

        assert_eq!(report.order_id, "42");
        assert_eq!(report.client_order_id.as_deref(), Some("cid-7"));
        assert_eq!(report.status, OrderStatus::Cancelled);
        assert_eq!(report.filled_quantity, 0.5);
        assert_eq!(report.remaining_quantity, 0.5);
        assert_eq!(report.average_fill_price, Some(100.0));
    }
}
