use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hmac::{Hmac, Mac};
use qae_config::{AppConfig, WsEndpointKind};
use qae_core::{OrderRequest, OrderSide, OrderType, RequestPriority, RuntimeMode};
use qae_time::{ExchangeTimestamp, WallClock};
use serde::Serialize;
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

    pub fn submit_live_order_stub(&self, _instrument: &str) -> Result<(), OkxClientError> {
        if !self.config.live_trading || self.config.mode != RuntimeMode::Live {
            return Err(OkxClientError::LiveTradingDisabled);
        }

        Err(OkxClientError::UnsupportedLiveOperation(
            "live order submission is not implemented in phase 1",
        ))
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
    #[error("live trading is disabled by runtime configuration")]
    LiveTradingDisabled,
    #[error("unsupported live operation: {0}")]
    UnsupportedLiveOperation(&'static str),
    #[error("invalid order request: {0}")]
    InvalidOrder(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, TimeZone, Utc};
    use qae_config::ApiCredentials;
    use qae_core::{InstrumentId, OrderRequest, OrderSide, OrderType};

    struct FixedClock(DateTime<Utc>);

    impl WallClock for FixedClock {
        fn now_utc(&self) -> DateTime<Utc> {
            self.0
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
    fn rejects_live_order_stub_by_default() {
        let client = OkxClient::new(AppConfig::default());
        assert!(matches!(
            client.submit_live_order_stub("BTC-USDT-SWAP"),
            Err(OkxClientError::LiveTradingDisabled)
        ));
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
}
