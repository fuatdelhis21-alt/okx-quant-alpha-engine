use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use hmac::{Hmac, Mac};
use qae_config::{AppConfig, WsEndpointKind};
use qae_core::{RequestPriority, RuntimeMode};
use qae_time::{ExchangeTimestamp, WallClock};
use sha2::Sha256;
use std::collections::BTreeMap;
use thiserror::Error;
use url::Url;

pub const OKX_ACCESS_KEY: &str = "OK-ACCESS-KEY";
pub const OKX_ACCESS_SIGN: &str = "OK-ACCESS-SIGN";
pub const OKX_ACCESS_TIMESTAMP: &str = "OK-ACCESS-TIMESTAMP";
pub const OKX_ACCESS_PASSPHRASE: &str = "OK-ACCESS-PASSPHRASE";
pub const OKX_SIMULATED_TRADING: &str = "x-simulated-trading";

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
        let credentials = &self.config.okx.credentials;
        let (api_key, api_secret, passphrase) = match (
            credentials.api_key.as_deref(),
            credentials.api_secret.as_deref(),
            credentials.passphrase.as_deref(),
        ) {
            (Some(k), Some(s), Some(p)) => (k, s, p),
            _ => return Err(OkxClientError::MissingCredentials),
        };

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
    #[error("live trading is disabled by runtime configuration")]
    LiveTradingDisabled,
    #[error("unsupported live operation: {0}")]
    UnsupportedLiveOperation(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, TimeZone, Utc};
    use qae_config::ApiCredentials;

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
}
