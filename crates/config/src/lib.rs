use qae_core::RuntimeMode;
use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use thiserror::Error;
use url::Url;

const DEFAULT_REST_ENDPOINT: &str = "https://www.okx.com";
const DEFAULT_PUBLIC_WS_ENDPOINT: &str = "wss://ws.okx.com:8443/ws/v5/public";
const DEFAULT_PRIVATE_WS_ENDPOINT: &str = "wss://ws.okx.com:8443/ws/v5/private";
const DEFAULT_BUSINESS_WS_ENDPOINT: &str = "wss://ws.okx.com:8443/ws/v5/business";

#[derive(Debug, Clone)]
pub struct AppConfig {
    pub live_trading: bool,
    pub mode: RuntimeMode,
    pub okx: OkxConfig,
    pub network: NetworkConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            live_trading: false,
            mode: RuntimeMode::Paper,
            okx: OkxConfig::default(),
            network: NetworkConfig::default(),
        }
    }
}

impl AppConfig {
    pub fn load(config_path: Option<&Path>) -> Result<Self, ConfigError> {
        let mut config = Self::default();

        let selected_path = config_path.map(Path::to_path_buf).or_else(|| {
            env::var("OKX_CONFIG_FILE")
                .ok()
                .filter(|value| !value.trim().is_empty())
                .map(PathBuf::from)
        });

        if let Some(path) = selected_path {
            config.apply_toml_file(&path)?;
        }

        config.apply_env_overrides(env::vars())?;
        config.validate()?;

        Ok(config)
    }

    pub fn apply_env_overrides<I, K, V>(&mut self, vars: I) -> Result<(), ConfigError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let map: HashMap<String, String> = vars
            .into_iter()
            .map(|(k, v)| (k.as_ref().to_owned(), v.as_ref().to_owned()))
            .collect();

        if let Some(value) = map.get("LIVE_TRADING") {
            self.live_trading = parse_bool("LIVE_TRADING", value)?;
        }

        if let Some(value) = map.get("MODE") {
            self.mode = RuntimeMode::from_str(value).map_err(ConfigError::InvalidMode)?;
        }

        if let Some(value) = map.get("OKX_DEMO_TRADING") {
            self.okx.demo_trading = parse_bool("OKX_DEMO_TRADING", value)?;
        }

        if let Some(value) = map.get("OKX_REST_ENDPOINT") {
            self.okx.endpoints.rest = parse_url("OKX_REST_ENDPOINT", value)?;
        }

        if let Some(value) = map.get("OKX_PUBLIC_WS_ENDPOINT") {
            self.okx.endpoints.public_ws = parse_url("OKX_PUBLIC_WS_ENDPOINT", value)?;
        }

        if let Some(value) = map.get("OKX_PRIVATE_WS_ENDPOINT") {
            self.okx.endpoints.private_ws = parse_url("OKX_PRIVATE_WS_ENDPOINT", value)?;
        }

        if let Some(value) = map.get("OKX_BUSINESS_WS_ENDPOINT") {
            self.okx.endpoints.business_ws = parse_url("OKX_BUSINESS_WS_ENDPOINT", value)?;
        }

        if let Some(value) = map.get("REQUEST_TIMEOUT_MS") {
            self.network.request_timeout_ms = parse_u64("REQUEST_TIMEOUT_MS", value)?;
        }

        if let Some(value) = map.get("RECONNECT_MAX_ATTEMPTS") {
            self.network.reconnect_max_attempts = parse_u32("RECONNECT_MAX_ATTEMPTS", value)?;
        }

        if let Some(value) = map.get("RATE_LIMIT_PER_SECOND") {
            self.network.rate_limit_per_second = parse_u32("RATE_LIMIT_PER_SECOND", value)?;
        }

        self.okx.credentials = ApiCredentials {
            api_key: map.get("OKX_API_KEY").cloned().filter(|v| !v.trim().is_empty()),
            api_secret: map
                .get("OKX_API_SECRET")
                .cloned()
                .filter(|v| !v.trim().is_empty()),
            passphrase: map
                .get("OKX_API_PASSPHRASE")
                .cloned()
                .filter(|v| !v.trim().is_empty()),
        };

        Ok(())
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.live_trading != self.mode.is_live() {
            return Err(ConfigError::LiveModeMismatch {
                mode: self.mode,
                live_trading: self.live_trading,
            });
        }

        if self.mode.is_live() && !self.okx.credentials.is_complete() {
            return Err(ConfigError::MissingLiveCredentials);
        }

        Ok(())
    }

    fn apply_toml_file(&mut self, path: &Path) -> Result<(), ConfigError> {
        let source = fs::read_to_string(path).map_err(|source| ConfigError::ReadFile {
            path: path.to_path_buf(),
            source,
        })?;

        let file_cfg: TomlAppConfig = toml::from_str(&source).map_err(ConfigError::ParseToml)?;

        if let Some(live_trading) = file_cfg.live_trading {
            self.live_trading = live_trading;
        }

        if let Some(mode) = file_cfg.mode {
            self.mode = mode;
        }

        if let Some(okx) = file_cfg.okx {
            if let Some(demo_trading) = okx.demo_trading {
                self.okx.demo_trading = demo_trading;
            }

            if let Some(endpoints) = okx.endpoints {
                if let Some(rest) = endpoints.rest {
                    self.okx.endpoints.rest = parse_url("okx.endpoints.rest", &rest)?;
                }
                if let Some(public_ws) = endpoints.public_ws {
                    self.okx.endpoints.public_ws = parse_url("okx.endpoints.public_ws", &public_ws)?;
                }
                if let Some(private_ws) = endpoints.private_ws {
                    self.okx.endpoints.private_ws = parse_url("okx.endpoints.private_ws", &private_ws)?;
                }
                if let Some(business_ws) = endpoints.business_ws {
                    self.okx.endpoints.business_ws = parse_url("okx.endpoints.business_ws", &business_ws)?;
                }
            }
        }

        if let Some(network) = file_cfg.network {
            if let Some(request_timeout_ms) = network.request_timeout_ms {
                self.network.request_timeout_ms = request_timeout_ms;
            }
            if let Some(reconnect_max_attempts) = network.reconnect_max_attempts {
                self.network.reconnect_max_attempts = reconnect_max_attempts;
            }
            if let Some(rate_limit_per_second) = network.rate_limit_per_second {
                self.network.rate_limit_per_second = rate_limit_per_second;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct OkxConfig {
    pub endpoints: OkxEndpoints,
    pub credentials: ApiCredentials,
    pub demo_trading: bool,
}

impl Default for OkxConfig {
    fn default() -> Self {
        Self {
            endpoints: OkxEndpoints::default(),
            credentials: ApiCredentials::default(),
            demo_trading: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct OkxEndpoints {
    pub rest: Url,
    pub public_ws: Url,
    pub private_ws: Url,
    pub business_ws: Url,
}

impl OkxEndpoints {
    #[must_use]
    pub fn ws_endpoint(&self, endpoint: WsEndpointKind) -> &Url {
        match endpoint {
            WsEndpointKind::Public => &self.public_ws,
            WsEndpointKind::Private => &self.private_ws,
            WsEndpointKind::Business => &self.business_ws,
        }
    }
}

impl Default for OkxEndpoints {
    fn default() -> Self {
        Self {
            rest: Url::parse(DEFAULT_REST_ENDPOINT).expect("default rest endpoint must parse"),
            public_ws: Url::parse(DEFAULT_PUBLIC_WS_ENDPOINT)
                .expect("default public ws endpoint must parse"),
            private_ws: Url::parse(DEFAULT_PRIVATE_WS_ENDPOINT)
                .expect("default private ws endpoint must parse"),
            business_ws: Url::parse(DEFAULT_BUSINESS_WS_ENDPOINT)
                .expect("default business ws endpoint must parse"),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub enum WsEndpointKind {
    Public,
    Private,
    Business,
}

#[derive(Debug, Clone, Default)]
pub struct ApiCredentials {
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    pub passphrase: Option<String>,
}

impl ApiCredentials {
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.api_key.is_some() && self.api_secret.is_some() && self.passphrase.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct NetworkConfig {
    pub request_timeout_ms: u64,
    pub reconnect_max_attempts: u32,
    pub rate_limit_per_second: u32,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            request_timeout_ms: 5_000,
            reconnect_max_attempts: 8,
            rate_limit_per_second: 10,
        }
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("failed to read config file {path}: {source}")]
    ReadFile {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("failed to parse config file: {0}")]
    ParseToml(toml::de::Error),
    #[error("invalid runtime mode: {0}")]
    InvalidMode(qae_core::ParseRuntimeModeError),
    #[error("invalid value for {name}: {value}")]
    InvalidValue { name: &'static str, value: String },
    #[error("invalid endpoint URL for {name}: {value}")]
    InvalidUrl { name: &'static str, value: String },
    #[error("runtime mode ({mode}) must match LIVE_TRADING={live_trading}")]
    LiveModeMismatch { mode: RuntimeMode, live_trading: bool },
    #[error("live mode requires OKX_API_KEY, OKX_API_SECRET, and OKX_API_PASSPHRASE")]
    MissingLiveCredentials,
}

fn parse_bool(name: &'static str, value: &str) -> Result<bool, ConfigError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" => Ok(true),
        "0" | "false" | "no" => Ok(false),
        _ => Err(ConfigError::InvalidValue {
            name,
            value: value.to_owned(),
        }),
    }
}

fn parse_u64(name: &'static str, value: &str) -> Result<u64, ConfigError> {
    value
        .trim()
        .parse::<u64>()
        .map_err(|_| ConfigError::InvalidValue {
            name,
            value: value.to_owned(),
        })
}

fn parse_u32(name: &'static str, value: &str) -> Result<u32, ConfigError> {
    value
        .trim()
        .parse::<u32>()
        .map_err(|_| ConfigError::InvalidValue {
            name,
            value: value.to_owned(),
        })
}

fn parse_url(name: &'static str, value: &str) -> Result<Url, ConfigError> {
    Url::parse(value).map_err(|_| ConfigError::InvalidUrl {
        name,
        value: value.to_owned(),
    })
}

#[derive(Debug, Deserialize)]
struct TomlAppConfig {
    live_trading: Option<bool>,
    mode: Option<RuntimeMode>,
    okx: Option<TomlOkxConfig>,
    network: Option<TomlNetworkConfig>,
}

#[derive(Debug, Deserialize)]
struct TomlOkxConfig {
    demo_trading: Option<bool>,
    endpoints: Option<TomlOkxEndpoints>,
}

#[derive(Debug, Deserialize)]
struct TomlOkxEndpoints {
    rest: Option<String>,
    public_ws: Option<String>,
    private_ws: Option<String>,
    business_ws: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TomlNetworkConfig {
    request_timeout_ms: Option<u64>,
    reconnect_max_attempts: Option<u32>,
    rate_limit_per_second: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_safe() {
        let cfg = AppConfig::default();
        assert!(!cfg.live_trading);
        assert_eq!(cfg.mode, RuntimeMode::Paper);
        assert_eq!(cfg.okx.endpoints.rest.as_str(), "https://www.okx.com/");
        assert!(!cfg.okx.credentials.is_complete());
    }

    #[test]
    fn env_overrides_endpoints() {
        let mut cfg = AppConfig::default();
        cfg.apply_env_overrides(vec![("OKX_PUBLIC_WS_ENDPOINT", "wss://example.com/public")])
            .expect("env should parse");
        assert_eq!(
            cfg.okx.endpoints.ws_endpoint(WsEndpointKind::Public).as_str(),
            "wss://example.com/public"
        );
    }

    #[test]
    fn validation_requires_consistent_live_mode() {
        let mut cfg = AppConfig::default();
        cfg.live_trading = true;
        cfg.mode = RuntimeMode::Paper;

        assert!(matches!(
            cfg.validate(),
            Err(ConfigError::LiveModeMismatch { .. })
        ));
    }

    #[test]
    fn live_mode_requires_credentials() {
        let mut cfg = AppConfig::default();
        cfg.live_trading = true;
        cfg.mode = RuntimeMode::Live;

        assert!(matches!(
            cfg.validate(),
            Err(ConfigError::MissingLiveCredentials)
        ));
    }

    #[test]
    fn live_mode_passes_with_credentials() {
        let mut cfg = AppConfig::default();
        cfg.live_trading = true;
        cfg.mode = RuntimeMode::Live;
        cfg.okx.credentials = ApiCredentials {
            api_key: Some("k".to_string()),
            api_secret: Some("s".to_string()),
            passphrase: Some("p".to_string()),
        };

        assert!(cfg.validate().is_ok());
    }
}
