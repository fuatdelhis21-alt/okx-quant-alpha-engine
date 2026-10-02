use qae_core::RuntimeMode;
use serde::Deserialize;
use std::net::SocketAddr;
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
    pub market_data: MarketDataConfig,
    pub risk: RiskConfig,
    pub execution: ExecutionConfig,
    pub strategy: StrategyConfig,
    pub control: ControlConfig,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            live_trading: false,
            mode: RuntimeMode::Paper,
            okx: OkxConfig::default(),
            network: NetworkConfig::default(),
            market_data: MarketDataConfig::default(),
            risk: RiskConfig::default(),
            execution: ExecutionConfig::default(),
            strategy: StrategyConfig::default(),
            control: ControlConfig::default(),
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

        if let Some(value) = map.get("MARKET_DATA_INSTRUMENTS") {
            self.market_data.instruments = parse_csv_list(value)?;
        }

        if let Some(value) = map.get("SUBSCRIBE_TICKERS") {
            self.market_data.subscribe_tickers = parse_bool("SUBSCRIBE_TICKERS", value)?;
        }

        if let Some(value) = map.get("SUBSCRIBE_ORDER_BOOK") {
            self.market_data.subscribe_order_book = parse_bool("SUBSCRIBE_ORDER_BOOK", value)?;
        }

        if let Some(value) = map.get("ORDER_BOOK_DEPTH") {
            self.market_data.order_book_depth = parse_u32("ORDER_BOOK_DEPTH", value)?;
        }

        if let Some(value) = map.get("CONNECT_PUBLIC_STREAM_ON_STARTUP") {
            self.market_data.connect_public_stream_on_startup =
                parse_bool("CONNECT_PUBLIC_STREAM_ON_STARTUP", value)?;
        }

        if let Some(value) = map.get("CONNECT_PRIVATE_STREAM_ON_STARTUP") {
            self.market_data.connect_private_stream_on_startup =
                parse_bool("CONNECT_PRIVATE_STREAM_ON_STARTUP", value)?;
        }

        if let Some(value) = map.get("STARTUP_STREAM_UPDATES_LIMIT") {
            self.market_data.startup_stream_updates_limit =
                parse_u32("STARTUP_STREAM_UPDATES_LIMIT", value)?;
        }

        if let Some(value) = map.get("STARTUP_STREAM_TIMEOUT_MS") {
            self.market_data.startup_stream_timeout_ms =
                parse_u64("STARTUP_STREAM_TIMEOUT_MS", value)?;
        }

        if let Some(value) = map.get("RISK_MAX_ORDER_QUANTITY") {
            self.risk.max_order_quantity = parse_f64("RISK_MAX_ORDER_QUANTITY", value)?;
        }

        if let Some(value) = map.get("RISK_MAX_ORDER_NOTIONAL") {
            self.risk.max_order_notional = parse_f64("RISK_MAX_ORDER_NOTIONAL", value)?;
        }

        if let Some(value) = map.get("RISK_MAX_PRICE_DEVIATION_BPS") {
            self.risk.max_price_deviation_bps = parse_f64("RISK_MAX_PRICE_DEVIATION_BPS", value)?;
        }

        if let Some(value) = map.get("RISK_MAX_POSITION_NOTIONAL") {
            self.risk.max_position_notional = parse_f64("RISK_MAX_POSITION_NOTIONAL", value)?;
        }

        if let Some(value) = map.get("RISK_MAX_TOTAL_EXPOSURE_NOTIONAL") {
            self.risk.max_total_exposure_notional =
                parse_f64("RISK_MAX_TOTAL_EXPOSURE_NOTIONAL", value)?;
        }

        if let Some(value) = map.get("RISK_MAX_DAILY_LOSS") {
            self.risk.max_daily_loss = parse_f64("RISK_MAX_DAILY_LOSS", value)?;
        }

        if let Some(value) = map.get("RISK_MAX_OPEN_ORDERS") {
            self.risk.max_open_orders = parse_u32("RISK_MAX_OPEN_ORDERS", value)?;
        }

        if let Some(value) = map.get("PAPER_STARTING_BALANCE_USDT") {
            self.execution.paper_starting_balance_usdt =
                parse_f64("PAPER_STARTING_BALANCE_USDT", value)?;
        }

        if let Some(value) = map.get("PAPER_FILL_SLIPPAGE_BPS") {
            self.execution.paper_fill_slippage_bps = parse_f64("PAPER_FILL_SLIPPAGE_BPS", value)?;
        }

        if let Some(value) = map.get("ENABLE_STARTUP_STRATEGY") {
            self.strategy.enable_startup_strategy = parse_bool("ENABLE_STARTUP_STRATEGY", value)?;
        }

        if let Some(value) = map.get("STRATEGY_ORDER_QUANTITY") {
            self.strategy.order_quantity = parse_f64("STRATEGY_ORDER_QUANTITY", value)?;
        }

        if let Some(value) = map.get("STRATEGY_ENTRY_SPREAD_BPS") {
            self.strategy.entry_spread_bps = parse_f64("STRATEGY_ENTRY_SPREAD_BPS", value)?;
        }

        if let Some(value) = map.get("STRATEGY_CANCEL_SPREAD_BPS") {
            self.strategy.cancel_spread_bps = parse_f64("STRATEGY_CANCEL_SPREAD_BPS", value)?;
        }

        if let Some(value) = map.get("STRATEGY_MAX_SIGNALS_PER_RUN") {
            self.strategy.max_signals_per_run = parse_u32("STRATEGY_MAX_SIGNALS_PER_RUN", value)?;
        }

        if let Some(value) = map.get("ENABLE_STARTUP_CONTROL_SERVER") {
            self.control.enable_startup_control_server =
                parse_bool("ENABLE_STARTUP_CONTROL_SERVER", value)?;
        }

        if let Some(value) = map.get("STARTUP_CONTROL_BIND_ADDRESS") {
            self.control.startup_control_bind_address =
                parse_socket_addr("STARTUP_CONTROL_BIND_ADDRESS", value)?;
        }

        if let Some(value) = map.get("STARTUP_CONTROL_TIMEOUT_MS") {
            self.control.startup_control_timeout_ms =
                parse_u64("STARTUP_CONTROL_TIMEOUT_MS", value)?;
        }

        if let Some(value) = map.get("STARTUP_CONTROL_MAX_REQUESTS") {
            self.control.startup_control_max_requests =
                parse_u32("STARTUP_CONTROL_MAX_REQUESTS", value)?;
        }

        self.okx.credentials = ApiCredentials {
            api_key: map
                .get("OKX_API_KEY")
                .cloned()
                .filter(|v| !v.trim().is_empty()),
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

        if self.market_data.instruments.is_empty() {
            return Err(ConfigError::EmptyMarketDataInstruments);
        }

        if !self.market_data.subscribe_tickers && !self.market_data.subscribe_order_book {
            return Err(ConfigError::NoMarketDataSubscriptions);
        }

        if self.market_data.order_book_depth == 0 {
            return Err(ConfigError::InvalidValue {
                name: "ORDER_BOOK_DEPTH",
                value: self.market_data.order_book_depth.to_string(),
            });
        }

        if self.market_data.startup_stream_updates_limit == 0 {
            return Err(ConfigError::InvalidValue {
                name: "STARTUP_STREAM_UPDATES_LIMIT",
                value: self.market_data.startup_stream_updates_limit.to_string(),
            });
        }

        if self.market_data.startup_stream_timeout_ms == 0 {
            return Err(ConfigError::InvalidValue {
                name: "STARTUP_STREAM_TIMEOUT_MS",
                value: self.market_data.startup_stream_timeout_ms.to_string(),
            });
        }

        for (name, value) in [
            ("RISK_MAX_ORDER_QUANTITY", self.risk.max_order_quantity),
            ("RISK_MAX_ORDER_NOTIONAL", self.risk.max_order_notional),
            (
                "RISK_MAX_PRICE_DEVIATION_BPS",
                self.risk.max_price_deviation_bps,
            ),
            (
                "RISK_MAX_POSITION_NOTIONAL",
                self.risk.max_position_notional,
            ),
            (
                "RISK_MAX_TOTAL_EXPOSURE_NOTIONAL",
                self.risk.max_total_exposure_notional,
            ),
            ("RISK_MAX_DAILY_LOSS", self.risk.max_daily_loss),
            (
                "PAPER_STARTING_BALANCE_USDT",
                self.execution.paper_starting_balance_usdt,
            ),
        ] {
            if !value.is_finite() || value <= 0.0 {
                return Err(ConfigError::InvalidValue {
                    name,
                    value: value.to_string(),
                });
            }
        }

        if !self.execution.paper_fill_slippage_bps.is_finite()
            || self.execution.paper_fill_slippage_bps <= 0.0
            || self.execution.paper_fill_slippage_bps >= 10_000.0
        {
            return Err(ConfigError::InvalidValue {
                name: "PAPER_FILL_SLIPPAGE_BPS",
                value: self.execution.paper_fill_slippage_bps.to_string(),
            });
        }

        if self.risk.max_open_orders == 0 {
            return Err(ConfigError::InvalidValue {
                name: "RISK_MAX_OPEN_ORDERS",
                value: self.risk.max_open_orders.to_string(),
            });
        }

        for (name, value) in [
            ("STRATEGY_ORDER_QUANTITY", self.strategy.order_quantity),
            ("STRATEGY_ENTRY_SPREAD_BPS", self.strategy.entry_spread_bps),
            (
                "STRATEGY_CANCEL_SPREAD_BPS",
                self.strategy.cancel_spread_bps,
            ),
        ] {
            if !value.is_finite() || value <= 0.0 {
                return Err(ConfigError::InvalidValue {
                    name,
                    value: value.to_string(),
                });
            }
        }

        if self.strategy.cancel_spread_bps < self.strategy.entry_spread_bps {
            return Err(ConfigError::InvalidValue {
                name: "STRATEGY_CANCEL_SPREAD_BPS",
                value: self.strategy.cancel_spread_bps.to_string(),
            });
        }

        if self.strategy.max_signals_per_run == 0 {
            return Err(ConfigError::InvalidValue {
                name: "STRATEGY_MAX_SIGNALS_PER_RUN",
                value: self.strategy.max_signals_per_run.to_string(),
            });
        }

        if self.control.startup_control_timeout_ms == 0 {
            return Err(ConfigError::InvalidValue {
                name: "STARTUP_CONTROL_TIMEOUT_MS",
                value: self.control.startup_control_timeout_ms.to_string(),
            });
        }

        if self.control.startup_control_max_requests == 0 {
            return Err(ConfigError::InvalidValue {
                name: "STARTUP_CONTROL_MAX_REQUESTS",
                value: self.control.startup_control_max_requests.to_string(),
            });
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
                    self.okx.endpoints.public_ws =
                        parse_url("okx.endpoints.public_ws", &public_ws)?;
                }
                if let Some(private_ws) = endpoints.private_ws {
                    self.okx.endpoints.private_ws =
                        parse_url("okx.endpoints.private_ws", &private_ws)?;
                }
                if let Some(business_ws) = endpoints.business_ws {
                    self.okx.endpoints.business_ws =
                        parse_url("okx.endpoints.business_ws", &business_ws)?;
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

        if let Some(market_data) = file_cfg.market_data {
            if let Some(instruments) = market_data.instruments {
                self.market_data.instruments = instruments;
            }
            if let Some(subscribe_tickers) = market_data.subscribe_tickers {
                self.market_data.subscribe_tickers = subscribe_tickers;
            }
            if let Some(subscribe_order_book) = market_data.subscribe_order_book {
                self.market_data.subscribe_order_book = subscribe_order_book;
            }
            if let Some(order_book_depth) = market_data.order_book_depth {
                self.market_data.order_book_depth = order_book_depth;
            }
            if let Some(connect_public_stream_on_startup) =
                market_data.connect_public_stream_on_startup
            {
                self.market_data.connect_public_stream_on_startup =
                    connect_public_stream_on_startup;
            }
            if let Some(connect_private_stream_on_startup) =
                market_data.connect_private_stream_on_startup
            {
                self.market_data.connect_private_stream_on_startup =
                    connect_private_stream_on_startup;
            }
            if let Some(startup_stream_updates_limit) = market_data.startup_stream_updates_limit {
                self.market_data.startup_stream_updates_limit = startup_stream_updates_limit;
            }
            if let Some(startup_stream_timeout_ms) = market_data.startup_stream_timeout_ms {
                self.market_data.startup_stream_timeout_ms = startup_stream_timeout_ms;
            }
        }

        if let Some(risk) = file_cfg.risk {
            if let Some(max_order_quantity) = risk.max_order_quantity {
                self.risk.max_order_quantity = max_order_quantity;
            }
            if let Some(max_order_notional) = risk.max_order_notional {
                self.risk.max_order_notional = max_order_notional;
            }
            if let Some(max_price_deviation_bps) = risk.max_price_deviation_bps {
                self.risk.max_price_deviation_bps = max_price_deviation_bps;
            }
            if let Some(max_position_notional) = risk.max_position_notional {
                self.risk.max_position_notional = max_position_notional;
            }
            if let Some(max_total_exposure_notional) = risk.max_total_exposure_notional {
                self.risk.max_total_exposure_notional = max_total_exposure_notional;
            }
            if let Some(max_daily_loss) = risk.max_daily_loss {
                self.risk.max_daily_loss = max_daily_loss;
            }
            if let Some(max_open_orders) = risk.max_open_orders {
                self.risk.max_open_orders = max_open_orders;
            }
        }

        if let Some(execution) = file_cfg.execution {
            if let Some(paper_starting_balance_usdt) = execution.paper_starting_balance_usdt {
                self.execution.paper_starting_balance_usdt = paper_starting_balance_usdt;
            }
            if let Some(paper_fill_slippage_bps) = execution.paper_fill_slippage_bps {
                self.execution.paper_fill_slippage_bps = paper_fill_slippage_bps;
            }
        }

        if let Some(strategy) = file_cfg.strategy {
            if let Some(enable_startup_strategy) = strategy.enable_startup_strategy {
                self.strategy.enable_startup_strategy = enable_startup_strategy;
            }
            if let Some(order_quantity) = strategy.order_quantity {
                self.strategy.order_quantity = order_quantity;
            }
            if let Some(entry_spread_bps) = strategy.entry_spread_bps {
                self.strategy.entry_spread_bps = entry_spread_bps;
            }
            if let Some(cancel_spread_bps) = strategy.cancel_spread_bps {
                self.strategy.cancel_spread_bps = cancel_spread_bps;
            }
            if let Some(max_signals_per_run) = strategy.max_signals_per_run {
                self.strategy.max_signals_per_run = max_signals_per_run;
            }
        }

        if let Some(control) = file_cfg.control {
            if let Some(enable_startup_control_server) = control.enable_startup_control_server {
                self.control.enable_startup_control_server = enable_startup_control_server;
            }
            if let Some(startup_control_bind_address) = control.startup_control_bind_address {
                self.control.startup_control_bind_address =
                    parse_socket_addr(
                        "control.startup_control_bind_address",
                        &startup_control_bind_address,
                    )?;
            }
            if let Some(startup_control_timeout_ms) = control.startup_control_timeout_ms {
                self.control.startup_control_timeout_ms = startup_control_timeout_ms;
            }
            if let Some(startup_control_max_requests) = control.startup_control_max_requests {
                self.control.startup_control_max_requests = startup_control_max_requests;
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

#[derive(Debug, Clone)]
pub struct MarketDataConfig {
    pub instruments: Vec<String>,
    pub subscribe_tickers: bool,
    pub subscribe_order_book: bool,
    pub order_book_depth: u32,
    pub connect_public_stream_on_startup: bool,
    pub connect_private_stream_on_startup: bool,
    pub startup_stream_updates_limit: u32,
    pub startup_stream_timeout_ms: u64,
}

impl Default for MarketDataConfig {
    fn default() -> Self {
        Self {
            instruments: vec!["BTC-USDT-SWAP".to_string()],
            subscribe_tickers: true,
            subscribe_order_book: true,
            order_book_depth: 5,
            connect_public_stream_on_startup: false,
            connect_private_stream_on_startup: false,
            startup_stream_updates_limit: 4,
            startup_stream_timeout_ms: 3_000,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RiskConfig {
    pub max_order_quantity: f64,
    pub max_order_notional: f64,
    pub max_price_deviation_bps: f64,
    pub max_position_notional: f64,
    pub max_total_exposure_notional: f64,
    pub max_daily_loss: f64,
    pub max_open_orders: u32,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            max_order_quantity: 5.0,
            max_order_notional: 100_000.0,
            max_price_deviation_bps: 500.0,
            max_position_notional: 200_000.0,
            max_total_exposure_notional: 500_000.0,
            max_daily_loss: 25_000.0,
            max_open_orders: 20,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionConfig {
    pub paper_starting_balance_usdt: f64,
    pub paper_fill_slippage_bps: f64,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            paper_starting_balance_usdt: 100_000.0,
            paper_fill_slippage_bps: 2.0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct StrategyConfig {
    pub enable_startup_strategy: bool,
    pub order_quantity: f64,
    pub entry_spread_bps: f64,
    pub cancel_spread_bps: f64,
    pub max_signals_per_run: u32,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            enable_startup_strategy: false,
            order_quantity: 1.0,
            entry_spread_bps: 75.0,
            cancel_spread_bps: 150.0,
            max_signals_per_run: 2,
        }
    }

    #[derive(Debug, Clone)]
    pub struct ControlConfig {
        pub enable_startup_control_server: bool,
        pub startup_control_bind_address: SocketAddr,
        pub startup_control_timeout_ms: u64,
        pub startup_control_max_requests: u32,
    }

    impl Default for ControlConfig {
        fn default() -> Self {
            Self {
                enable_startup_control_server: false,
                startup_control_bind_address: "127.0.0.1:9000"
                    .parse()
                    .expect("default control bind address must parse"),
                startup_control_timeout_ms: 3_000,
                startup_control_max_requests: 4,
            }
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
    LiveModeMismatch {
        mode: RuntimeMode,
        live_trading: bool,
    },
    #[error("live mode requires OKX_API_KEY, OKX_API_SECRET, and OKX_API_PASSPHRASE")]
    MissingLiveCredentials,
    #[error("market data requires at least one configured instrument")]
    EmptyMarketDataInstruments,
    #[error("at least one market data subscription must be enabled")]
    NoMarketDataSubscriptions,
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

fn parse_f64(name: &'static str, value: &str) -> Result<f64, ConfigError> {
    value
        .trim()
        .parse::<f64>()
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

fn parse_socket_addr(name: &'static str, value: &str) -> Result<SocketAddr, ConfigError> {
    value
        .trim()
        .parse::<SocketAddr>()
        .map_err(|_| ConfigError::InvalidValue {
            name,
            value: value.to_owned(),
        })
}

fn parse_csv_list(value: &str) -> Result<Vec<String>, ConfigError> {
    let parsed = value
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();

    if parsed.is_empty() {
        return Err(ConfigError::EmptyMarketDataInstruments);
    }

    Ok(parsed)
}

#[derive(Debug, Deserialize)]
struct TomlAppConfig {
    live_trading: Option<bool>,
    mode: Option<RuntimeMode>,
    okx: Option<TomlOkxConfig>,
    network: Option<TomlNetworkConfig>,
    market_data: Option<TomlMarketDataConfig>,
    risk: Option<TomlRiskConfig>,
    execution: Option<TomlExecutionConfig>,
    strategy: Option<TomlStrategyConfig>,
    control: Option<TomlControlConfig>,
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

#[derive(Debug, Deserialize)]
struct TomlMarketDataConfig {
    instruments: Option<Vec<String>>,
    subscribe_tickers: Option<bool>,
    subscribe_order_book: Option<bool>,
    order_book_depth: Option<u32>,
    connect_public_stream_on_startup: Option<bool>,
    connect_private_stream_on_startup: Option<bool>,
    startup_stream_updates_limit: Option<u32>,
    startup_stream_timeout_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct TomlRiskConfig {
    max_order_quantity: Option<f64>,
    max_order_notional: Option<f64>,
    max_price_deviation_bps: Option<f64>,
    max_position_notional: Option<f64>,
    max_total_exposure_notional: Option<f64>,
    max_daily_loss: Option<f64>,
    max_open_orders: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct TomlExecutionConfig {
    paper_starting_balance_usdt: Option<f64>,
    paper_fill_slippage_bps: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct TomlStrategyConfig {
    enable_startup_strategy: Option<bool>,
    order_quantity: Option<f64>,
    entry_spread_bps: Option<f64>,
    cancel_spread_bps: Option<f64>,
    max_signals_per_run: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct TomlControlConfig {
    enable_startup_control_server: Option<bool>,
    startup_control_bind_address: Option<String>,
    startup_control_timeout_ms: Option<u64>,
    startup_control_max_requests: Option<u32>,
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
        assert_eq!(
            cfg.market_data.instruments,
            vec!["BTC-USDT-SWAP".to_string()]
        );
        assert!(!cfg.market_data.connect_public_stream_on_startup);
        assert!(!cfg.market_data.connect_private_stream_on_startup);
        assert_eq!(cfg.risk.max_open_orders, 20);
        assert!(!cfg.strategy.enable_startup_strategy);
        assert!(!cfg.control.enable_startup_control_server);
    }

    #[test]
    fn env_overrides_endpoints() {
        let mut cfg = AppConfig::default();
        cfg.apply_env_overrides(vec![("OKX_PUBLIC_WS_ENDPOINT", "wss://example.com/public")])
            .expect("env should parse");
        assert_eq!(
            cfg.okx
                .endpoints
                .ws_endpoint(WsEndpointKind::Public)
                .as_str(),
            "wss://example.com/public"
        );
    }

    #[test]
    fn validation_requires_consistent_live_mode() {
        let cfg = AppConfig {
            live_trading: true,
            mode: RuntimeMode::Paper,
            ..AppConfig::default()
        };

        assert!(matches!(
            cfg.validate(),
            Err(ConfigError::LiveModeMismatch { .. })
        ));
    }

    #[test]
    fn live_mode_requires_credentials() {
        let cfg = AppConfig {
            live_trading: true,
            mode: RuntimeMode::Live,
            ..AppConfig::default()
        };

        assert!(matches!(
            cfg.validate(),
            Err(ConfigError::MissingLiveCredentials)
        ));
    }

    #[test]
    fn env_overrides_strategy_settings() {
        let mut cfg = AppConfig::default();
        cfg.apply_env_overrides(vec![
            ("ENABLE_STARTUP_STRATEGY", "true"),
            ("STRATEGY_ORDER_QUANTITY", "2.5"),
            ("STRATEGY_ENTRY_SPREAD_BPS", "40"),
            ("STRATEGY_CANCEL_SPREAD_BPS", "90"),
            ("STRATEGY_MAX_SIGNALS_PER_RUN", "3"),
        ])
        .expect("env should parse");

        assert!(cfg.strategy.enable_startup_strategy);
        assert_eq!(cfg.strategy.order_quantity, 2.5);
        assert_eq!(cfg.strategy.entry_spread_bps, 40.0);
        assert_eq!(cfg.strategy.cancel_spread_bps, 90.0);
        assert_eq!(cfg.strategy.max_signals_per_run, 3);
    }

    #[test]
    fn env_overrides_control_settings() {
        let mut cfg = AppConfig::default();
        cfg.apply_env_overrides(vec![
            ("ENABLE_STARTUP_CONTROL_SERVER", "true"),
            ("STARTUP_CONTROL_BIND_ADDRESS", "127.0.0.1:9100"),
            ("STARTUP_CONTROL_TIMEOUT_MS", "1500"),
            ("STARTUP_CONTROL_MAX_REQUESTS", "2"),
        ])
        .expect("env should parse");

        assert!(cfg.control.enable_startup_control_server);
        assert_eq!(
            cfg.control.startup_control_bind_address,
            "127.0.0.1:9100".parse().expect("addr")
        );
        assert_eq!(cfg.control.startup_control_timeout_ms, 1_500);
        assert_eq!(cfg.control.startup_control_max_requests, 2);
    }

    #[test]
    fn strategy_validation_rejects_cancel_threshold_below_entry() {
        let cfg = AppConfig {
            strategy: StrategyConfig {
                entry_spread_bps: 90.0,
                cancel_spread_bps: 80.0,
                ..StrategyConfig::default()
            },
            ..AppConfig::default()
        };

        assert!(matches!(
            cfg.validate(),
            Err(ConfigError::InvalidValue {
                name: "STRATEGY_CANCEL_SPREAD_BPS",
                ..
            })
        ));
    }

    #[test]
    fn live_mode_passes_with_credentials() {
        let mut cfg = AppConfig {
            live_trading: true,
            mode: RuntimeMode::Live,
            ..AppConfig::default()
        };
        cfg.okx.credentials = ApiCredentials {
            api_key: Some("k".to_string()),
            api_secret: Some("s".to_string()),
            passphrase: Some("p".to_string()),
        };

        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn env_overrides_market_data_and_risk() {
        let mut cfg = AppConfig::default();
        cfg.apply_env_overrides(vec![
            ("MARKET_DATA_INSTRUMENTS", "BTC-USDT-SWAP,ETH-USDT-SWAP"),
            ("RISK_MAX_ORDER_NOTIONAL", "25000"),
            ("RISK_MAX_OPEN_ORDERS", "5"),
            ("CONNECT_PUBLIC_STREAM_ON_STARTUP", "true"),
            ("CONNECT_PRIVATE_STREAM_ON_STARTUP", "true"),
        ])
        .expect("env should parse");

        assert_eq!(
            cfg.market_data.instruments,
            vec!["BTC-USDT-SWAP".to_string(), "ETH-USDT-SWAP".to_string()]
        );
        assert_eq!(cfg.risk.max_order_notional, 25_000.0);
        assert_eq!(cfg.risk.max_open_orders, 5);
        assert!(cfg.market_data.connect_public_stream_on_startup);
        assert!(cfg.market_data.connect_private_stream_on_startup);
    }

    #[test]
    fn validation_requires_market_data_subscription() {
        let mut cfg = AppConfig::default();
        cfg.market_data.subscribe_tickers = false;
        cfg.market_data.subscribe_order_book = false;

        assert!(matches!(
            cfg.validate(),
            Err(ConfigError::NoMarketDataSubscriptions)
        ));
    }

    #[test]
    fn validation_rejects_zero_order_book_depth() {
        let mut cfg = AppConfig::default();
        cfg.market_data.order_book_depth = 0;

        assert!(matches!(
            cfg.validate(),
            Err(ConfigError::InvalidValue {
                name: "ORDER_BOOK_DEPTH",
                ..
            })
        ));
    }

    #[test]
    fn validation_rejects_zero_startup_update_limit() {
        let mut cfg = AppConfig::default();
        cfg.market_data.startup_stream_updates_limit = 0;

        assert!(matches!(
            cfg.validate(),
            Err(ConfigError::InvalidValue {
                name: "STARTUP_STREAM_UPDATES_LIMIT",
                ..
            })
        ));
    }

    #[test]
    fn validation_rejects_invalid_slippage_bounds() {
        let mut cfg = AppConfig::default();
        cfg.execution.paper_fill_slippage_bps = 10_000.0;

        assert!(matches!(
            cfg.validate(),
            Err(ConfigError::InvalidValue {
                name: "PAPER_FILL_SLIPPAGE_BPS",
                ..
            })
        ));
    }

    #[test]
    fn validation_rejects_zero_open_order_limit() {
        let mut cfg = AppConfig::default();
        cfg.risk.max_open_orders = 0;

        assert!(matches!(
            cfg.validate(),
            Err(ConfigError::InvalidValue {
                name: "RISK_MAX_OPEN_ORDERS",
                ..
            })
        ));
    }
}
