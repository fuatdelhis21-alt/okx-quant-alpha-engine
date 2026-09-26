use anyhow::{Context, Result};
use qae_config::AppConfig;
use qae_monitoring::{MonitoringConfig, init_tracing};
use qae_websocket_engine::WebsocketEngine;
use tracing::{info, warn};

fn main() -> Result<()> {
    let config = AppConfig::load(None).context("failed to load configuration")?;
    init_tracing(&MonitoringConfig::default());

    info!(mode = %config.mode, live_trading = config.live_trading, "starting trader");

    if !config.live_trading {
        warn!("live trading is disabled; running in safe mode");
    }

    let websocket_engine =
        WebsocketEngine::new(&config).context("websocket engine configuration invalid")?;

    info!(
        state = ?websocket_engine.state,
        reconnect_limit = websocket_engine.reconnect_policy.max_attempts,
        "phase 1 infrastructure initialized"
    );

    Ok(())
}
