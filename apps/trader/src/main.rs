use anyhow::{Context, Result};
use qae_config::AppConfig;
use qae_execution::PaperExecutionEngine;
use qae_monitoring::{MonitoringConfig, init_tracing};
use qae_risk::RiskManager;
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
    let risk_manager = RiskManager::new(&config).context("risk manager configuration invalid")?;
    let execution_engine =
        PaperExecutionEngine::new(&config).context("paper execution configuration invalid")?;

    info!(
        state = ?websocket_engine.state,
        reconnect_limit = websocket_engine.reconnect_policy.max_attempts,
        subscriptions = websocket_engine.subscriptions().len(),
        subscribe_requests = websocket_engine.subscription_requests().len(),
        "market data infrastructure initialized"
    );
    info!(
        ?risk_manager,
        ?execution_engine,
        "risk and paper execution scaffolding initialized"
    );

    Ok(())
}
