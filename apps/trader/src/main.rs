use anyhow::{Context, Result};
use qae_config::AppConfig;
use qae_execution::{OmsCommand, OmsService, OmsServiceHandle, OmsServiceOutcome};
use qae_monitoring::{MonitoringConfig, init_tracing};
use qae_okx_client::{HmacSha256Signer, OkxClient};
use qae_risk::RiskManager;
use qae_strategy::{StrategyAction, StrategyRuntime};
use qae_time::SystemWallClock;
use qae_websocket_engine::WebsocketEngine;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let config = AppConfig::load(None).context("failed to load configuration")?;
    init_tracing(&MonitoringConfig::default());

    info!(mode = %config.mode, live_trading = config.live_trading, "starting trader");

    if !config.live_trading {
        warn!("live trading is disabled; running in safe mode");
    }

    let mut websocket_engine =
        WebsocketEngine::new(&config).context("websocket engine configuration invalid")?;
    let risk_manager = RiskManager::new(&config).context("risk manager configuration invalid")?;
    let mut oms_service =
        OmsService::spawn(config.clone()).context("execution service configuration invalid")?;
    let oms_handle = oms_service.handle.clone();
    let mut strategy = StrategyRuntime::new(&config);
    let mut processed_outcomes = 0usize;

    info!(
        state = ?websocket_engine.state,
        reconnect_limit = websocket_engine.reconnect_policy.max_attempts,
        subscriptions = websocket_engine.subscriptions().len(),
        subscribe_requests = websocket_engine.subscription_requests().len(),
        "market data infrastructure initialized"
    );
    info!(?risk_manager, "risk and execution scaffolding initialized");
    info!(
        strategy_enabled = strategy.is_enabled(),
        "oms service initialized"
    );

    if config.market_data.connect_public_stream_on_startup {
        let updates = websocket_engine
            .connect_public_stream(
                config.okx.endpoints.public_ws.as_str(),
                config.market_data.startup_stream_updates_limit as usize,
                Duration::from_millis(config.market_data.startup_stream_timeout_ms),
            )
            .await
            .context("failed to collect public websocket updates")?;

        for update in updates.iter().cloned() {
            oms_handle
                .publish_market_update(update.clone())
                .await
                .context("failed to publish public market update to oms service")?;

            if strategy.is_enabled() {
                let actions = strategy
                    .on_market_update(update)
                    .context("failed to evaluate strategy runtime market update")?;
                for action in actions {
                    let report = dispatch_strategy_action(&oms_handle, action)
                        .await
                        .context("failed to submit strategy action to oms service")?;
                    strategy.on_execution_report(&report);
                    info!(
                        order_id = %report.order_id,
                        status = ?report.status,
                        "processed direct oms execution report"
                    );
                    processed_outcomes += 1;
                }
            }

            processed_outcomes += drain_oms_outcomes(&mut oms_service.outcomes, &mut strategy);
        }

        info!(
            received_updates = updates.len(),
            processed_outcomes,
            cached_snapshots = updates
                .iter()
                .map(|update| match update {
                    qae_market_data::MarketDataUpdate::Ticker(ticker) =>
                        ticker.instrument_id.as_str(),
                    qae_market_data::MarketDataUpdate::OrderBook(order_book) => {
                        order_book.instrument_id.as_str()
                    }
                })
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            "startup websocket session completed"
        );
    } else {
        info!("public websocket startup session disabled by configuration");
    }

    if config.market_data.connect_private_stream_on_startup {
        let okx_client = OkxClient::new(config.clone());
        let login_command = okx_client
            .build_ws_login_command(&HmacSha256Signer, &SystemWallClock)
            .context("failed to build private websocket login command")?;

        let events = websocket_engine
            .connect_private_stream(
                config.okx.endpoints.private_ws.as_str(),
                &login_command,
                config.market_data.startup_stream_updates_limit as usize,
                Duration::from_millis(config.market_data.startup_stream_timeout_ms),
            )
            .await
            .context("failed to authenticate private websocket stream")?;

        for event in events {
            oms_handle
                .publish_private_event(event)
                .await
                .context("failed to publish private stream event to oms service")?;
            processed_outcomes += drain_oms_outcomes(&mut oms_service.outcomes, &mut strategy);
        }

        info!("private websocket startup authentication and subscription handling completed");
    } else {
        info!("private websocket startup session disabled by configuration");
    }

    processed_outcomes += drain_oms_outcomes(&mut oms_service.outcomes, &mut strategy);
    let oms_state = oms_handle
        .query_state()
        .await
        .context("failed to query oms service state")?;
    oms_handle
        .shutdown()
        .await
        .context("failed to stop oms service")?;
    oms_service
        .task
        .await
        .context("failed to join oms service task")?
        .context("oms service exited with error")?;

    info!(
        processed_outcomes,
        cached_snapshots = oms_state.cached_snapshots,
        open_orders = oms_state.open_orders,
        tracked_orders = oms_state.tracked_orders,
        tracked_balances = oms_state.tracked_balances,
        tracked_positions = oms_state.tracked_positions,
        tracked_fills = oms_state.tracked_fills,
        "oms service shutdown completed"
    );

    Ok(())
}

async fn dispatch_strategy_action(
    handle: &OmsServiceHandle,
    action: StrategyAction,
) -> Result<qae_core::ExecutionReport> {
    let command = match action {
        StrategyAction::Submit(order) => OmsCommand::Submit(order),
        StrategyAction::Cancel(cancel) => OmsCommand::Cancel(cancel),
    };

    handle
        .execute_command(command)
        .await
        .context("oms service command execution failed")
}

fn drain_oms_outcomes(
    outcomes: &mut mpsc::Receiver<OmsServiceOutcome>,
    strategy: &mut StrategyRuntime,
) -> usize {
    let mut drained = 0;

    loop {
        match outcomes.try_recv() {
            Ok(Ok(report)) => {
                strategy.on_execution_report(&report);
                info!(
                    order_id = %report.order_id,
                    status = ?report.status,
                    "processed oms execution report"
                );
                drained += 1;
            }
            Ok(Err(error)) => {
                warn!(error = %error, "oms command failed");
                drained += 1;
            }
            Err(mpsc::error::TryRecvError::Empty | mpsc::error::TryRecvError::Disconnected) => {
                break;
            }
        }
    }

    drained
}
