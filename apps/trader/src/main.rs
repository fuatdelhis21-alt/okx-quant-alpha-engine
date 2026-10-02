use anyhow::{Context, Result};
use qae_config::AppConfig;
use qae_core::{CancelOrderRequest, ExecutionReport, OrderRequest};
use qae_execution::{
    OmsCommand, OmsService, OmsServiceHandle, OmsServiceOutcome, OmsStateSnapshot,
};
use qae_monitoring::{MonitoringConfig, init_tracing};
use qae_okx_client::{HmacSha256Signer, OkxClient};
use qae_risk::RiskManager;
use qae_strategy::{StrategyAction, StrategyRuntime};
use qae_time::SystemWallClock;
use qae_websocket_engine::WebsocketEngine;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time;
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
    processed_outcomes += run_startup_control_server(&config, &oms_handle, &mut strategy)
        .await
        .context("startup control server failed")?;
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
) -> Result<ExecutionReport> {
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

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum StartupControlRequest {
    QueryState,
    SubmitOrder { order: OrderRequest },
    CancelOrder { cancel: CancelOrderRequest },
    Shutdown,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum StartupControlResponse {
    State { state: OmsStateSnapshot },
    Report { report: ExecutionReport },
    ShutdownAccepted,
    Error { message: String },
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct ControlLoopOutcome {
    processed_requests: usize,
    processed_reports: usize,
    shutdown_requested: bool,
}

async fn run_startup_control_server(
    config: &AppConfig,
    handle: &OmsServiceHandle,
    strategy: &mut StrategyRuntime,
) -> Result<usize> {
    if !config.control.enable_startup_control_server {
        info!("startup control server disabled by configuration");
        return Ok(0);
    }

    let listener = TcpListener::bind(config.control.startup_control_bind_address)
        .await
        .with_context(|| {
            format!(
                "failed to bind startup control server on {}",
                config.control.startup_control_bind_address
            )
        })?;
    let timeout = Duration::from_millis(config.control.startup_control_timeout_ms);
    let persistent = config.control.startup_control_run_until_shutdown;
    let max_requests = config.control.startup_control_max_requests as usize;
    let mut processed_requests = 0usize;
    let mut processed_outcomes = 0usize;

    info!(
        bind_address = %config.control.startup_control_bind_address,
        max_requests = config.control.startup_control_max_requests,
        timeout_ms = config.control.startup_control_timeout_ms,
        persistent,
        "startup control server listening"
    );

    loop {
        if !persistent && processed_requests >= max_requests {
            break;
        }

        let (stream, remote_addr) =
            match next_startup_control_connection(&listener, timeout, persistent)
                .await
                .context("failed to accept startup control client")?
            {
                Some(connection) => connection,
                None => {
                    info!(
                        processed_requests,
                        "startup control server timed out waiting for more requests"
                    );
                    break;
                }
            };

        let remaining_requests = (!persistent).then_some(max_requests - processed_requests);
        let outcome = handle_startup_control_connection(
            stream,
            handle,
            strategy,
            timeout,
            remaining_requests,
        )
        .await
        .with_context(|| format!("failed to process startup control client {remote_addr}"))?;
        processed_requests += outcome.processed_requests;
        processed_outcomes += outcome.processed_reports;

        if outcome.shutdown_requested {
            info!(
                processed_requests,
                "startup control server received shutdown request"
            );
            break;
        }
    }

    Ok(processed_outcomes)
}

async fn next_startup_control_connection(
    listener: &TcpListener,
    timeout: Duration,
    persistent: bool,
) -> Result<Option<(TcpStream, std::net::SocketAddr)>> {
    if persistent {
        listener
            .accept()
            .await
            .map(Some)
            .context("failed to accept persistent startup control client")
    } else {
        match time::timeout(timeout, listener.accept()).await {
            Ok(Ok(connection)) => Ok(Some(connection)),
            Ok(Err(error)) => Err(error).context("failed to accept startup control client"),
            Err(_) => Ok(None),
        }
    }
}

async fn handle_startup_control_connection<T>(
    stream: T,
    handle: &OmsServiceHandle,
    strategy: &mut StrategyRuntime,
    timeout: Duration,
    max_requests_per_connection: Option<usize>,
) -> Result<ControlLoopOutcome>
where
    T: AsyncRead + AsyncWrite + Unpin,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = BufReader::new(reader);
    let mut outcome = ControlLoopOutcome::default();

    loop {
        if let Some(max_requests) = max_requests_per_connection {
            if outcome.processed_requests >= max_requests {
                break;
            }
        }

        let mut payload = String::new();
        let bytes_read = time::timeout(timeout, reader.read_line(&mut payload))
            .await
            .context("startup control request timed out while reading")?
            .context("failed to read startup control request")?;

        if bytes_read == 0 {
            break;
        }

        let (response, processed_reports, shutdown_requested) =
            match serde_json::from_str::<StartupControlRequest>(&payload) {
                Ok(request) => {
                    let (response, processed_reports, shutdown_requested) =
                        process_startup_control_request(handle, strategy, request).await?;
                    (response, processed_reports, shutdown_requested)
                }
                Err(error) => (
                    StartupControlResponse::Error {
                        message: format!("invalid startup control request: {error}"),
                    },
                    0,
                    false,
                ),
            };

        write_startup_control_response(&mut writer, &response, timeout).await?;
        outcome.processed_requests += 1;
        outcome.processed_reports += processed_reports;
        outcome.shutdown_requested = shutdown_requested;

        if shutdown_requested {
            break;
        }
    }

    Ok(outcome)
}

async fn write_startup_control_response<W>(
    writer: &mut W,
    response: &StartupControlResponse,
    timeout: Duration,
) -> Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut body = serde_json::to_vec(response).context("failed to serialize control response")?;
    body.push(b'\n');
    time::timeout(timeout, writer.write_all(&body))
        .await
        .context("startup control response timed out while writing")?
        .context("failed to write startup control response")?;
    Ok(())
}

async fn process_startup_control_request(
    handle: &OmsServiceHandle,
    strategy: &mut StrategyRuntime,
    request: StartupControlRequest,
) -> Result<(StartupControlResponse, usize, bool)> {
    match request {
        StartupControlRequest::QueryState => {
            let state = handle
                .query_state()
                .await
                .context("failed to query oms state from startup control server")?;
            Ok((StartupControlResponse::State { state }, 0, false))
        }
        StartupControlRequest::SubmitOrder { order } => {
            let report = handle
                .execute_command(OmsCommand::Submit(order))
                .await
                .context("failed to execute startup control submit command")?;
            strategy.on_execution_report(&report);
            Ok((StartupControlResponse::Report { report }, 1, false))
        }
        StartupControlRequest::CancelOrder { cancel } => {
            let report = handle
                .execute_command(OmsCommand::Cancel(cancel))
                .await
                .context("failed to execute startup control cancel command")?;
            strategy.on_execution_report(&report);
            Ok((StartupControlResponse::Report { report }, 1, false))
        }
        StartupControlRequest::Shutdown => Ok((StartupControlResponse::ShutdownAccepted, 0, true)),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        StartupControlRequest, StartupControlResponse, handle_startup_control_connection,
        process_startup_control_request,
    };
    use qae_config::AppConfig;
    use qae_core::{InstrumentId, OrderSide, OrderType, RuntimeMode};
    use qae_execution::OmsService;
    use qae_market_data::{MarketDataUpdate, Ticker};
    use qae_strategy::StrategyRuntime;
    use qae_time::ExchangeTimestamp;
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    fn test_order_request() -> qae_core::OrderRequest {
        qae_core::OrderRequest {
            instrument_id: InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
            client_order_id: Some("startup-control-order".to_owned()),
            side: OrderSide::Buy,
            order_type: OrderType::Limit,
            quantity: 1.0,
            limit_price: Some(60_000.0),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn startup_control_query_state_returns_snapshot() {
        let config = AppConfig::default();
        let service = OmsService::spawn(config.clone()).expect("service should start");
        let mut strategy = StrategyRuntime::new(&config);

        let (response, processed_reports, shutdown_requested) = process_startup_control_request(
            &service.handle,
            &mut strategy,
            StartupControlRequest::QueryState,
        )
        .await
        .expect("query should succeed");

        assert_eq!(processed_reports, 0);
        assert!(!shutdown_requested);
        match response {
            StartupControlResponse::State { state } => {
                assert_eq!(state.cached_snapshots, 0);
                assert_eq!(state.open_orders, 0);
            }
            other => panic!("unexpected response: {other:?}"),
        }

        service
            .handle
            .shutdown()
            .await
            .expect("shutdown should succeed");
        service
            .task
            .await
            .expect("task should join")
            .expect("service should exit cleanly");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn startup_control_submit_order_returns_direct_report() {
        let config = AppConfig {
            mode: RuntimeMode::Paper,
            ..AppConfig::default()
        };
        let mut service = OmsService::spawn(config.clone()).expect("service should start");
        let mut strategy = StrategyRuntime::new(&config);
        let instrument_id = InstrumentId::new("BTC-USDT-SWAP").expect("instrument");

        service
            .handle
            .publish_market_update(MarketDataUpdate::Ticker(
                Ticker::new(
                    instrument_id,
                    60_000.0,
                    59_999.0,
                    60_001.0,
                    ExchangeTimestamp::from_millis(1_700_000_000_000),
                )
                .expect("ticker"),
            ))
            .await
            .expect("market data should publish");

        let (response, processed_reports, shutdown_requested) = process_startup_control_request(
            &service.handle,
            &mut strategy,
            StartupControlRequest::SubmitOrder {
                order: test_order_request(),
            },
        )
        .await
        .expect("submit should succeed");

        assert_eq!(processed_reports, 1);
        assert!(!shutdown_requested);
        match response {
            StartupControlResponse::Report { report } => {
                assert_eq!(
                    report.client_order_id.as_deref(),
                    Some("startup-control-order")
                );
            }
            other => panic!("unexpected response: {other:?}"),
        }

        match service.outcomes.try_recv() {
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
            Ok(Ok(_)) | Ok(Err(_)) => {
                panic!("direct control execution should not use shared outcomes queue")
            }
            Err(other) => panic!("unexpected channel state: {other:?}"),
        }

        service
            .handle
            .shutdown()
            .await
            .expect("shutdown should succeed");
        service
            .task
            .await
            .expect("task should join")
            .expect("service should exit cleanly");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn startup_control_shutdown_request_is_acknowledged() {
        let config = AppConfig::default();
        let service = OmsService::spawn(config.clone()).expect("service should start");
        let mut strategy = StrategyRuntime::new(&config);

        let (response, processed_reports, shutdown_requested) = process_startup_control_request(
            &service.handle,
            &mut strategy,
            StartupControlRequest::Shutdown,
        )
        .await
        .expect("shutdown request should succeed");

        assert_eq!(processed_reports, 0);
        assert!(shutdown_requested);
        assert!(matches!(response, StartupControlResponse::ShutdownAccepted));

        service
            .handle
            .shutdown()
            .await
            .expect("shutdown should succeed");
        service
            .task
            .await
            .expect("task should join")
            .expect("service should exit cleanly");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn startup_control_connection_handles_multiple_requests() {
        let config = AppConfig::default();
        let service = OmsService::spawn(config.clone()).expect("service should start");
        let mut strategy = StrategyRuntime::new(&config);
        let (client, server) = tokio::io::duplex(1024);
        let handle = service.handle.clone();

        let server_task = tokio::spawn(async move {
            handle_startup_control_connection(
                server,
                &handle,
                &mut strategy,
                Duration::from_millis(1_000),
                Some(8),
            )
            .await
        });

        let (client_reader, mut client_writer) = tokio::io::split(client);
        let mut client_reader = BufReader::new(client_reader);
        let query = serde_json::to_string(&StartupControlRequest::QueryState).expect("query json");
        let shutdown =
            serde_json::to_string(&StartupControlRequest::Shutdown).expect("shutdown json");

        client_writer
            .write_all(format!("{query}\n{shutdown}\n").as_bytes())
            .await
            .expect("requests should write");

        let mut query_response = String::new();
        client_reader
            .read_line(&mut query_response)
            .await
            .expect("query response should read");
        let parsed_query: StartupControlResponse =
            serde_json::from_str(&query_response).expect("query response should parse");
        assert!(matches!(parsed_query, StartupControlResponse::State { .. }));

        let mut shutdown_response = String::new();
        client_reader
            .read_line(&mut shutdown_response)
            .await
            .expect("shutdown response should read");
        let parsed_shutdown: StartupControlResponse =
            serde_json::from_str(&shutdown_response).expect("shutdown response should parse");
        assert!(matches!(
            parsed_shutdown,
            StartupControlResponse::ShutdownAccepted
        ));

        let outcome = server_task
            .await
            .expect("server task should join")
            .expect("connection handling should succeed");
        assert_eq!(outcome.processed_requests, 2);
        assert_eq!(outcome.processed_reports, 0);
        assert!(outcome.shutdown_requested);

        service
            .handle
            .shutdown()
            .await
            .expect("shutdown should succeed");
        service
            .task
            .await
            .expect("task should join")
            .expect("service should exit cleanly");
    }
}
