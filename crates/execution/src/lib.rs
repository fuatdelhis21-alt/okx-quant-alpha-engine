use qae_config::AppConfig;
use qae_core::{
    AccountBalance, CancelOrderRequest, ExecutionReport, FillUpdate, InstrumentId, OrderRequest,
    OrderStatus, PositionUpdate, PrivateStreamEvent, RuntimeMode,
};
use qae_market_data::{MarketDataUpdate, MarketSnapshot};
use qae_okx_client::{
    HmacSha256Signer, OkxClient, OkxClientError, RequestSigner, ReqwestLiveOrderTransport,
};
use qae_risk::{PortfolioRiskManager, PortfolioRiskState, RiskManager, RiskViolation};
use qae_time::{SystemWallClock, WallClock};
use std::collections::{BTreeMap, VecDeque};
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

#[derive(Debug)]
pub struct PaperExecutionEngine {
    mode: RuntimeMode,
    slippage_bps: f64,
    next_order_id: u64,
    risk_manager: RiskManager,
}

#[derive(Debug)]
pub struct ExecutionGateway {
    mode: RuntimeMode,
    paper_engine: PaperExecutionEngine,
    okx_client: OkxClient,
    transport: Option<ReqwestLiveOrderTransport>,
    portfolio_risk: PortfolioRiskManager,
    reconciliation: ReconciliationState,
    next_live_client_order_id: u64,
}

#[derive(Debug, Clone)]
pub enum OmsCommand {
    Submit(OrderRequest),
    Cancel(CancelOrderRequest),
}

#[derive(Debug)]
pub struct OmsRuntime {
    gateway: ExecutionGateway,
    snapshots: BTreeMap<InstrumentId, MarketSnapshot>,
    pending_commands: VecDeque<OmsCommand>,
}

pub type OmsServiceOutcome = Result<ExecutionReport, ExecutionError>;

#[derive(Debug, Clone)]
enum OmsServiceMessage {
    Command(OmsCommand),
    MarketData(MarketDataUpdate),
    PrivateEvent(PrivateStreamEvent),
    Shutdown,
}

#[derive(Debug, Clone)]
pub struct OmsServiceHandle {
    message_tx: mpsc::Sender<OmsServiceMessage>,
}

#[derive(Debug)]
pub struct OmsService {
    pub handle: OmsServiceHandle,
    pub outcomes: mpsc::Receiver<OmsServiceOutcome>,
    pub task: JoinHandle<Result<(), ExecutionError>>,
}

impl OmsService {
    pub fn spawn(config: AppConfig) -> Result<Self, ExecutionError> {
        let mut runtime = OmsRuntime::new(&config)?;
        let (message_tx, mut message_rx) = mpsc::channel(256);
        let (outcome_tx, outcome_rx) = mpsc::channel(256);
        let task = tokio::spawn(async move {
            let signer = HmacSha256Signer;
            let clock = SystemWallClock;

            while let Some(message) = message_rx.recv().await {
                match message {
                    OmsServiceMessage::Command(command) => {
                        runtime.enqueue(command);
                        for outcome in runtime.drain(&signer, &clock) {
                            if outcome_tx.send(outcome).await.is_err() {
                                return Ok(());
                            }
                        }
                    }
                    OmsServiceMessage::MarketData(update) => runtime.apply_market_update(update),
                    OmsServiceMessage::PrivateEvent(event) => {
                        if let Some(report) = runtime.apply_private_event(event)
                            && outcome_tx.send(Ok(report)).await.is_err()
                        {
                            return Ok(());
                        }
                    }
                    OmsServiceMessage::Shutdown => break,
                }
            }

            Ok(())
        });

        Ok(Self {
            handle: OmsServiceHandle { message_tx },
            outcomes: outcome_rx,
            task,
        })
    }
}

impl OmsServiceHandle {
    pub async fn submit_command(&self, command: OmsCommand) -> Result<(), OmsServiceSendError> {
        self.message_tx
            .send(OmsServiceMessage::Command(command))
            .await
            .map_err(|_| OmsServiceSendError::Closed)
    }

    pub async fn publish_market_update(
        &self,
        update: MarketDataUpdate,
    ) -> Result<(), OmsServiceSendError> {
        self.message_tx
            .send(OmsServiceMessage::MarketData(update))
            .await
            .map_err(|_| OmsServiceSendError::Closed)
    }

    pub async fn publish_private_event(
        &self,
        event: PrivateStreamEvent,
    ) -> Result<(), OmsServiceSendError> {
        self.message_tx
            .send(OmsServiceMessage::PrivateEvent(event))
            .await
            .map_err(|_| OmsServiceSendError::Closed)
    }

    pub async fn shutdown(&self) -> Result<(), OmsServiceSendError> {
        self.message_tx
            .send(OmsServiceMessage::Shutdown)
            .await
            .map_err(|_| OmsServiceSendError::Closed)
    }
}

impl ExecutionGateway {
    pub fn new(config: &AppConfig) -> Result<Self, ExecutionError> {
        Ok(Self {
            mode: config.mode,
            paper_engine: PaperExecutionEngine::new(config)?,
            okx_client: OkxClient::new(config.clone()),
            transport: None,
            portfolio_risk: PortfolioRiskManager::new(config)
                .map_err(ExecutionError::PortfolioRiskConfig)?,
            reconciliation: ReconciliationState::default(),
            next_live_client_order_id: 1,
        })
    }

    pub fn submit<C: WallClock>(
        &mut self,
        signer: &impl RequestSigner,
        order: OrderRequest,
        snapshot: Option<&MarketSnapshot>,
        clock: &C,
    ) -> Result<ExecutionReport, ExecutionError> {
        let order = self.prepare_order(order);
        self.portfolio_risk
            .evaluate(&order, snapshot, &self.reconciliation.portfolio_risk)
            .map_err(ExecutionError::PortfolioRiskRejected)?;

        let report = match self.mode {
            RuntimeMode::Paper | RuntimeMode::Simulation => {
                self.paper_engine.submit(order.clone(), snapshot)?
            }
            RuntimeMode::Live => {
                if self.transport.is_none() {
                    self.transport =
                        Some(ReqwestLiveOrderTransport::new().map_err(ExecutionError::Okx)?);
                }
                let transport = self.transport.as_ref().expect("transport initialized");
                self.okx_client
                    .submit_live_order(transport, signer, &order, clock)
                    .map_err(ExecutionError::Okx)?
            }
        };

        self.reconciliation.record_submission(order, report.clone());
        Ok(report)
    }

    pub fn apply_private_event(&mut self, event: PrivateStreamEvent) -> Option<ExecutionReport> {
        self.reconciliation.apply_private_event(event)
    }

    pub fn cancel<C: WallClock>(
        &mut self,
        signer: &impl RequestSigner,
        cancel: CancelOrderRequest,
        clock: &C,
    ) -> Result<ExecutionReport, ExecutionError> {
        let existing_report = self
            .reconciliation
            .find_report(&cancel)
            .cloned()
            .ok_or_else(|| ExecutionError::OrderNotFound(cancel.instrument_id.to_string()))?;

        if !status_is_open(existing_report.status) {
            return Err(ExecutionError::OrderNotCancellable(
                existing_report.order_id,
            ));
        }

        let report = match self.mode {
            RuntimeMode::Paper | RuntimeMode::Simulation => self
                .reconciliation
                .cancel_open_order(cancel, existing_report)?,
            RuntimeMode::Live => {
                if self.transport.is_none() {
                    self.transport =
                        Some(ReqwestLiveOrderTransport::new().map_err(ExecutionError::Okx)?);
                }
                let transport = self.transport.as_ref().expect("transport initialized");
                let report = self
                    .okx_client
                    .cancel_live_order(transport, signer, &cancel, Some(&existing_report), clock)
                    .map_err(ExecutionError::Okx)?;
                self.reconciliation.upsert_report(report.clone());
                report
            }
        };

        Ok(report)
    }

    #[must_use]
    pub fn reconciliation_state(&self) -> &ReconciliationState {
        &self.reconciliation
    }

    #[must_use]
    pub fn current_report(&self, order_id: &str) -> Option<&ExecutionReport> {
        self.reconciliation.current_report(order_id)
    }

    #[must_use]
    pub fn order_history(&self, order_id: &str) -> Option<&[ExecutionReport]> {
        self.reconciliation.history_for_order(order_id)
    }

    #[must_use]
    pub fn open_orders(&self) -> Vec<&ExecutionReport> {
        self.reconciliation.open_orders()
    }

    fn prepare_order(&mut self, order: OrderRequest) -> OrderRequest {
        if self.mode != RuntimeMode::Live || order.client_order_id.is_some() {
            return order;
        }

        let client_order_id = format!("live-{:08}", self.next_live_client_order_id);
        self.next_live_client_order_id += 1;
        order.with_client_order_id(client_order_id)
    }
}

impl OmsRuntime {
    pub fn new(config: &AppConfig) -> Result<Self, ExecutionError> {
        Ok(Self {
            gateway: ExecutionGateway::new(config)?,
            snapshots: BTreeMap::new(),
            pending_commands: VecDeque::new(),
        })
    }

    pub fn apply_market_update(&mut self, update: MarketDataUpdate) {
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

    pub fn apply_private_event(&mut self, event: PrivateStreamEvent) -> Option<ExecutionReport> {
        self.gateway.apply_private_event(event)
    }

    pub fn enqueue(&mut self, command: OmsCommand) {
        self.pending_commands.push_back(command);
    }

    #[must_use]
    pub fn pending_command_count(&self) -> usize {
        self.pending_commands.len()
    }

    pub fn process_next<C: WallClock>(
        &mut self,
        signer: &impl RequestSigner,
        clock: &C,
    ) -> Result<Option<ExecutionReport>, ExecutionError> {
        let Some(command) = self.pending_commands.pop_front() else {
            return Ok(None);
        };

        match command {
            OmsCommand::Submit(order) => {
                let snapshot = self.snapshots.get(&order.instrument_id);
                self.gateway
                    .submit(signer, order, snapshot, clock)
                    .map(Some)
            }
            OmsCommand::Cancel(cancel) => self.gateway.cancel(signer, cancel, clock).map(Some),
        }
    }

    pub fn drain<C: WallClock>(
        &mut self,
        signer: &impl RequestSigner,
        clock: &C,
    ) -> Vec<Result<ExecutionReport, ExecutionError>> {
        let mut outcomes = Vec::new();
        while self.pending_commands.front().is_some() {
            match self.process_next(signer, clock) {
                Ok(Some(report)) => outcomes.push(Ok(report)),
                Ok(None) => break,
                Err(error) => outcomes.push(Err(error)),
            }
        }
        outcomes
    }

    #[must_use]
    pub fn gateway(&self) -> &ExecutionGateway {
        &self.gateway
    }
}

#[derive(Debug, Clone, Default)]
pub struct ReconciliationState {
    pub reports: BTreeMap<String, ExecutionReport>,
    pub order_records: BTreeMap<String, OrderReconciliationRecord>,
    pub reports_by_client_order_id: BTreeMap<String, String>,
    pub balances: BTreeMap<String, AccountBalance>,
    pub positions: BTreeMap<String, PositionUpdate>,
    pub fills: Vec<FillUpdate>,
    pub portfolio_risk: PortfolioRiskState,
}

#[derive(Debug, Clone)]
pub struct OrderReconciliationRecord {
    pub current: ExecutionReport,
    pub history: Vec<ExecutionReport>,
}

impl ReconciliationState {
    pub fn record_submission(&mut self, order: OrderRequest, report: ExecutionReport) {
        if let Some(client_order_id) = report
            .client_order_id
            .clone()
            .or(order.client_order_id.clone())
        {
            self.reports_by_client_order_id
                .insert(client_order_id, report.order_id.clone());
        }
        self.upsert_report(report);
    }

    pub fn apply_private_event(&mut self, event: PrivateStreamEvent) -> Option<ExecutionReport> {
        match event {
            PrivateStreamEvent::Account(balance) => {
                self.portfolio_risk.apply_account_balance(&balance);
                self.balances.insert(balance.currency.clone(), balance);
                None
            }
            PrivateStreamEvent::Position(position) => {
                self.portfolio_risk.apply_position_update(&position);
                self.positions
                    .insert(position.instrument_id.to_string(), position);
                None
            }
            PrivateStreamEvent::Fill(fill) => {
                self.portfolio_risk.apply_fill_update(&fill);
                self.fills.push(fill.clone());
                let report_key = if self.reports.contains_key(&fill.order_id) {
                    Some(fill.order_id.clone())
                } else {
                    fill.client_order_id.as_ref().and_then(|client_order_id| {
                        self.reports_by_client_order_id
                            .get(client_order_id)
                            .cloned()
                    })
                };

                report_key
                    .and_then(|report_key| self.reports.get(&report_key).cloned())
                    .map(|mut report| {
                        report.filled_quantity += fill.fill_quantity;
                        report.remaining_quantity =
                            (report.remaining_quantity - fill.fill_quantity).max(0.0);
                        report.average_fill_price = Some(fill.fill_price);
                        report.status = if report.remaining_quantity > 0.0 {
                            OrderStatus::PartiallyFilled
                        } else {
                            OrderStatus::Filled
                        };
                        self.upsert_report(report.clone());
                        report.clone()
                    })
            }
            PrivateStreamEvent::Order(report) => {
                self.upsert_report(report.clone());
                Some(report)
            }
        }
    }

    pub fn find_report(&self, cancel: &CancelOrderRequest) -> Option<&ExecutionReport> {
        self.resolve_report_key(cancel)
            .and_then(|report_key| self.reports.get(&report_key))
    }

    #[must_use]
    pub fn current_report(&self, order_id: &str) -> Option<&ExecutionReport> {
        self.order_records
            .get(order_id)
            .map(|record| &record.current)
    }

    #[must_use]
    pub fn history_for_order(&self, order_id: &str) -> Option<&[ExecutionReport]> {
        self.order_records
            .get(order_id)
            .map(|record| record.history.as_slice())
    }

    #[must_use]
    pub fn open_orders(&self) -> Vec<&ExecutionReport> {
        self.order_records
            .values()
            .filter(|record| status_is_open(record.current.status))
            .map(|record| &record.current)
            .collect()
    }

    fn upsert_report(&mut self, report: ExecutionReport) {
        let previous_open = self
            .reports
            .get(&report.order_id)
            .map(|existing| status_is_open(existing.status))
            .unwrap_or(false);
        let next_open = status_is_open(report.status);

        if !previous_open && next_open {
            self.portfolio_risk.open_orders = self.portfolio_risk.open_orders.saturating_add(1);
        } else if previous_open && !next_open {
            self.portfolio_risk.open_orders = self.portfolio_risk.open_orders.saturating_sub(1);
        }

        if let Some(client_order_id) = report.client_order_id.clone() {
            self.reports_by_client_order_id
                .insert(client_order_id, report.order_id.clone());
        }
        let report_clone = report.clone();
        self.reports.insert(report.order_id.clone(), report);
        self.order_records
            .entry(report_clone.order_id.clone())
            .and_modify(|record| {
                record.current = report_clone.clone();
                record.history.push(report_clone.clone());
            })
            .or_insert_with(|| OrderReconciliationRecord {
                current: report_clone.clone(),
                history: vec![report_clone],
            });
    }

    fn cancel_open_order(
        &mut self,
        cancel: CancelOrderRequest,
        existing_report: ExecutionReport,
    ) -> Result<ExecutionReport, ExecutionError> {
        self.resolve_report_key(&cancel)
            .ok_or_else(|| ExecutionError::OrderNotFound(cancel.instrument_id.to_string()))?;
        let mut updated = existing_report;
        updated.status = OrderStatus::Cancelled;
        self.upsert_report(updated.clone());
        Ok(updated)
    }

    fn resolve_report_key(&self, cancel: &CancelOrderRequest) -> Option<String> {
        cancel
            .order_id
            .as_ref()
            .and_then(|order_id| {
                if self.reports.contains_key(order_id) {
                    Some(order_id.clone())
                } else {
                    None
                }
            })
            .or_else(|| {
                cancel.client_order_id.as_ref().and_then(|client_order_id| {
                    self.reports_by_client_order_id
                        .get(client_order_id)
                        .cloned()
                })
            })
    }
}

fn status_is_open(status: OrderStatus) -> bool {
    matches!(
        status,
        OrderStatus::Submitted | OrderStatus::Accepted | OrderStatus::PartiallyFilled
    )
}

impl PaperExecutionEngine {
    pub fn new(config: &AppConfig) -> Result<Self, ExecutionError> {
        Ok(Self {
            mode: config.mode,
            slippage_bps: config.execution.paper_fill_slippage_bps,
            next_order_id: 1,
            risk_manager: RiskManager::new(config).map_err(ExecutionError::RiskConfig)?,
        })
    }

    pub fn submit(
        &mut self,
        order: OrderRequest,
        snapshot: Option<&MarketSnapshot>,
    ) -> Result<ExecutionReport, ExecutionError> {
        if self.mode == RuntimeMode::Live {
            return Err(ExecutionError::LiveExecutionUnsupported);
        }

        let approval = self
            .risk_manager
            .evaluate(&order, snapshot)
            .map_err(ExecutionError::RiskRejected)?;
        let order_id = self.allocate_order_id();

        match order.order_type {
            qae_core::OrderType::Market => Ok(self.filled_report(
                order,
                order_id,
                apply_slippage(approval.reference_price, approval.side, self.slippage_bps),
            )),
            qae_core::OrderType::Limit => self.submit_limit(order, order_id, snapshot),
        }
    }

    fn submit_limit(
        &mut self,
        order: OrderRequest,
        order_id: String,
        snapshot: Option<&MarketSnapshot>,
    ) -> Result<ExecutionReport, ExecutionError> {
        let snapshot = snapshot.ok_or(ExecutionError::MarketDataUnavailable)?;
        let reference_price = snapshot
            .reference_price_for(order.side)
            .ok_or(ExecutionError::MarketDataUnavailable)?;
        let limit_price = order
            .limit_price
            .ok_or(ExecutionError::MarketDataUnavailable)?;

        let is_crossing = match order.side {
            qae_core::OrderSide::Buy => limit_price >= reference_price,
            qae_core::OrderSide::Sell => limit_price <= reference_price,
        };

        if is_crossing {
            let fill_price = match order.side {
                qae_core::OrderSide::Buy => reference_price.min(limit_price),
                qae_core::OrderSide::Sell => reference_price.max(limit_price),
            };

            Ok(self.filled_report(order, order_id, fill_price))
        } else {
            Ok(ExecutionReport {
                order_id,
                client_order_id: order.client_order_id.clone(),
                venue_order_id: None,
                instrument_id: order.instrument_id,
                status: OrderStatus::Accepted,
                filled_quantity: 0.0,
                remaining_quantity: order.quantity,
                average_fill_price: None,
                rejection_reason: None,
            })
        }
    }

    fn filled_report(
        &self,
        order: OrderRequest,
        order_id: String,
        fill_price: f64,
    ) -> ExecutionReport {
        ExecutionReport {
            order_id,
            client_order_id: order.client_order_id,
            venue_order_id: None,
            instrument_id: order.instrument_id,
            status: OrderStatus::Filled,
            filled_quantity: order.quantity,
            remaining_quantity: 0.0,
            average_fill_price: Some(fill_price),
            rejection_reason: None,
        }
    }

    fn allocate_order_id(&mut self) -> String {
        let order_id = format!("paper-{:08}", self.next_order_id);
        self.next_order_id += 1;
        order_id
    }
}

fn apply_slippage(reference_price: f64, side: qae_core::OrderSide, slippage_bps: f64) -> f64 {
    let multiplier = slippage_bps / 10_000.0;
    if side == qae_core::OrderSide::Buy {
        reference_price * (1.0 + multiplier)
    } else {
        (reference_price * (1.0 - multiplier)).max(0.0)
    }
}

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("live execution remains unsupported")]
    LiveExecutionUnsupported,
    #[error("market data is unavailable for execution")]
    MarketDataUnavailable,
    #[error("order rejected by per-order risk checks: {0}")]
    RiskRejected(#[from] RiskViolation),
    #[error("failed to build per-order risk manager: {0}")]
    RiskConfig(#[source] qae_risk::RiskError),
    #[error("order rejected by portfolio risk checks: {0}")]
    PortfolioRiskRejected(#[source] RiskViolation),
    #[error("failed to build portfolio risk manager: {0}")]
    PortfolioRiskConfig(#[source] qae_risk::RiskError),
    #[error("failed to interact with OKX client: {0}")]
    Okx(#[from] OkxClientError),
    #[error("order was not found for cancellation: {0}")]
    OrderNotFound(String),
    #[error("order is not cancellable: {0}")]
    OrderNotCancellable(String),
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum OmsServiceSendError {
    #[error("oms service is not running")]
    Closed,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, TimeZone, Utc};
    use qae_core::{CancelOrderRequest, InstrumentId, OrderRequest, OrderSide, OrderType};
    use qae_market_data::{
        MarketDataUpdate, MarketSnapshot, OrderBookLevel, OrderBookSnapshot, Ticker,
    };
    use qae_okx_client::HmacSha256Signer;
    use qae_time::ExchangeTimestamp;

    struct FixedClock(DateTime<Utc>);

    impl WallClock for FixedClock {
        fn now_utc(&self) -> DateTime<Utc> {
            self.0
        }
    }

    fn sample_snapshot() -> MarketSnapshot {
        MarketSnapshot {
            ticker: Some(
                Ticker::new(
                    InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
                    100.0,
                    99.5,
                    100.5,
                    ExchangeTimestamp::from_millis(1_700_000_000_000),
                )
                .expect("ticker"),
            ),
            order_book: Some(
                OrderBookSnapshot::new(
                    InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
                    vec![OrderBookLevel::new(99.5, 1.0).expect("bid")],
                    vec![OrderBookLevel::new(100.5, 1.0).expect("ask")],
                    None,
                    ExchangeTimestamp::from_millis(1_700_000_000_000),
                )
                .expect("book"),
            ),
        }
    }

    #[test]
    fn market_orders_fill_immediately_with_slippage() {
        let config = AppConfig::default();
        let mut engine = PaperExecutionEngine::new(&config).expect("engine");
        let order = OrderRequest::new(
            InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
            OrderSide::Buy,
            OrderType::Market,
            1.0,
        )
        .expect("order");

        let report = engine
            .submit(order, Some(&sample_snapshot()))
            .expect("report");

        assert_eq!(report.status, OrderStatus::Filled);
        assert_eq!(report.average_fill_price, Some(100.5201));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn oms_service_processes_market_data_and_commands() {
        let service = OmsService::spawn(AppConfig::default()).expect("service");
        let handle = service.handle.clone();
        let mut outcomes = service.outcomes;
        let task = service.task;
        let instrument_id = InstrumentId::new("BTC-USDT-SWAP").expect("instrument");

        handle
            .publish_market_update(MarketDataUpdate::Ticker(
                Ticker::new(
                    instrument_id.clone(),
                    100.0,
                    99.5,
                    100.5,
                    ExchangeTimestamp::from_millis(1_700_000_000_000),
                )
                .expect("ticker"),
            ))
            .await
            .expect("ticker sent");
        handle
            .publish_market_update(MarketDataUpdate::OrderBook(
                OrderBookSnapshot::new(
                    instrument_id.clone(),
                    vec![OrderBookLevel::new(99.5, 1.0).expect("bid")],
                    vec![OrderBookLevel::new(100.5, 1.0).expect("ask")],
                    None,
                    ExchangeTimestamp::from_millis(1_700_000_000_000),
                )
                .expect("book"),
            ))
            .await
            .expect("book sent");
        handle
            .submit_command(OmsCommand::Submit(
                OrderRequest::new(instrument_id, OrderSide::Buy, OrderType::Market, 1.0)
                    .expect("order"),
            ))
            .await
            .expect("command sent");

        let report = outcomes
            .recv()
            .await
            .expect("outcome")
            .expect("execution report");
        assert_eq!(report.status, OrderStatus::Filled);

        handle.shutdown().await.expect("shutdown sent");
        task.await.expect("task join").expect("task result");
    }

    #[test]
    fn resting_limit_orders_are_kept_open() {
        let config = AppConfig::default();
        let mut engine = PaperExecutionEngine::new(&config).expect("engine");
        let order = OrderRequest::new(
            InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
            OrderSide::Buy,
            OrderType::Limit,
            1.0,
        )
        .and_then(|order| order.with_limit_price(99.0))
        .expect("order");

        let report = engine
            .submit(order, Some(&sample_snapshot()))
            .expect("report");

        assert_eq!(report.status, OrderStatus::Accepted);
    }

    #[test]
    fn marketable_limit_orders_respect_limit_price() {
        let config = AppConfig::default();
        let mut engine = PaperExecutionEngine::new(&config).expect("engine");
        let order = OrderRequest::new(
            InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
            OrderSide::Sell,
            OrderType::Limit,
            1.0,
        )
        .and_then(|order| order.with_limit_price(99.0))
        .expect("order");

        let report = engine
            .submit(order, Some(&sample_snapshot()))
            .expect("report");

        assert_eq!(report.status, OrderStatus::Filled);
        assert_eq!(report.average_fill_price, Some(99.5));
    }

    #[test]
    fn live_mode_stays_disabled_for_paper_engine() {
        let mut config = AppConfig {
            mode: RuntimeMode::Live,
            live_trading: true,
            ..AppConfig::default()
        };
        config.okx.credentials.api_key = Some("k".to_string());
        config.okx.credentials.api_secret = Some("s".to_string());
        config.okx.credentials.passphrase = Some("p".to_string());

        let mut engine = PaperExecutionEngine::new(&config).expect("engine");
        let order = OrderRequest::new(
            InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
            OrderSide::Buy,
            OrderType::Market,
            1.0,
        )
        .expect("order");

        assert!(matches!(
            engine.submit(order, Some(&sample_snapshot())),
            Err(ExecutionError::LiveExecutionUnsupported)
        ));
    }

    #[test]
    fn execution_gateway_assigns_client_order_ids_for_live_orders() {
        let mut config = AppConfig {
            mode: RuntimeMode::Live,
            live_trading: true,
            ..AppConfig::default()
        };
        config.okx.credentials.api_key = Some("k".to_string());
        config.okx.credentials.api_secret = Some("s".to_string());
        config.okx.credentials.passphrase = Some("p".to_string());

        let mut gateway = ExecutionGateway::new(&config).expect("gateway");
        let order = OrderRequest::new(
            InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
            OrderSide::Sell,
            OrderType::Limit,
            1.0,
        )
        .and_then(|order| order.with_limit_price(101.0))
        .expect("order");

        let result = gateway.submit(
            &HmacSha256Signer,
            order,
            Some(&sample_snapshot()),
            &FixedClock(
                Utc.timestamp_millis_opt(1_700_000_000_000)
                    .single()
                    .expect("ts"),
            ),
        );

        assert!(result.is_err());
        // transport is real and not exercised in unit tests, but id allocation occurs before I/O.
        assert_eq!(gateway.next_live_client_order_id, 2);
    }

    #[test]
    fn reconciliation_updates_from_private_events() {
        let mut reconciliation = ReconciliationState::default();
        let report = ExecutionReport {
            order_id: "ord-1".to_string(),
            client_order_id: Some("cid-1".to_string()),
            venue_order_id: Some("venue-1".to_string()),
            instrument_id: InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
            status: OrderStatus::Accepted,
            filled_quantity: 0.0,
            remaining_quantity: 2.0,
            average_fill_price: None,
            rejection_reason: None,
        };
        let order = OrderRequest::new(
            InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
            OrderSide::Buy,
            OrderType::Limit,
            2.0,
        )
        .and_then(|order| order.with_limit_price(100.0))
        .expect("order")
        .with_client_order_id("cid-1");
        reconciliation.record_submission(order, report);

        let updated = reconciliation
            .apply_private_event(PrivateStreamEvent::Fill(FillUpdate {
                order_id: "ord-1".to_string(),
                client_order_id: Some("cid-1".to_string()),
                instrument_id: InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
                side: OrderSide::Buy,
                fill_price: 100.0,
                fill_quantity: 2.0,
            }))
            .is_some();

        assert!(updated);
        assert_eq!(reconciliation.fills.len(), 1);
        assert_eq!(reconciliation.reports["ord-1"].status, OrderStatus::Filled);
    }

    #[test]
    fn gateway_cancels_open_paper_orders() {
        let config = AppConfig::default();
        let mut gateway = ExecutionGateway::new(&config).expect("gateway");
        let order = OrderRequest::new(
            InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
            OrderSide::Buy,
            OrderType::Limit,
            1.0,
        )
        .and_then(|order| order.with_limit_price(99.0))
        .expect("order")
        .with_client_order_id("cid-cancel");

        let report = gateway
            .submit(
                &HmacSha256Signer,
                order,
                Some(&sample_snapshot()),
                &FixedClock(
                    Utc.timestamp_millis_opt(1_700_000_000_000)
                        .single()
                        .expect("ts"),
                ),
            )
            .expect("submit");
        assert_eq!(report.status, OrderStatus::Accepted);

        let cancelled = gateway
            .cancel(
                &HmacSha256Signer,
                CancelOrderRequest::new(InstrumentId::new("BTC-USDT-SWAP").expect("instrument"))
                    .with_client_order_id("cid-cancel"),
                &FixedClock(
                    Utc.timestamp_millis_opt(1_700_000_000_001)
                        .single()
                        .expect("ts"),
                ),
            )
            .expect("cancel");

        assert_eq!(cancelled.status, OrderStatus::Cancelled);
        assert_eq!(gateway.reconciliation_state().portfolio_risk.open_orders, 0);
    }

    #[test]
    fn gateway_tracks_order_history_and_open_orders() {
        let config = AppConfig::default();
        let mut gateway = ExecutionGateway::new(&config).expect("gateway");
        let order = OrderRequest::new(
            InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
            OrderSide::Buy,
            OrderType::Limit,
            2.0,
        )
        .and_then(|order| order.with_limit_price(100.0))
        .expect("order")
        .with_client_order_id("cid-history");

        let submitted = gateway
            .submit(
                &HmacSha256Signer,
                order,
                Some(&sample_snapshot()),
                &FixedClock(
                    Utc.timestamp_millis_opt(1_700_000_000_000)
                        .single()
                        .expect("ts"),
                ),
            )
            .expect("submit");

        assert_eq!(gateway.open_orders().len(), 1);
        assert_eq!(
            gateway
                .current_report(&submitted.order_id)
                .map(|report| report.status),
            Some(OrderStatus::Accepted)
        );

        let updated = gateway
            .apply_private_event(PrivateStreamEvent::Fill(FillUpdate {
                order_id: submitted.order_id.clone(),
                client_order_id: Some("cid-history".to_string()),
                instrument_id: InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
                side: OrderSide::Buy,
                fill_price: 100.0,
                fill_quantity: 1.0,
            }))
            .expect("update");

        assert_eq!(updated.status, OrderStatus::PartiallyFilled);
        assert_eq!(
            gateway
                .order_history(&submitted.order_id)
                .map(|history| history.len()),
            Some(2)
        );
        assert_eq!(gateway.open_orders().len(), 1);
    }

    #[test]
    fn oms_runtime_processes_queued_commands_in_order() {
        let config = AppConfig::default();
        let mut runtime = OmsRuntime::new(&config).expect("runtime");
        let snapshot = sample_snapshot();

        runtime.apply_market_update(MarketDataUpdate::Ticker(
            snapshot.ticker.clone().expect("ticker"),
        ));
        runtime.apply_market_update(MarketDataUpdate::OrderBook(
            snapshot.order_book.clone().expect("book"),
        ));

        let submit = OrderRequest::new(
            InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
            OrderSide::Buy,
            OrderType::Limit,
            1.0,
        )
        .and_then(|order| order.with_limit_price(99.0))
        .expect("order")
        .with_client_order_id("cid-oms");
        runtime.enqueue(OmsCommand::Submit(submit));
        runtime.enqueue(OmsCommand::Cancel(
            CancelOrderRequest::new(InstrumentId::new("BTC-USDT-SWAP").expect("instrument"))
                .with_client_order_id("cid-oms"),
        ));

        let first = runtime
            .process_next(
                &HmacSha256Signer,
                &FixedClock(
                    Utc.timestamp_millis_opt(1_700_000_000_000)
                        .single()
                        .expect("ts"),
                ),
            )
            .expect("first result")
            .expect("first report");
        let second = runtime
            .process_next(
                &HmacSha256Signer,
                &FixedClock(
                    Utc.timestamp_millis_opt(1_700_000_000_001)
                        .single()
                        .expect("ts"),
                ),
            )
            .expect("second result")
            .expect("second report");

        assert_eq!(first.status, OrderStatus::Accepted);
        assert_eq!(second.status, OrderStatus::Cancelled);
        assert_eq!(runtime.pending_command_count(), 0);
    }

    #[test]
    fn reconciliation_tracks_account_balances_in_portfolio_risk_state() {
        let mut reconciliation = ReconciliationState::default();
        reconciliation.apply_private_event(PrivateStreamEvent::Account(AccountBalance {
            currency: "USDT".to_string(),
            equity: 1_000.0,
            available_equity: 950.0,
        }));

        assert_eq!(
            reconciliation.portfolio_risk.last_known_equity,
            Some(1_000.0)
        );
        assert_eq!(
            reconciliation.portfolio_risk.last_known_available_equity,
            Some(950.0)
        );
    }
}
