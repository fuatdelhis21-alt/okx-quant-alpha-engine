use qae_config::AppConfig;
use qae_core::{
    AccountBalance, CancelOrderRequest, ExecutionReport, FillUpdate, OrderRequest, OrderStatus,
    PositionUpdate, PrivateStreamEvent, RuntimeMode,
};
use qae_market_data::MarketSnapshot;
use qae_okx_client::{OkxClient, OkxClientError, RequestSigner, ReqwestLiveOrderTransport};
use qae_risk::{PortfolioRiskManager, PortfolioRiskState, RiskManager, RiskViolation};
use qae_time::WallClock;
use std::collections::BTreeMap;
use thiserror::Error;

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
            return Err(ExecutionError::OrderNotCancellable(existing_report.order_id));
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

    fn prepare_order(&mut self, order: OrderRequest) -> OrderRequest {
        if self.mode != RuntimeMode::Live || order.client_order_id.is_some() {
            return order;
        }

        let client_order_id = format!("live-{:08}", self.next_live_client_order_id);
        self.next_live_client_order_id += 1;
        order.with_client_order_id(client_order_id)
    }
}

#[derive(Debug, Clone, Default)]
pub struct ReconciliationState {
    pub reports: BTreeMap<String, ExecutionReport>,
    pub reports_by_client_order_id: BTreeMap<String, String>,
    pub balances: BTreeMap<String, AccountBalance>,
    pub positions: BTreeMap<String, PositionUpdate>,
    pub fills: Vec<FillUpdate>,
    pub portfolio_risk: PortfolioRiskState,
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
                    .and_then(|report_key| self.reports.get_mut(&report_key))
                    .map(|report| {
                        report.filled_quantity += fill.fill_quantity;
                        report.remaining_quantity =
                            (report.remaining_quantity - fill.fill_quantity).max(0.0);
                        report.average_fill_price = Some(fill.fill_price);
                        report.status = if report.remaining_quantity > 0.0 {
                            OrderStatus::PartiallyFilled
                        } else {
                            OrderStatus::Filled
                        };
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
        self.reports.insert(report.order_id.clone(), report);
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
                    self.reports_by_client_order_id.get(client_order_id).cloned()
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, TimeZone, Utc};
    use qae_core::{CancelOrderRequest, InstrumentId, OrderRequest, OrderSide, OrderType};
    use qae_market_data::{MarketSnapshot, OrderBookLevel, OrderBookSnapshot, Ticker};
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
                CancelOrderRequest::new(
                    InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
                )
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
}
