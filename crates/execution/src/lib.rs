use qae_config::AppConfig;
use qae_core::{ExecutionReport, OrderRequest, OrderSide, OrderStatus, OrderType, RuntimeMode};
use qae_market_data::MarketSnapshot;
use qae_risk::{RiskManager, RiskViolation};
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug)]
pub struct PaperExecutionEngine {
    mode: RuntimeMode,
    slippage_bps: f64,
    next_order_id: u64,
    open_orders: BTreeMap<String, OrderRequest>,
    risk_manager: RiskManager,
}

impl PaperExecutionEngine {
    pub fn new(config: &AppConfig) -> Result<Self, ExecutionError> {
        Ok(Self {
            mode: config.mode,
            slippage_bps: config.execution.paper_fill_slippage_bps,
            next_order_id: 1,
            open_orders: BTreeMap::new(),
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
            OrderType::Market => Ok(self.filled_report(
                order,
                order_id,
                apply_slippage(approval.reference_price, approval.side, self.slippage_bps),
            )),
            OrderType::Limit => self.submit_limit(order, order_id, snapshot),
        }
    }

    #[must_use]
    pub fn open_order_count(&self) -> usize {
        self.open_orders.len()
    }

    pub fn cancel(&mut self, order_id: &str) -> Option<ExecutionReport> {
        self.open_orders.remove(order_id).map(|order| {
            let remaining_quantity = order.quantity;
            let instrument_id = order.instrument_id;

            ExecutionReport {
                order_id: order_id.to_string(),
                instrument_id,
                status: OrderStatus::Cancelled,
                filled_quantity: 0.0,
                remaining_quantity,
                average_fill_price: None,
                rejection_reason: None,
            }
        })
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
            OrderSide::Buy => limit_price >= reference_price,
            OrderSide::Sell => limit_price <= reference_price,
        };

        if is_crossing {
            Ok(self.filled_report(order, order_id, reference_price))
        } else {
            self.open_orders.insert(order_id.clone(), order.clone());
            let remaining_quantity = order.quantity;
            let instrument_id = order.instrument_id;
            Ok(ExecutionReport {
                order_id,
                instrument_id,
                status: OrderStatus::Accepted,
                filled_quantity: 0.0,
                remaining_quantity,
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
        let filled_quantity = order.quantity;
        let instrument_id = order.instrument_id;

        ExecutionReport {
            order_id,
            instrument_id,
            status: OrderStatus::Filled,
            filled_quantity,
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

fn apply_slippage(reference_price: f64, side: OrderSide, slippage_bps: f64) -> f64 {
    let multiplier = slippage_bps / 10_000.0;
    if side == OrderSide::Buy {
        reference_price * (1.0 + multiplier)
    } else {
        reference_price * (1.0 - multiplier)
    }
}

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("live execution remains unsupported")]
    LiveExecutionUnsupported,
    #[error("market data is unavailable for execution")]
    MarketDataUnavailable,
    #[error("order rejected by risk checks: {0}")]
    RiskRejected(#[from] RiskViolation),
    #[error("failed to build risk manager: {0}")]
    RiskConfig(#[source] qae_risk::RiskError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use qae_core::{InstrumentId, OrderRequest, OrderType};
    use qae_market_data::{MarketSnapshot, OrderBookLevel, OrderBookSnapshot, Ticker};
    use qae_time::ExchangeTimestamp;

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
        assert_eq!(engine.open_order_count(), 1);
    }

    #[test]
    fn live_mode_stays_disabled() {
        let mut config = AppConfig {
            mode: RuntimeMode::Live,
            live_trading: true,
            ..AppConfig::default()
        };
        config.okx.credentials.api_key = Some("k".to_string());
        config.okx.credentials.api_secret = Some("s".to_string());
        config.okx.credentials.passphrase = Some("p".to_string());

        let engine = PaperExecutionEngine::new(&config).expect("engine");
        let mut engine = engine;
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
}
