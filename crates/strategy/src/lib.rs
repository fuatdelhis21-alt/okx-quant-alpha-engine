use qae_config::AppConfig;
use qae_core::{
    CancelOrderRequest, ExecutionReport, InstrumentId, OrderRequest, OrderSide, OrderStatus,
    OrderType,
};
use qae_market_data::MarketSnapshot;
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Clone)]
pub enum StrategyAction {
    Submit(OrderRequest),
    Cancel(CancelOrderRequest),
}

#[derive(Debug, Clone)]
pub struct StartupSpreadStrategy {
    enabled: bool,
    order_quantity: f64,
    entry_spread_bps: f64,
    cancel_spread_bps: f64,
    max_signals_per_run: u32,
    signals_emitted: u32,
    instrument_states: BTreeMap<InstrumentId, InstrumentStrategyState>,
}

#[derive(Debug, Clone, Default)]
struct InstrumentStrategyState {
    pending_submit: bool,
    active_order: Option<TrackedOrder>,
}

#[derive(Debug, Clone)]
struct TrackedOrder {
    order_id: String,
    client_order_id: Option<String>,
    cancel_requested: bool,
}

impl StartupSpreadStrategy {
    pub fn new(config: &AppConfig) -> Self {
        Self {
            enabled: config.strategy.enable_startup_strategy,
            order_quantity: config.strategy.order_quantity,
            entry_spread_bps: config.strategy.entry_spread_bps,
            cancel_spread_bps: config.strategy.cancel_spread_bps,
            max_signals_per_run: config.strategy.max_signals_per_run,
            signals_emitted: 0,
            instrument_states: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn on_execution_report(&mut self, report: &ExecutionReport) {
        let state = self
            .instrument_states
            .entry(report.instrument_id.clone())
            .or_default();

        state.pending_submit = false;
        if status_is_open(report.status) {
            state.active_order = Some(TrackedOrder {
                order_id: report.order_id.clone(),
                client_order_id: report.client_order_id.clone(),
                cancel_requested: false,
            });
        } else {
            state.active_order = None;
        }

        if !state.pending_submit && state.active_order.is_none() {
            self.instrument_states.remove(&report.instrument_id);
        }
    }

    pub fn evaluate_snapshot(
        &mut self,
        snapshot: &MarketSnapshot,
    ) -> Result<Option<StrategyAction>, StrategyError> {
        if !self.enabled || self.signals_emitted >= self.max_signals_per_run {
            return Ok(None);
        }

        let instrument_id = snapshot
            .instrument_id()
            .cloned()
            .ok_or(StrategyError::MissingInstrument)?;
        let spread_bps = snapshot
            .spread_bps()
            .ok_or_else(|| StrategyError::MissingBook(instrument_id.to_string()))?;
        let mid_price = snapshot
            .mid_price()
            .ok_or_else(|| StrategyError::MissingBook(instrument_id.to_string()))?;
        let ticker = snapshot
            .ticker
            .as_ref()
            .ok_or_else(|| StrategyError::MissingTicker(instrument_id.to_string()))?;
        let book = snapshot
            .order_book
            .as_ref()
            .ok_or_else(|| StrategyError::MissingBook(instrument_id.to_string()))?;
        let state = self
            .instrument_states
            .entry(instrument_id.clone())
            .or_default();

        if let Some(active_order) = state.active_order.as_mut() {
            if spread_bps >= self.cancel_spread_bps && !active_order.cancel_requested {
                active_order.cancel_requested = true;
                self.signals_emitted += 1;
                let mut cancel = CancelOrderRequest::new(instrument_id);
                cancel.order_id = Some(active_order.order_id.clone());
                cancel.client_order_id = active_order.client_order_id.clone();
                return Ok(Some(StrategyAction::Cancel(cancel)));
            }
            return Ok(None);
        }

        if state.pending_submit || spread_bps > self.entry_spread_bps {
            return Ok(None);
        }

        let side = if ticker.last_price <= mid_price {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        };
        let limit_price = if side.is_buy() {
            book.best_bid()
                .ok_or_else(|| StrategyError::MissingBook(ticker.instrument_id.to_string()))?
                .price
        } else {
            book.best_ask()
                .ok_or_else(|| StrategyError::MissingBook(ticker.instrument_id.to_string()))?
                .price
        };
        let client_order_id = format!(
            "strategy-{:08}",
            self.signals_emitted.saturating_add(1)
        );
        let order = OrderRequest::new(instrument_id, side, OrderType::Limit, self.order_quantity)?
            .with_limit_price(limit_price)?
            .with_client_order_id(client_order_id);

        state.pending_submit = true;
        self.signals_emitted += 1;
        Ok(Some(StrategyAction::Submit(order)))
    }
}

fn status_is_open(status: OrderStatus) -> bool {
    matches!(
        status,
        OrderStatus::Submitted | OrderStatus::Accepted | OrderStatus::PartiallyFilled
    )
}

#[derive(Debug, Error, PartialEq)]
pub enum StrategyError {
    #[error("market snapshot is missing an instrument identifier")]
    MissingInstrument,
    #[error("market snapshot is missing a ticker for {0}")]
    MissingTicker(String),
    #[error("market snapshot is missing an order book for {0}")]
    MissingBook(String),
    #[error("invalid strategy order: {0}")]
    InvalidOrder(#[from] qae_core::OrderRequestError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use qae_market_data::{OrderBookLevel, OrderBookSnapshot, Ticker};
    use qae_time::ExchangeTimestamp;

    fn sample_config() -> AppConfig {
        AppConfig {
            strategy: qae_config::StrategyConfig {
                enable_startup_strategy: true,
                order_quantity: 1.5,
                entry_spread_bps: 75.0,
                cancel_spread_bps: 150.0,
                max_signals_per_run: 3,
            },
            ..AppConfig::default()
        }
    }

    fn sample_snapshot(last_price: f64, best_bid: f64, best_ask: f64) -> MarketSnapshot {
        let instrument_id = InstrumentId::new("BTC-USDT-SWAP").expect("instrument");
        MarketSnapshot {
            ticker: Some(
                Ticker::new(
                    instrument_id.clone(),
                    last_price,
                    best_bid,
                    best_ask,
                    ExchangeTimestamp::from_millis(1_700_000_000_000),
                )
                .expect("ticker"),
            ),
            order_book: Some(
                OrderBookSnapshot::new(
                    instrument_id,
                    vec![OrderBookLevel::new(best_bid, 1.0).expect("bid")],
                    vec![OrderBookLevel::new(best_ask, 1.0).expect("ask")],
                    None,
                    ExchangeTimestamp::from_millis(1_700_000_000_000),
                )
                .expect("book"),
            ),
        }
    }

    #[test]
    fn submits_limit_order_when_spread_is_tight() {
        let mut strategy = StartupSpreadStrategy::new(&sample_config());

        let action = strategy
            .evaluate_snapshot(&sample_snapshot(99.5, 99.0, 99.4))
            .expect("decision")
            .expect("action");

        match action {
            StrategyAction::Submit(order) => {
                assert_eq!(order.side, OrderSide::Sell);
                assert_eq!(order.limit_price, Some(99.4));
                assert_eq!(order.quantity, 1.5);
                assert_eq!(order.client_order_id.as_deref(), Some("strategy-00000001"));
            }
            StrategyAction::Cancel(_) => panic!("expected submit action"),
        }
    }

    #[test]
    fn cancels_open_order_when_spread_widens() {
        let mut strategy = StartupSpreadStrategy::new(&sample_config());
        strategy.on_execution_report(&ExecutionReport {
            order_id: "paper-00000001".to_string(),
            client_order_id: Some("strategy-00000001".to_string()),
            venue_order_id: None,
            instrument_id: InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
            status: OrderStatus::Accepted,
            filled_quantity: 0.0,
            remaining_quantity: 1.5,
            average_fill_price: None,
            rejection_reason: None,
        });

        let action = strategy
            .evaluate_snapshot(&sample_snapshot(100.0, 99.0, 101.0))
            .expect("decision")
            .expect("action");

        match action {
            StrategyAction::Cancel(cancel) => {
                assert_eq!(cancel.order_id.as_deref(), Some("paper-00000001"));
                assert_eq!(
                    cancel.client_order_id.as_deref(),
                    Some("strategy-00000001")
                );
            }
            StrategyAction::Submit(_) => panic!("expected cancel action"),
        }
    }

    #[test]
    fn waits_for_report_before_submitting_again() {
        let mut strategy = StartupSpreadStrategy::new(&sample_config());

        let first = strategy
            .evaluate_snapshot(&sample_snapshot(99.0, 98.9, 99.1))
            .expect("decision");
        let second = strategy
            .evaluate_snapshot(&sample_snapshot(99.0, 98.9, 99.1))
            .expect("decision");

        assert!(matches!(first, Some(StrategyAction::Submit(_))));
        assert!(second.is_none());
    }
}
