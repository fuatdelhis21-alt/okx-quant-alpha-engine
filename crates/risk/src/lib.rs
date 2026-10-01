use qae_config::AppConfig;
use qae_core::{
    AccountBalance, ExecutionReport, FillUpdate, InstrumentId, OrderRequest, OrderSide,
    OrderStatus, OrderType, PositionUpdate,
};
use qae_market_data::MarketSnapshot;
use std::collections::BTreeMap;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct RiskManager {
    allowed_instruments: Vec<InstrumentId>,
    max_order_quantity: f64,
    max_order_notional: f64,
    max_price_deviation_bps: f64,
}

impl RiskManager {
    pub fn new(config: &AppConfig) -> Result<Self, RiskError> {
        let allowed_instruments = config
            .market_data
            .instruments
            .iter()
            .cloned()
            .map(|instrument| {
                InstrumentId::new(instrument).map_err(|_| RiskError::InvalidInstrument)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            allowed_instruments,
            max_order_quantity: config.risk.max_order_quantity,
            max_order_notional: config.risk.max_order_notional,
            max_price_deviation_bps: config.risk.max_price_deviation_bps,
        })
    }

    pub fn evaluate(
        &self,
        order: &OrderRequest,
        snapshot: Option<&MarketSnapshot>,
    ) -> Result<RiskApproval, RiskViolation> {
        order
            .validate()
            .map_err(|error| RiskViolation::InvalidOrder(error.to_string()))?;

        if !self
            .allowed_instruments
            .iter()
            .any(|instrument| instrument == &order.instrument_id)
        {
            return Err(RiskViolation::InstrumentNotAllowed(
                order.instrument_id.to_string(),
            ));
        }

        if order.quantity > self.max_order_quantity {
            return Err(RiskViolation::OrderQuantityTooLarge {
                quantity: order.quantity,
                max: self.max_order_quantity,
            });
        }

        let snapshot = snapshot.ok_or(RiskViolation::MissingMarketData)?;
        let reference_price = snapshot
            .reference_price_for(order.side)
            .ok_or(RiskViolation::MissingMarketData)?;

        let notional_price = match order.order_type {
            OrderType::Market => reference_price,
            OrderType::Limit => order.limit_price.unwrap_or(reference_price),
        };
        let notional = order.estimated_notional(notional_price);

        if notional > self.max_order_notional {
            return Err(RiskViolation::OrderNotionalTooLarge {
                notional,
                max: self.max_order_notional,
            });
        }

        if let Some(limit_price) = order.limit_price {
            let deviation_bps =
                ((limit_price - reference_price).abs() / reference_price) * 10_000.0;
            if deviation_bps > self.max_price_deviation_bps {
                return Err(RiskViolation::PriceBandExceeded {
                    limit_price,
                    reference_price,
                    max_deviation_bps: self.max_price_deviation_bps,
                });
            }
        }

        Ok(RiskApproval {
            reference_price,
            side: order.side,
            notional,
        })
    }
}

#[derive(Debug, Clone)]
pub struct PortfolioRiskManager {
    order_risk: RiskManager,
    max_position_notional: f64,
    max_total_exposure_notional: f64,
    max_daily_loss: f64,
    max_open_orders: u32,
}

impl PortfolioRiskManager {
    pub fn new(config: &AppConfig) -> Result<Self, RiskError> {
        Ok(Self {
            order_risk: RiskManager::new(config)?,
            max_position_notional: config.risk.max_position_notional,
            max_total_exposure_notional: config.risk.max_total_exposure_notional,
            max_daily_loss: config.risk.max_daily_loss,
            max_open_orders: config.risk.max_open_orders,
        })
    }

    pub fn evaluate(
        &self,
        order: &OrderRequest,
        snapshot: Option<&MarketSnapshot>,
        state: &PortfolioRiskState,
    ) -> Result<RiskApproval, RiskViolation> {
        let approval = self.order_risk.evaluate(order, snapshot)?;
        let current_position = state.position_notional(&order.instrument_id);
        let projected_position = match order.side {
            OrderSide::Buy => current_position + approval.notional,
            OrderSide::Sell => (current_position - approval.notional).max(0.0),
        };
        if projected_position > self.max_position_notional {
            return Err(RiskViolation::PositionNotionalTooLarge {
                instrument_id: order.instrument_id.to_string(),
                projected: projected_position,
                max: self.max_position_notional,
            });
        }

        let projected_total =
            (state.total_exposure_notional - current_position).max(0.0) + projected_position;
        if projected_total > self.max_total_exposure_notional {
            return Err(RiskViolation::TotalExposureTooLarge {
                projected: projected_total,
                max: self.max_total_exposure_notional,
            });
        }

        if state.daily_realized_pnl <= -self.max_daily_loss {
            return Err(RiskViolation::DailyLossLimitExceeded {
                realized_pnl: state.daily_realized_pnl,
                max_loss: self.max_daily_loss,
            });
        }

        if state.open_orders >= self.max_open_orders {
            return Err(RiskViolation::OpenOrderLimitExceeded {
                open_orders: state.open_orders,
                max: self.max_open_orders,
            });
        }

        Ok(approval)
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PortfolioRiskState {
    pub position_notionals: BTreeMap<InstrumentId, f64>,
    pub position_profiles: BTreeMap<InstrumentId, PositionRiskProfile>,
    pub balances: BTreeMap<String, AccountBalance>,
    pub total_exposure_notional: f64,
    pub daily_realized_pnl: f64,
    pub open_orders: u32,
    pub last_known_equity: Option<f64>,
    pub last_known_available_equity: Option<f64>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct PositionRiskProfile {
    pub net_quantity: f64,
    pub average_entry_price: Option<f64>,
    pub realized_pnl: f64,
}

impl PortfolioRiskState {
    #[must_use]
    pub fn position_notional(&self, instrument_id: &InstrumentId) -> f64 {
        self.position_notionals
            .get(instrument_id)
            .copied()
            .unwrap_or_default()
    }

    pub fn set_position_notional(&mut self, instrument_id: InstrumentId, notional: f64) {
        if notional <= 0.0 {
            self.position_notionals.remove(&instrument_id);
        } else {
            self.position_notionals.insert(instrument_id, notional);
        }
        self.recompute_total_exposure();
    }

    pub fn set_daily_realized_pnl(&mut self, pnl: f64) {
        self.daily_realized_pnl = pnl;
    }

    #[must_use]
    pub fn position_profile(&self, instrument_id: &InstrumentId) -> Option<&PositionRiskProfile> {
        self.position_profiles.get(instrument_id)
    }

    pub fn apply_account_balance(&mut self, balance: &AccountBalance) {
        self.last_known_equity = Some(balance.equity);
        self.last_known_available_equity = Some(balance.available_equity);
        self.balances.insert(balance.currency.clone(), balance.clone());
    }

    pub fn apply_execution_report(&mut self, report: &ExecutionReport) {
        match report.status {
            OrderStatus::Submitted | OrderStatus::Accepted | OrderStatus::PartiallyFilled => {
                self.open_orders = self.open_orders.saturating_add(1);
            }
            OrderStatus::Filled | OrderStatus::Cancelled | OrderStatus::Rejected => {
                self.open_orders = self.open_orders.saturating_sub(1);
            }
        }
    }

    pub fn apply_position_update(&mut self, update: &PositionUpdate) {
        let signed_quantity = if update.side == OrderSide::Buy {
            update.quantity.abs()
        } else {
            -update.quantity.abs()
        };
        let realized_pnl = self
            .position_profiles
            .get(&update.instrument_id)
            .map_or(0.0, |profile| profile.realized_pnl);
        self.upsert_position_profile(
            update.instrument_id.clone(),
            PositionRiskProfile {
                net_quantity: signed_quantity,
                average_entry_price: update.average_entry_price,
                realized_pnl,
            },
        );
        let notional = update.average_entry_price.unwrap_or_default() * update.quantity.abs();
        self.set_position_notional(update.instrument_id.clone(), notional);
    }

    pub fn apply_fill_update(&mut self, fill: &FillUpdate) {
        let mut profile = self
            .position_profiles
            .get(&fill.instrument_id)
            .cloned()
            .unwrap_or_default();
        let signed_fill_quantity = if fill.side == OrderSide::Buy {
            fill.fill_quantity.abs()
        } else {
            -fill.fill_quantity.abs()
        };

        if profile.net_quantity.abs() < f64::EPSILON
            || profile.net_quantity.signum() == signed_fill_quantity.signum()
        {
            let current_abs_quantity = profile.net_quantity.abs();
            let next_abs_quantity = current_abs_quantity + fill.fill_quantity.abs();
            let average_entry = profile.average_entry_price.unwrap_or(fill.fill_price);
            let next_average_entry = if next_abs_quantity > 0.0 {
                ((average_entry * current_abs_quantity)
                    + (fill.fill_price * fill.fill_quantity.abs()))
                    / next_abs_quantity
            } else {
                fill.fill_price
            };

            profile.net_quantity += signed_fill_quantity;
            profile.average_entry_price = Some(next_average_entry);
        } else {
            let current_abs_quantity = profile.net_quantity.abs();
            let closing_quantity = current_abs_quantity.min(fill.fill_quantity.abs());
            let entry_price = profile.average_entry_price.unwrap_or(fill.fill_price);
            let realized_delta = if profile.net_quantity > 0.0 {
                (fill.fill_price - entry_price) * closing_quantity
            } else {
                (entry_price - fill.fill_price) * closing_quantity
            };

            profile.realized_pnl += realized_delta;
            self.daily_realized_pnl += realized_delta;
            profile.net_quantity += signed_fill_quantity;

            if profile.net_quantity.abs() < f64::EPSILON {
                profile.net_quantity = 0.0;
                profile.average_entry_price = None;
            } else if profile.net_quantity.signum() == signed_fill_quantity.signum() {
                profile.average_entry_price = Some(fill.fill_price);
            }
        }

        self.upsert_position_profile(fill.instrument_id.clone(), profile);
    }

    fn recompute_total_exposure(&mut self) {
        self.total_exposure_notional = self.position_notionals.values().sum();
    }

    fn upsert_position_profile(
        &mut self,
        instrument_id: InstrumentId,
        profile: PositionRiskProfile,
    ) {
        let notional = profile.net_quantity.abs() * profile.average_entry_price.unwrap_or_default();
        if profile.net_quantity.abs() < f64::EPSILON {
            self.position_profiles.remove(&instrument_id);
        } else {
            self.position_profiles.insert(instrument_id.clone(), profile);
        }
        self.set_position_notional(instrument_id, notional);
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RiskApproval {
    pub reference_price: f64,
    pub side: OrderSide,
    pub notional: f64,
}

#[derive(Debug, Error, PartialEq)]
pub enum RiskViolation {
    #[error("instrument is not allowed: {0}")]
    InstrumentNotAllowed(String),
    #[error("order quantity {quantity} exceeds maximum {max}")]
    OrderQuantityTooLarge { quantity: f64, max: f64 },
    #[error("order notional {notional} exceeds maximum {max}")]
    OrderNotionalTooLarge { notional: f64, max: f64 },
    #[error(
        "limit price {limit_price} deviates from reference price {reference_price} beyond {max_deviation_bps} bps"
    )]
    PriceBandExceeded {
        limit_price: f64,
        reference_price: f64,
        max_deviation_bps: f64,
    },
    #[error("projected position notional for {instrument_id} {projected} exceeds maximum {max}")]
    PositionNotionalTooLarge {
        instrument_id: String,
        projected: f64,
        max: f64,
    },
    #[error("projected total exposure {projected} exceeds maximum {max}")]
    TotalExposureTooLarge { projected: f64, max: f64 },
    #[error("daily realized pnl {realized_pnl} breaches max loss {max_loss}")]
    DailyLossLimitExceeded { realized_pnl: f64, max_loss: f64 },
    #[error("open orders {open_orders} exceed maximum {max}")]
    OpenOrderLimitExceeded { open_orders: u32, max: u32 },
    #[error("market data is required before evaluating risk")]
    MissingMarketData,
    #[error("invalid order: {0}")]
    InvalidOrder(String),
}

#[derive(Debug, Error)]
pub enum RiskError {
    #[error("invalid instrument configured for risk manager")]
    InvalidInstrument,
}

#[cfg(test)]
mod tests {
    use super::*;
    use qae_config::AppConfig;
    use qae_core::{OrderRequest, OrderSide, OrderType};
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
                    vec![OrderBookLevel::new(99.5, 2.0).expect("bid")],
                    vec![OrderBookLevel::new(100.5, 2.0).expect("ask")],
                    None,
                    ExchangeTimestamp::from_millis(1_700_000_000_000),
                )
                .expect("book"),
            ),
        }
    }

    #[test]
    fn rejects_orders_for_unknown_instruments() {
        let config = AppConfig::default();
        let manager = RiskManager::new(&config).expect("manager");
        let order = OrderRequest::new(
            InstrumentId::new("ETH-USDT-SWAP").expect("instrument"),
            OrderSide::Buy,
            OrderType::Market,
            1.0,
        )
        .expect("order");

        assert!(matches!(
            manager.evaluate(&order, Some(&sample_snapshot())),
            Err(RiskViolation::InstrumentNotAllowed(_))
        ));
    }

    #[test]
    fn rejects_orders_that_exceed_notional_limit() {
        let mut config = AppConfig::default();
        config.risk.max_order_notional = 10.0;
        let manager = RiskManager::new(&config).expect("manager");
        let order = OrderRequest::new(
            InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
            OrderSide::Buy,
            OrderType::Market,
            1.0,
        )
        .expect("order");

        assert!(matches!(
            manager.evaluate(&order, Some(&sample_snapshot())),
            Err(RiskViolation::OrderNotionalTooLarge { .. })
        ));
    }

    #[test]
    fn approves_reasonable_limit_orders() {
        let config = AppConfig::default();
        let manager = RiskManager::new(&config).expect("manager");
        let order = OrderRequest::new(
            InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
            OrderSide::Buy,
            OrderType::Limit,
            1.0,
        )
        .and_then(|order| order.with_limit_price(100.7))
        .expect("order");

        let approval = manager
            .evaluate(&order, Some(&sample_snapshot()))
            .expect("approval");

        assert_eq!(approval.reference_price, 100.5);
    }

    #[test]
    fn portfolio_manager_rejects_excess_open_orders() {
        let config = AppConfig::default();
        let manager = PortfolioRiskManager::new(&config).expect("manager");
        let order = OrderRequest::new(
            InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
            OrderSide::Buy,
            OrderType::Market,
            1.0,
        )
        .expect("order");
        let state = PortfolioRiskState {
            open_orders: config.risk.max_open_orders,
            ..PortfolioRiskState::default()
        };

        assert!(matches!(
            manager.evaluate(&order, Some(&sample_snapshot()), &state),
            Err(RiskViolation::OpenOrderLimitExceeded { .. })
        ));
    }

    #[test]
    fn portfolio_state_tracks_position_updates() {
        let mut state = PortfolioRiskState::default();
        state.apply_position_update(&PositionUpdate {
            instrument_id: InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
            side: OrderSide::Buy,
            quantity: 2.0,
            average_entry_price: Some(100.0),
            unrealized_pnl: Some(5.0),
        });

        assert_eq!(state.total_exposure_notional, 200.0);
    }

    #[test]
    fn portfolio_state_tracks_realized_pnl_from_fills() {
        let instrument_id = InstrumentId::new("BTC-USDT-SWAP").expect("instrument");
        let mut state = PortfolioRiskState::default();

        state.apply_fill_update(&FillUpdate {
            order_id: "buy-1".to_string(),
            client_order_id: Some("cid-buy-1".to_string()),
            instrument_id: instrument_id.clone(),
            side: OrderSide::Buy,
            fill_price: 100.0,
            fill_quantity: 2.0,
        });
        state.apply_fill_update(&FillUpdate {
            order_id: "sell-1".to_string(),
            client_order_id: Some("cid-sell-1".to_string()),
            instrument_id: instrument_id.clone(),
            side: OrderSide::Sell,
            fill_price: 110.0,
            fill_quantity: 1.0,
        });

        assert_eq!(state.daily_realized_pnl, 10.0);
        assert_eq!(state.total_exposure_notional, 100.0);
        assert_eq!(
            state
                .position_profile(&instrument_id)
                .map(|profile| profile.net_quantity),
            Some(1.0)
        );
    }

    #[test]
    fn portfolio_state_tracks_account_balances() {
        let mut state = PortfolioRiskState::default();
        state.apply_account_balance(&AccountBalance {
            currency: "USDT".to_string(),
            equity: 1_000.0,
            available_equity: 900.0,
        });

        assert_eq!(state.last_known_equity, Some(1_000.0));
        assert_eq!(state.last_known_available_equity, Some(900.0));
        assert!(state.balances.contains_key("USDT"));
    }
}
