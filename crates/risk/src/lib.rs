use qae_config::AppConfig;
use qae_core::{InstrumentId, OrderRequest, OrderSide, OrderType};
use qae_market_data::MarketSnapshot;
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
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct RiskApproval {
    pub reference_price: f64,
    pub side: OrderSide,
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
}
