use qae_core::{InstrumentId, OrderSide};
use qae_time::ExchangeTimestamp;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq)]
pub struct OrderBookLevel {
    pub price: f64,
    pub quantity: f64,
}

impl OrderBookLevel {
    pub fn new(price: f64, quantity: f64) -> Result<Self, MarketDataError> {
        if !price.is_finite() || price <= 0.0 {
            return Err(MarketDataError::InvalidPrice(price));
        }

        if !quantity.is_finite() || quantity <= 0.0 {
            return Err(MarketDataError::InvalidQuantity(quantity));
        }

        Ok(Self { price, quantity })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderBookSnapshot {
    pub instrument_id: InstrumentId,
    pub bids: Vec<OrderBookLevel>,
    pub asks: Vec<OrderBookLevel>,
    pub checksum: Option<i64>,
    pub ts: ExchangeTimestamp,
}

impl OrderBookSnapshot {
    pub fn new(
        instrument_id: InstrumentId,
        bids: Vec<OrderBookLevel>,
        asks: Vec<OrderBookLevel>,
        checksum: Option<i64>,
        ts: ExchangeTimestamp,
    ) -> Result<Self, MarketDataError> {
        for levels in bids.windows(2) {
            if levels[0].price < levels[1].price {
                return Err(MarketDataError::NonMonotonicBids);
            }
        }

        for levels in asks.windows(2) {
            if levels[0].price > levels[1].price {
                return Err(MarketDataError::NonMonotonicAsks);
            }
        }

        if let (Some(best_bid), Some(best_ask)) = (bids.first(), asks.first()) {
            if best_bid.price >= best_ask.price {
                return Err(MarketDataError::CrossedBook {
                    best_bid: best_bid.price,
                    best_ask: best_ask.price,
                });
            }
        }

        Ok(Self {
            instrument_id,
            bids,
            asks,
            checksum,
            ts,
        })
    }

    #[must_use]
    pub fn best_bid(&self) -> Option<&OrderBookLevel> {
        self.bids.first()
    }

    #[must_use]
    pub fn best_ask(&self) -> Option<&OrderBookLevel> {
        self.asks.first()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Ticker {
    pub instrument_id: InstrumentId,
    pub last_price: f64,
    pub bid_price: f64,
    pub ask_price: f64,
    pub ts: ExchangeTimestamp,
}

impl Ticker {
    pub fn new(
        instrument_id: InstrumentId,
        last_price: f64,
        bid_price: f64,
        ask_price: f64,
        ts: ExchangeTimestamp,
    ) -> Result<Self, MarketDataError> {
        for price in [last_price, bid_price, ask_price] {
            if !price.is_finite() || price <= 0.0 {
                return Err(MarketDataError::InvalidPrice(price));
            }
        }

        if bid_price > ask_price {
            return Err(MarketDataError::CrossedBook {
                best_bid: bid_price,
                best_ask: ask_price,
            });
        }

        Ok(Self {
            instrument_id,
            last_price,
            bid_price,
            ask_price,
            ts,
        })
    }

    #[must_use]
    pub fn reference_price_for(&self, side: OrderSide) -> f64 {
        if side.is_buy() {
            self.ask_price
        } else {
            self.bid_price
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct MarketSnapshot {
    pub ticker: Option<Ticker>,
    pub order_book: Option<OrderBookSnapshot>,
}

impl MarketSnapshot {
    #[must_use]
    pub fn instrument_id(&self) -> Option<&InstrumentId> {
        self.order_book
            .as_ref()
            .map(|book| &book.instrument_id)
            .or_else(|| self.ticker.as_ref().map(|ticker| &ticker.instrument_id))
    }

    #[must_use]
    pub fn reference_price_for(&self, side: OrderSide) -> Option<f64> {
        self.order_book
            .as_ref()
            .and_then(|book| {
                if side.is_buy() {
                    book.best_ask().map(|level| level.price)
                } else {
                    book.best_bid().map(|level| level.price)
                }
            })
            .or_else(|| {
                self.ticker
                    .as_ref()
                    .map(|ticker| ticker.reference_price_for(side))
            })
    }

    #[must_use]
    pub fn mid_price(&self) -> Option<f64> {
        self.order_book.as_ref().and_then(|book| {
            let best_bid = book.best_bid()?.price;
            let best_ask = book.best_ask()?.price;
            Some((best_bid + best_ask) / 2.0)
        })
    }

    #[must_use]
    pub fn spread_bps(&self) -> Option<f64> {
        let book = self.order_book.as_ref()?;
        let best_bid = book.best_bid()?.price;
        let best_ask = book.best_ask()?.price;
        let mid_price = self.mid_price()?;
        if mid_price <= 0.0 {
            return None;
        }
        Some(((best_ask - best_bid) / mid_price) * 10_000.0)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum MarketDataUpdate {
    Ticker(Ticker),
    OrderBook(OrderBookSnapshot),
}

#[derive(Debug, Error, PartialEq)]
pub enum MarketDataError {
    #[error("price must be positive and finite: {0}")]
    InvalidPrice(f64),
    #[error("quantity must be positive and finite: {0}")]
    InvalidQuantity(f64),
    #[error("bids must be sorted from highest to lowest price")]
    NonMonotonicBids,
    #[error("asks must be sorted from lowest to highest price")]
    NonMonotonicAsks,
    #[error("best bid {best_bid} must remain below best ask {best_ask}")]
    CrossedBook { best_bid: f64, best_ask: f64 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_book_rejects_crossed_top_of_book() {
        let result = OrderBookSnapshot::new(
            InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
            vec![OrderBookLevel::new(101.0, 1.0).expect("bid")],
            vec![OrderBookLevel::new(100.0, 1.0).expect("ask")],
            None,
            ExchangeTimestamp::from_millis(1_700_000_000_000),
        );

        assert!(matches!(result, Err(MarketDataError::CrossedBook { .. })));
    }

    #[test]
    fn order_book_requires_sorted_bids() {
        let result = OrderBookSnapshot::new(
            InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
            vec![
                OrderBookLevel::new(100.0, 1.0).expect("bid"),
                OrderBookLevel::new(101.0, 1.0).expect("bid"),
            ],
            vec![OrderBookLevel::new(102.0, 1.0).expect("ask")],
            None,
            ExchangeTimestamp::from_millis(1_700_000_000_000),
        );

        assert_eq!(result.err(), Some(MarketDataError::NonMonotonicBids));
    }

    #[test]
    fn ticker_uses_side_aware_reference_price() {
        let ticker = Ticker::new(
            InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
            100.0,
            99.5,
            100.5,
            ExchangeTimestamp::from_millis(1_700_000_000_000),
        )
        .expect("ticker");

        assert_eq!(ticker.reference_price_for(OrderSide::Buy), 100.5);
        assert_eq!(ticker.reference_price_for(OrderSide::Sell), 99.5);
    }

    #[test]
    fn snapshot_reports_mid_price_and_spread() {
        let snapshot = MarketSnapshot {
            ticker: None,
            order_book: Some(
                OrderBookSnapshot::new(
                    InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
                    vec![OrderBookLevel::new(99.0, 1.0).expect("bid")],
                    vec![OrderBookLevel::new(101.0, 1.0).expect("ask")],
                    None,
                    ExchangeTimestamp::from_millis(1_700_000_000_000),
                )
                .expect("book"),
            ),
        };

        assert_eq!(
            snapshot.instrument_id().map(InstrumentId::as_str),
            Some("BTC-USDT-SWAP")
        );
        assert_eq!(snapshot.mid_price(), Some(100.0));
        assert_eq!(snapshot.spread_bps(), Some(200.0));
    }
}
