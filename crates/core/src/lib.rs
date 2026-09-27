use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeMode {
    #[default]
    Paper,
    Simulation,
    Live,
}

impl RuntimeMode {
    #[must_use]
    pub fn is_live(self) -> bool {
        matches!(self, Self::Live)
    }
}

impl Display for RuntimeMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Paper => write!(f, "paper"),
            Self::Simulation => write!(f, "simulation"),
            Self::Live => write!(f, "live"),
        }
    }
}

impl FromStr for RuntimeMode {
    type Err = ParseRuntimeModeError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "paper" => Ok(Self::Paper),
            "simulation" | "sim" => Ok(Self::Simulation),
            "live" => Ok(Self::Live),
            _ => Err(ParseRuntimeModeError(value.to_string())),
        }
    }
}

#[derive(Debug, Error)]
#[error("unsupported runtime mode: {0}")]
pub struct ParseRuntimeModeError(String);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RequestPriority {
    Low,
    #[default]
    Normal,
    High,
    Critical,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InstrumentId(String);

impl InstrumentId {
    pub fn new(raw: impl Into<String>) -> Result<Self, InstrumentIdError> {
        let value = raw.into();
        let normalized = value.trim();
        if normalized.is_empty() {
            return Err(InstrumentIdError::Empty);
        }

        Ok(Self(normalized.to_owned()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for InstrumentId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderSide {
    Buy,
    Sell,
}

impl OrderSide {
    #[must_use]
    pub fn is_buy(self) -> bool {
        matches!(self, Self::Buy)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderType {
    Market,
    Limit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub instrument_id: InstrumentId,
    pub side: OrderSide,
    pub order_type: OrderType,
    pub quantity: f64,
    pub limit_price: Option<f64>,
    pub client_order_id: Option<String>,
}

impl OrderRequest {
    pub fn new(
        instrument_id: InstrumentId,
        side: OrderSide,
        order_type: OrderType,
        quantity: f64,
    ) -> Result<Self, OrderRequestError> {
        if !quantity.is_finite() || quantity <= 0.0 {
            return Err(OrderRequestError::InvalidQuantity(quantity));
        }

        Ok(Self {
            instrument_id,
            side,
            order_type,
            quantity,
            limit_price: None,
            client_order_id: None,
        })
    }

    pub fn with_limit_price(mut self, limit_price: f64) -> Result<Self, OrderRequestError> {
        if !matches!(self.order_type, OrderType::Limit) {
            return Err(OrderRequestError::UnexpectedLimitPrice);
        }

        if !limit_price.is_finite() || limit_price <= 0.0 {
            return Err(OrderRequestError::InvalidLimitPrice(limit_price));
        }

        self.limit_price = Some(limit_price);
        Ok(self)
    }

    #[must_use]
    pub fn with_client_order_id(mut self, client_order_id: impl Into<String>) -> Self {
        self.client_order_id = Some(client_order_id.into());
        self
    }

    pub fn validate(&self) -> Result<(), OrderRequestError> {
        if !self.quantity.is_finite() || self.quantity <= 0.0 {
            return Err(OrderRequestError::InvalidQuantity(self.quantity));
        }

        match (self.order_type, self.limit_price) {
            (OrderType::Market, Some(_)) => Err(OrderRequestError::UnexpectedLimitPrice),
            (OrderType::Limit, None) => Err(OrderRequestError::MissingLimitPrice),
            (OrderType::Limit, Some(limit_price))
                if !limit_price.is_finite() || limit_price <= 0.0 =>
            {
                Err(OrderRequestError::InvalidLimitPrice(limit_price))
            }
            _ => Ok(()),
        }
    }

    #[must_use]
    pub fn estimated_notional(&self, reference_price: f64) -> f64 {
        reference_price * self.quantity
    }
}

#[derive(Debug, Error, PartialEq)]
pub enum OrderRequestError {
    #[error("order quantity must be positive and finite: {0}")]
    InvalidQuantity(f64),
    #[error("limit orders require a limit price")]
    MissingLimitPrice,
    #[error("market orders cannot specify a limit price")]
    UnexpectedLimitPrice,
    #[error("limit price must be positive and finite: {0}")]
    InvalidLimitPrice(f64),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    Accepted,
    Filled,
    Rejected,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionReport {
    pub order_id: String,
    pub instrument_id: InstrumentId,
    pub status: OrderStatus,
    pub filled_quantity: f64,
    pub remaining_quantity: f64,
    pub average_fill_price: Option<f64>,
    pub rejection_reason: Option<String>,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum InstrumentIdError {
    #[error("instrument identifier cannot be empty")]
    Empty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComponentStatus {
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub component: &'static str,
    pub status: ComponentStatus,
    pub details: Option<String>,
    pub observed_at: DateTime<Utc>,
}

impl HealthStatus {
    #[must_use]
    pub fn new(
        component: &'static str,
        status: ComponentStatus,
        details: Option<String>,
        observed_at: DateTime<Utc>,
    ) -> Self {
        Self {
            component,
            status,
            details,
            observed_at,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mode_defaults_to_paper() {
        assert_eq!(RuntimeMode::default(), RuntimeMode::Paper);
        assert!(!RuntimeMode::default().is_live());
    }

    #[test]
    fn runtime_mode_parses_variants() {
        assert_eq!(
            "paper".parse::<RuntimeMode>().ok(),
            Some(RuntimeMode::Paper)
        );
        assert_eq!(
            "sim".parse::<RuntimeMode>().ok(),
            Some(RuntimeMode::Simulation)
        );
        assert_eq!("live".parse::<RuntimeMode>().ok(), Some(RuntimeMode::Live));
    }

    #[test]
    fn instrument_rejects_empty() {
        assert_eq!(
            InstrumentId::new("   ").err(),
            Some(InstrumentIdError::Empty)
        );
    }

    #[test]
    fn market_order_rejects_limit_price() {
        let order = OrderRequest::new(
            InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
            OrderSide::Buy,
            OrderType::Market,
            1.0,
        )
        .expect("valid");

        let order = OrderRequest {
            limit_price: Some(1.0),
            ..order
        };

        assert_eq!(
            order.validate().err(),
            Some(OrderRequestError::UnexpectedLimitPrice)
        );
    }

    #[test]
    fn limit_order_requires_limit_price() {
        let order = OrderRequest::new(
            InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
            OrderSide::Sell,
            OrderType::Limit,
            1.0,
        )
        .expect("valid");

        assert_eq!(
            order.validate().err(),
            Some(OrderRequestError::MissingLimitPrice)
        );
    }

    #[test]
    fn limit_order_builder_sets_price() {
        let order = OrderRequest::new(
            InstrumentId::new("BTC-USDT-SWAP").expect("instrument"),
            OrderSide::Sell,
            OrderType::Limit,
            1.0,
        )
        .and_then(|order| order.with_limit_price(42.0))
        .expect("valid limit order");

        assert_eq!(order.limit_price, Some(42.0));
        assert!(order.validate().is_ok());
    }
}
