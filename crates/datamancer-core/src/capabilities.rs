//! Per-instrument order/fractional capabilities: source-agnostic reference
//! data describing the orders a venue will accept for an instrument.
//!
//! This is descriptive reference data, **not an order API** — datamancer
//! places no orders. Every non-universal field is `Option`; `None` means the
//! provider did not report it, never "false" or "zero".

use serde::{Deserialize, Serialize};

use crate::{Price, Quantity};

/// What kinds of (fractional) orders a venue accepts for an instrument.
///
/// All fields optional: coverage is ragged across providers and asset classes
/// (e.g. Alpaca equities advertise `fractionable` but no per-asset sizing;
/// Alpaca crypto likewise advertises no sizing and reports `fractionable`
/// trivially true; IBKR advertises sizing but gates fractional eligibility on
/// the account). `None` = unknown.
#[non_exhaustive]
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstrumentCapabilities {
    /// Whether fractional-quantity orders are accepted at all.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fractionable: Option<bool>,
    /// Minimum order size (fractional quantity).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_qty: Option<Quantity>,
    /// Quantity step / increment.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qty_increment: Option<Quantity>,
    /// Price increment (tick size).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub price_increment: Option<Price>,
    /// Minimum notional (dollar) order value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_notional: Option<Price>,
    /// Whether notional (dollar-based) orders are accepted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_notional_orders: Option<bool>,
    /// Order types valid for a fractional order on this instrument.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_order_types: Option<Vec<OrderType>>,
    /// Times-in-force valid for a fractional order on this instrument.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_tif: Option<Vec<TimeInForce>>,
}

/// Descriptive order-type vocabulary. Reference data, not an order API.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum OrderType {
    Market,
    Limit,
    Stop,
    StopLimit,
}

/// Descriptive time-in-force vocabulary. Reference data, not an order API.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TimeInForce {
    Day,
    Gtc,
    Ioc,
    Fok,
    Opg,
    Cls,
}

#[cfg(test)]
mod tests {
    use super::{InstrumentCapabilities, OrderType, TimeInForce};
    use crate::{Price, Quantity};

    #[test]
    fn capabilities_serde_round_trips_full() {
        let caps = InstrumentCapabilities {
            fractionable: Some(true),
            min_qty: Some(Quantity::from_units(1)),
            qty_increment: Some(Quantity::from_f64_round(0.0001)),
            price_increment: Some(Price::from_f64_round(0.01)),
            min_notional: Some(Price::from_units(1)),
            supports_notional_orders: Some(true),
            allowed_order_types: Some(vec![
                OrderType::Market,
                OrderType::Limit,
                OrderType::Stop,
                OrderType::StopLimit,
            ]),
            allowed_tif: Some(vec![TimeInForce::Day]),
        };
        let json = serde_json::to_string(&caps).unwrap();
        let back: InstrumentCapabilities = serde_json::from_str(&json).unwrap();
        assert_eq!(caps, back);
    }

    #[test]
    fn capabilities_all_none_round_trips() {
        let caps = InstrumentCapabilities::default();
        let json = serde_json::to_string(&caps).unwrap();
        let back: InstrumentCapabilities = serde_json::from_str(&json).unwrap();
        assert_eq!(caps, back);
        assert!(caps.fractionable.is_none());
    }

    #[test]
    fn order_type_serializes_kebab_case() {
        assert_eq!(
            serde_json::to_string(&OrderType::StopLimit).unwrap(),
            "\"stop-limit\""
        );
    }

    #[test]
    fn tif_serializes_kebab_case() {
        assert_eq!(serde_json::to_string(&TimeInForce::Gtc).unwrap(), "\"gtc\"");
    }
}
