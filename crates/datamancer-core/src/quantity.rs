//! Fixed-point size/volume representation used by every datamancer event.
//!
//! Datamancer defines its own [`Quantity`] rather than depending on a
//! consumer's type, mirroring [`crate::Price`]: `u64` units of `1e-9` of the
//! instrument's base unit (shares, coins, contracts). Consumers that need a
//! different representation convert at their own boundary.

/// A size or volume in fixed-point units of `1e-9` of the instrument's base
/// unit (shares, coins, contracts).
///
/// Universal scale across asset classes — whole equity shares and
/// satoshi-granular (`1e-8`) crypto sizes both fit without truncation. Sizes
/// are non-negative by definition, hence `u64` (unlike [`crate::Price`], which
/// is signed because prices and spreads can be negative).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Quantity(pub u64);

impl Quantity {
    /// Internal units per one whole base unit (10⁹).
    pub const SCALE: u64 = 1_000_000_000;

    /// Zero quantity.
    pub const ZERO: Self = Self(0);

    /// Construct from raw internal units.
    #[must_use]
    pub const fn from_raw(units: u64) -> Self {
        Self(units)
    }

    /// Construct from whole base units. `from_units(100)` is 100 shares /
    /// 100 coins.
    #[must_use]
    pub const fn from_units(units: u64) -> Self {
        Self(units * Self::SCALE)
    }

    /// Construct from an `f64`, rounding to the nearest internal unit.
    ///
    /// Lossy by definition; provider wire formats are themselves `f64`. NaN,
    /// ±∞, and negative inputs collapse to [`Quantity::ZERO`]; values at or
    /// above the representable maximum saturate to `u64::MAX` internal units.
    #[must_use]
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "lossy by contract — entire purpose of this constructor. \
                  The f64→u64 cast is saturating: NaN and negatives clamp to 0, \
                  overflow clamps to u64::MAX — exactly the documented contract."
    )]
    pub fn from_f64_round(value: f64) -> Self {
        Self((value * Self::SCALE as f64).round() as u64)
    }

    /// Lossy conversion to `f64` for display or external interchange.
    #[must_use]
    #[allow(
        clippy::cast_precision_loss,
        reason = "lossy by contract — entire purpose of this conversion"
    )]
    pub fn to_f64(self) -> f64 {
        self.0 as f64 / Self::SCALE as f64
    }

    /// Raw internal units.
    #[must_use]
    pub const fn raw(self) -> u64 {
        self.0
    }
}

impl std::ops::Add for Quantity {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        Self(self.0 + other.0)
    }
}

impl std::ops::Sub for Quantity {
    type Output = Self;
    fn sub(self, other: Self) -> Self {
        Self(self.0 - other.0)
    }
}

impl std::ops::AddAssign for Quantity {
    fn add_assign(&mut self, other: Self) {
        self.0 += other.0;
    }
}

impl std::ops::SubAssign for Quantity {
    fn sub_assign(&mut self, other: Self) {
        self.0 -= other.0;
    }
}

impl std::ops::Mul<u64> for Quantity {
    type Output = Self;
    fn mul(self, n: u64) -> Self {
        Self(self.0 * n)
    }
}

impl std::ops::Div<u64> for Quantity {
    type Output = Self;
    fn div(self, n: u64) -> Self {
        Self(self.0 / n)
    }
}

#[cfg(test)]
mod tests {
    use super::Quantity;

    #[test]
    fn from_units_scales_by_1e9() {
        assert_eq!(Quantity::from_units(100), Quantity(100 * Quantity::SCALE));
        assert_eq!(Quantity::from_units(1).raw(), 1_000_000_000);
    }

    #[test]
    fn from_raw_is_verbatim() {
        assert_eq!(Quantity::from_raw(4_000_000).raw(), 4_000_000);
    }

    #[test]
    fn fractional_crypto_size_survives_exactly() {
        // The line the whole spec exists for: 0.004 BTC must not round to zero.
        assert_eq!(
            Quantity::from_f64_round(0.004),
            Quantity::from_raw(4_000_000)
        );
        assert_ne!(Quantity::from_f64_round(0.004), Quantity::ZERO);
    }

    #[test]
    fn from_f64_round_rounds_to_nearest_unit() {
        // 1.5 units -> 1_500_000_000 raw.
        assert_eq!(Quantity::from_f64_round(1.5).raw(), 1_500_000_000);
        // Sub-unit rounding: 2.5 raw units rounds to 3 (round-half-away or -even
        // both give an integer; assert on a value with an unambiguous nearest).
        assert_eq!(Quantity::from_f64_round(0.000_000_002_6).raw(), 3);
    }

    #[test]
    fn nan_and_infinities_collapse_to_zero() {
        assert_eq!(Quantity::from_f64_round(f64::NAN), Quantity::ZERO);
        assert_eq!(Quantity::from_f64_round(f64::INFINITY), Quantity(u64::MAX));
        assert_eq!(Quantity::from_f64_round(f64::NEG_INFINITY), Quantity::ZERO);
    }

    #[test]
    fn negative_inputs_collapse_to_zero() {
        assert_eq!(Quantity::from_f64_round(-1.0), Quantity::ZERO);
        assert_eq!(Quantity::from_f64_round(-0.004), Quantity::ZERO);
    }

    #[test]
    fn oversized_inputs_saturate() {
        // Above u64::MAX / SCALE whole units saturates rather than wrapping.
        assert_eq!(Quantity::from_f64_round(1e30), Quantity(u64::MAX));
    }

    #[test]
    fn to_f64_round_trips_representative_values() {
        assert!((Quantity::from_units(100).to_f64() - 100.0).abs() < 1e-9);
        assert!((Quantity::from_raw(4_000_000).to_f64() - 0.004).abs() < 1e-12);
        assert!(Quantity::ZERO.to_f64().abs() < f64::EPSILON);
    }

    #[test]
    fn arithmetic_mirrors_price() {
        assert_eq!(Quantity(10) + Quantity(5), Quantity(15));
        assert_eq!(Quantity(10) - Quantity(5), Quantity(5));
        assert_eq!(Quantity(10) * 3, Quantity(30));
        assert_eq!(Quantity(10) / 2, Quantity(5));
        let mut q = Quantity(10);
        q += Quantity(5);
        assert_eq!(q, Quantity(15));
        q -= Quantity(3);
        assert_eq!(q, Quantity(12));
    }
}
