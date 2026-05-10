//! Fixed-point price representation used by every datamancer event.
//!
//! Datamancer is intended to be extractable as a standalone library, so it
//! defines its own [`Price`] rather than depending on a consumer's type. The
//! representation matches Citadel's convention (`i64` nanos of currency,
//! `1e-9` resolution); consumers that need a different representation convert
//! at their own boundary.

/// A price in fixed-point units of `1e-9` of the quoted currency.
///
/// Universal scale across instruments — equities (2 dp), FX (5 dp), and
/// crypto (8 dp) all fit without truncation. Negative values are valid
/// (futures can settle negative; deltas and spreads are routinely signed).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Price(pub i64);

impl Price {
    /// Internal units per one whole currency unit (10⁹).
    pub const SCALE: i64 = 1_000_000_000;

    /// Zero price.
    pub const ZERO: Self = Self(0);

    /// Construct from raw internal units.
    #[must_use]
    pub const fn from_raw(units: i64) -> Self {
        Self(units)
    }

    /// Construct from whole currency units. `from_units(150)` is `$150.00`.
    #[must_use]
    pub const fn from_units(units: i64) -> Self {
        Self(units * Self::SCALE)
    }

    /// Construct from an `f64`, rounding to the nearest internal unit.
    ///
    /// Lossy by definition; use only for fixtures, tests, or initial parsing
    /// where the source representation is itself `f64`.
    #[must_use]
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "lossy by contract — entire purpose of this constructor"
    )]
    pub fn from_f64_round(value: f64) -> Self {
        Self((value * Self::SCALE as f64).round() as i64)
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
    pub const fn raw(self) -> i64 {
        self.0
    }
}

impl std::ops::Add for Price {
    type Output = Self;
    fn add(self, other: Self) -> Self {
        Self(self.0 + other.0)
    }
}

impl std::ops::Sub for Price {
    type Output = Self;
    fn sub(self, other: Self) -> Self {
        Self(self.0 - other.0)
    }
}

impl std::ops::AddAssign for Price {
    fn add_assign(&mut self, other: Self) {
        self.0 += other.0;
    }
}

impl std::ops::SubAssign for Price {
    fn sub_assign(&mut self, other: Self) {
        self.0 -= other.0;
    }
}

impl std::ops::Mul<i64> for Price {
    type Output = Self;
    fn mul(self, n: i64) -> Self {
        Self(self.0 * n)
    }
}

impl std::ops::Div<i64> for Price {
    type Output = Self;
    fn div(self, n: i64) -> Self {
        Self(self.0 / n)
    }
}
