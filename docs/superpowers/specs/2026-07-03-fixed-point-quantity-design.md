# Fixed-Point Quantity Design

**Date:** 2026-07-03
**Status:** Approved for implementation
**Motivation:** Crypto trade/quote sizes are destroyed at the provider
boundary. `alpaca_crypto.rs` converts Alpaca's fractional `f64` sizes with
`f64_to_u64_saturating(t.size.round())` — a 0.004 BTC trade arrives at every
consumer as `size: 0`. Sizes and volumes must become fixed-point, mirroring
the existing [`Price`] design, before crypto consumers (executioner) bind to
the feed.

## Design summary

Introduce `Quantity`, a fixed-point size/volume type in `datamancer-core`,
and use it for `Trade.size`, `Quote.bid_size`, `Quote.ask_size`, and
`Bar.volume`. Same philosophy as `Price`: datamancer defines its own
representation; consumers convert at their own boundary.

```rust
/// A size or volume in fixed-point units of `1e-9` of the instrument's
/// base unit (shares, coins, contracts).
///
/// Universal scale across asset classes — whole equity shares and
/// satoshi-granular (1e-8) crypto sizes both fit without truncation.
/// Sizes are non-negative by definition, hence `u64` (unlike `Price`,
/// which is signed because prices and spreads can be negative).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub struct Quantity(pub u64);

impl Quantity {
    /// Internal units per one whole base unit (10⁹).
    pub const SCALE: u64 = 1_000_000_000;
    pub const ZERO: Self = Self(0);

    #[must_use]
    pub const fn from_raw(units: u64) -> Self { Self(units) }

    /// `from_units(100)` is 100 shares / 100 coins.
    #[must_use]
    pub const fn from_units(units: u64) -> Self { Self(units * Self::SCALE) }

    /// Construct from an `f64`, rounding to the nearest internal unit.
    /// Lossy by definition (provider wire formats are themselves `f64`).
    /// NaN, ±∞, and negative inputs collapse to `ZERO`; values at or above
    /// the representable maximum saturate.
    #[must_use]
    pub fn from_f64_round(value: f64) -> Self { /* saturating, like Price::from_f64_round + the current f64_to_u64_saturating contract */ }

    /// Lossy conversion for display / interchange.
    #[must_use]
    pub fn to_f64(self) -> f64 { /* raw / SCALE */ }

    #[must_use]
    pub const fn raw(self) -> u64 { self.0 }
}
```

Arithmetic impls (`Add`, `Sub`, `AddAssign`, `SubAssign`, `Mul<u64>`,
`Div<u64>`) mirror `Price`. `Sub` on `u64` panics on underflow in debug and
wraps in release — if a call site needs signed deltas, it converts to `i128`
itself; do not add a signed variant speculatively (YAGNI).

### Range check (why 1e-9 is safe)

- `u64::MAX / SCALE` ≈ 1.8 × 10¹⁰ whole units per value.
- Largest realistic field is `Bar.volume` on 1-day bars: extreme meme-stock
  days reach ~10⁹ shares — 18× headroom remains.
- Smallest crypto increment in the wild is 1e-8 (satoshi); 1e-9 covers it
  with a spare digit.
- f64 round-trip error at the top of the range is bounded by the f64 ulp
  (~128 raw units at 10¹⁸ raw = 1.3e-7 of a share) — immaterial, and the
  provider wire is f64 to begin with.

## Alternatives rejected

- **`f64` sizes end-to-end** — loses `Eq`/`Hash`/`Ord` on every event type
  and contradicts the reasoning that produced `Price`.
- **Per-asset-class units** (satoshis for crypto, shares for equities in the
  same `u64`) — implicit unit keyed on asset class is a silent
  misinterpretation hazard.
- **`i64`-backed `Quantity`** — signed symmetry with `Price` buys nothing
  (sizes are non-negative) and halves the volume headroom.

## Change inventory

### 1. `datamancer-core`

- `src/quantity.rs` (new): the type above, unit tests for construction,
  saturation, rounding, and the f64 round-trip.
- `src/event.rs`: `Trade.size: Quantity`, `Quote.bid_size: Quantity`,
  `Quote.ask_size: Quantity`, `Bar.volume: Quantity`.
- `src/lib.rs`: export `Quantity`.
- Like `Price`, `Quantity` does **not** derive `Serialize`/`Deserialize`;
  wire layers carry raw integers explicitly.

### 2. Providers (`datamancer/src/providers`)

The actual bug fix — conversions stop truncating:

- `mod.rs`: replace `f64_to_u64_saturating` with a
  `Quantity::from_f64_round`-based boundary (the saturation/NaN contract in
  the current helper's doc comment moves onto `from_f64_round`).
- `alpaca.rs`: WS sizes `f64` → `Quantity::from_f64_round(q.bid_size)` etc.;
  historical whole-share counts → `Quantity::from_units(u64::from(t.size))`.
- `alpaca_crypto.rs`: drop the `.round()` calls — fractional sizes convert
  exactly at 1e-9. **This is the line the whole spec exists for.**

### 3. iceoryx2 transport (`datamancer-transport-iceoryx2`)

- `payload.rs`: `size0`/`size1` stay `u64` — **layout unchanged** (the field
  already holds 64 bits; only the interpretation changes to raw 1e-9 units).
  `to_pod`/`from_pod` gain `.raw()`/`from_raw` calls. Update the field doc
  comments to say "raw `Quantity` units".
- Same-host transport, daemon and consumers rebuild together — no version
  negotiation exists or is added. Mixed-version same-host deployment is
  already unsupported.

### 4. WS transport (`datamancer-transport-ws`)

- `wire.rs`: `size`/`bid_size`/`ask_size`/`volume` stay `u64` JSON numbers
  but carry raw units — **consistent with `price`, which already crosses as
  raw fixed-point `i64` under a plain field name.** Update the module doc
  ("Prices and quantities cross as raw fixed-point integers because core
  types do not derive `Serialize`").
- This is a breaking semantic change to the operator-facing wire contract:
  update the regression-guard tests' expected JSON and note the break in the
  commit message. There is one known consumer (executioner, not yet bound).

### 5. Storage (`datamancer/src/storage`)

- `surreal.rs` / `surreal_tap_log.rs`: rename columns `size` → `size_raw`,
  `bid_size` → `bid_size_raw`, `ask_size` → `ask_size_raw`,
  `volume` → `volume_raw` — consistent with the existing `price_raw`
  convention, and the rename makes pre-change rows fail loudly (missing
  column) instead of being silently misread at the wrong scale.
- Cached historical data written before this change is invalid regardless
  (crypto sizes were already rounded at ingest). The cache is read-through:
  document "drop the store (or the affected tables) and re-fetch" as the
  migration. No in-place migration code (YAGNI for a pre-1.0 cache).

### 6. `datamancer-client`

- No structural change (specs/codes don't carry sizes). Re-run the
  regression-guarded vocabulary tests; update any doc references to size
  semantics.

## Testing

- `datamancer-core`: `Quantity` unit tests (round-trip over representative
  values including 0.004 BTC ⇒ `Quantity::from_raw(4_000_000)`, saturation,
  NaN/negative collapse, `from_units`).
- Provider tests: `alpaca_crypto` conversion test asserts a fractional size
  survives exactly (the current test's `size: 1` fixture becomes a
  fractional case). Equity tests updated to `Quantity::from_units`.
- Transport round-trip tests (both transports): fractional-size events
  survive logical → wire → logical unchanged.
- Storage: write/read round-trip with fractional sizes against the renamed
  columns.

## Non-goals

- Fractional **order** sizes in executioner (its own concern, its own types).
- Changing `Price` in any way.
- In-place migration of previously cached rows. (Pre-migration databases are
  instead refused at open via a schema-version marker, added in review —
  loud error rather than silent decode failures.)

Wire version negotiation was originally a non-goal here, but review found
that reinterpreting the size fields under unchanged layouts/field names lets
mixed-version peers silently exchange 1e9x-wrong sizes, so both transports
gained version gating: the iceoryx2 service names embed `WIRE_VERSION`, and
the WS transport negotiates the `datamancer.v2` subprotocol on the handshake.
