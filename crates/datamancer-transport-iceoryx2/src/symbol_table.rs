//! Sink-local symbol interning.
//!
//! The data plane carries a compact [`SymbolId`] instead of the heap-backed
//! [`Instrument`]. A [`SymbolTable`] interns `Instrument -> SymbolId` at the
//! source (per client service) and produces [`SymbolAnnouncement`]s that the
//! announcement service publishes so a subscriber can rebuild the reverse
//! `SymbolId -> Instrument` mapping in a [`SymbolResolver`].
//!
//! `SymbolId` is **not** a global identity — it is a per-service compaction
//! handle. Two independent tables may assign the same id to different
//! instruments. Cross-client `seq` agreement does not depend on the id: `seq`
//! is carried verbatim from the source-stamped event.

use std::collections::HashMap;

use datamancer_core::{AssetClass, Instrument, ProviderId};
use iceoryx2::prelude::ZeroCopySend;

/// Maximum encoded instrument-tuple length, in bytes, carried in a
/// [`SymbolAnnouncement`]. Encoded form is `provider\x1fasset_class\x1fsymbol`.
pub const SYMBOL_STRING_CAPACITY: usize = 64;

/// Field separator for the encoded instrument tuple. ASCII unit-separator,
/// which never appears in a provider id, asset-class name, or symbol grammar.
const SEP: u8 = 0x1f;

type SymbolString = iceoryx2_bb_container::string::StaticString<SYMBOL_STRING_CAPACITY>;

/// A per-service compaction handle for an [`Instrument`] on the wire.
///
/// `#[repr(C)]` `Copy` POD (`ZeroCopySend` requires `repr(C)`) so it embeds in
/// the zero-copy payloads.
/// Real instruments are interned densely from `0`; [`SymbolId::CONNECTION`] is
/// reserved for connection-scoped controls and is never assigned to a real
/// instrument.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, ZeroCopySend)]
#[repr(C)]
pub struct SymbolId(pub u32);

impl SymbolId {
    /// Reserved id for connection-scoped controls (`SessionClosing`). Never
    /// assigned to a real instrument, so subscribers can route it distinctly.
    pub const CONNECTION: SymbolId = SymbolId(u32::MAX);
}

/// Error decoding an announcement's encoded instrument tuple.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SymbolDecodeError {
    /// The encoded tuple did not have exactly three `\x1f`-separated fields.
    MalformedTuple,
    /// The asset-class field was not a recognized variant.
    UnknownAssetClass,
    /// The encoded bytes were not valid UTF-8.
    InvalidUtf8,
}

impl std::fmt::Display for SymbolDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MalformedTuple => f.write_str("malformed instrument tuple"),
            Self::UnknownAssetClass => f.write_str("unknown asset class"),
            Self::InvalidUtf8 => f.write_str("invalid utf-8 in instrument tuple"),
        }
    }
}

impl std::error::Error for SymbolDecodeError {}

/// Error interning an instrument whose encoded tuple exceeds the announcement
/// capacity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstrumentTooLong {
    /// The encoded length that exceeded [`SYMBOL_STRING_CAPACITY`].
    pub encoded_len: usize,
}

impl std::fmt::Display for InstrumentTooLong {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "encoded instrument tuple is {} bytes, exceeds capacity {}",
            self.encoded_len, SYMBOL_STRING_CAPACITY
        )
    }
}

impl std::error::Error for InstrumentTooLong {}

fn encode_instrument(instrument: &Instrument) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(instrument.provider().as_str().as_bytes());
    out.push(SEP);
    out.extend_from_slice(instrument.asset_class().to_string().as_bytes());
    out.push(SEP);
    out.extend_from_slice(instrument.symbol().as_bytes());
    out
}

fn parse_asset_class(s: &str) -> Result<AssetClass, SymbolDecodeError> {
    match s {
        "equity" => Ok(AssetClass::Equity),
        "etf" => Ok(AssetClass::Etf),
        "crypto" => Ok(AssetClass::Crypto),
        _ => Err(SymbolDecodeError::UnknownAssetClass),
    }
}

fn decode_instrument(bytes: &[u8]) -> Result<Instrument, SymbolDecodeError> {
    let text = std::str::from_utf8(bytes).map_err(|_| SymbolDecodeError::InvalidUtf8)?;
    let mut parts = text.split('\u{1f}');
    let provider = parts.next().ok_or(SymbolDecodeError::MalformedTuple)?;
    let asset_class = parts.next().ok_or(SymbolDecodeError::MalformedTuple)?;
    let symbol = parts.next().ok_or(SymbolDecodeError::MalformedTuple)?;
    if parts.next().is_some() {
        return Err(SymbolDecodeError::MalformedTuple);
    }
    Ok(Instrument::new(
        ProviderId::new(provider),
        parse_asset_class(asset_class)?,
        symbol,
    ))
}

/// A POD announcement mapping a [`SymbolId`] to its encoded instrument tuple.
///
/// Published on the low-rate announcement service so subscribers (including
/// late joiners draining history) can resolve `SymbolId -> Instrument`.
/// Treated as an idempotent upsert keyed by `id`.
#[derive(Debug, Clone, Copy, ZeroCopySend)]
#[repr(C)]
pub struct SymbolAnnouncement {
    pub id: SymbolId,
    instrument: SymbolString,
}

impl SymbolAnnouncement {
    /// Decode the announced instrument tuple.
    ///
    /// # Errors
    ///
    /// Returns [`SymbolDecodeError`] if the encoded bytes are malformed.
    pub fn instrument(&self) -> Result<Instrument, SymbolDecodeError> {
        use iceoryx2_bb_container::string::String as _;
        decode_instrument(self.instrument.as_bytes())
    }
}

/// Source-side interner. Owned by the per-client sink instance.
#[derive(Debug, Default)]
pub struct SymbolTable {
    forward: HashMap<Instrument, SymbolId>,
    reverse: Vec<Instrument>,
}

impl SymbolTable {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Intern an instrument, returning its dense [`SymbolId`] (first-seen
    /// order). Idempotent: a repeated instrument returns the same id.
    ///
    /// # Errors
    ///
    /// Returns [`InstrumentTooLong`] if the encoded tuple exceeds
    /// [`SYMBOL_STRING_CAPACITY`] (it could not be announced, so it cannot be
    /// interned).
    ///
    /// # Panics
    ///
    /// Panics if more than `u32::MAX` distinct instruments are interned — the
    /// `SymbolId` space (minus the reserved [`SymbolId::CONNECTION`]) is
    /// exhausted. Unreachable for any realistic subscription count.
    pub fn intern(&mut self, instrument: &Instrument) -> Result<SymbolId, InstrumentTooLong> {
        if let Some(id) = self.forward.get(instrument) {
            return Ok(*id);
        }
        let encoded_len = encode_instrument(instrument).len();
        if encoded_len > SYMBOL_STRING_CAPACITY {
            return Err(InstrumentTooLong { encoded_len });
        }
        let next = u32::try_from(self.reverse.len()).expect("symbol-id space exhausted");
        assert_ne!(next, SymbolId::CONNECTION.0, "symbol-id space exhausted");
        let id = SymbolId(next);
        self.forward.insert(instrument.clone(), id);
        self.reverse.push(instrument.clone());
        Ok(id)
    }

    /// Build the announcement for an interned id, or `None` if `id` is not a
    /// real interned instrument (e.g. [`SymbolId::CONNECTION`]).
    #[must_use]
    pub fn announcement(&self, id: SymbolId) -> Option<SymbolAnnouncement> {
        let instrument = self.reverse.get(id.0 as usize)?;
        let encoded = encode_instrument(instrument);
        let string = SymbolString::from_bytes(&encoded).ok()?;
        Some(SymbolAnnouncement {
            id,
            instrument: string,
        })
    }

    /// Announcements for every interned instrument, for full-table republish.
    /// Ids are dense and bounded by the `intern` cap, so no conversion can fail.
    pub fn announcements(&self) -> impl Iterator<Item = SymbolAnnouncement> + '_ {
        self.reverse.iter().enumerate().filter_map(|(i, _)| {
            let id = SymbolId(u32::try_from(i).ok()?);
            self.announcement(id)
        })
    }
}

/// Subscriber-side reverse map, fed by the announcement stream.
#[derive(Debug, Default)]
pub struct SymbolResolver {
    map: HashMap<u32, Instrument>,
}

impl SymbolResolver {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply one announcement as an idempotent upsert keyed by `SymbolId`.
    ///
    /// # Errors
    ///
    /// Returns [`SymbolDecodeError`] if the announcement's tuple is malformed.
    pub fn apply(&mut self, announcement: &SymbolAnnouncement) -> Result<(), SymbolDecodeError> {
        let instrument = announcement.instrument()?;
        self.map.insert(announcement.id.0, instrument);
        Ok(())
    }

    /// Insert a known mapping directly (test/helper convenience).
    pub fn insert(&mut self, id: SymbolId, instrument: Instrument) {
        self.map.insert(id.0, instrument);
    }

    /// Resolve a [`SymbolId`] to its [`Instrument`], or `None` if no
    /// announcement for it has been seen yet (the subscriber must hold the
    /// affected data sample until it resolves).
    #[must_use]
    pub fn resolve(&self, id: SymbolId) -> Option<&Instrument> {
        self.map.get(&id.0)
    }
}

#[cfg(test)]
mod tests {
    use super::{InstrumentTooLong, SYMBOL_STRING_CAPACITY, SymbolId, SymbolResolver, SymbolTable};
    use datamancer_core::{AssetClass, Instrument, ProviderId};

    fn inst(symbol: &str) -> Instrument {
        Instrument::new(
            ProviderId::from_static("alpaca"),
            AssetClass::Crypto,
            symbol,
        )
    }

    #[test]
    fn intern_is_dense_and_idempotent() {
        let mut t = SymbolTable::new();
        let a = t.intern(&inst("BTC/USD")).unwrap();
        let b = t.intern(&inst("ETH/USD")).unwrap();
        let a2 = t.intern(&inst("BTC/USD")).unwrap();
        assert_eq!(a, SymbolId(0));
        assert_eq!(b, SymbolId(1));
        assert_eq!(a, a2);
    }

    #[test]
    fn connection_id_is_never_assigned() {
        let mut t = SymbolTable::new();
        for i in 0..100 {
            let id = t.intern(&inst(&format!("S{i}"))).unwrap();
            assert_ne!(id, SymbolId::CONNECTION);
        }
    }

    #[test]
    fn symbol_table_round_trip() {
        let mut t = SymbolTable::new();
        let instrument = inst("BTC/USD");
        let id = t.intern(&instrument).unwrap();
        let announcement = t.announcement(id).unwrap();

        let mut resolver = SymbolResolver::new();
        resolver.apply(&announcement).unwrap();
        assert_eq!(resolver.resolve(id), Some(&instrument));
    }

    #[test]
    fn announcement_preserves_full_tuple() {
        let mut t = SymbolTable::new();
        let instrument = Instrument::new(
            ProviderId::from_static("alpaca"),
            AssetClass::Equity,
            "AAPL",
        );
        let id = t.intern(&instrument).unwrap();
        let decoded = t.announcement(id).unwrap().instrument().unwrap();
        assert_eq!(decoded, instrument);
    }

    #[test]
    fn symbol_id_is_not_global_identity() {
        let mut a = SymbolTable::new();
        let mut b = SymbolTable::new();
        let id_a = a.intern(&inst("BTC/USD")).unwrap();
        let id_b = b.intern(&inst("ETH/USD")).unwrap();
        // Two independent tables assign the same id to different instruments.
        assert_eq!(id_a, id_b);
        assert_eq!(id_a, SymbolId(0));
    }

    #[test]
    fn instrument_over_capacity_is_rejected() {
        let mut t = SymbolTable::new();
        let long_symbol = "X".repeat(SYMBOL_STRING_CAPACITY);
        let err = t.intern(&inst(&long_symbol)).unwrap_err();
        assert!(matches!(err, InstrumentTooLong { .. }));
        // The rejected instrument was not interned.
        assert!(t.announcement(SymbolId(0)).is_none());
    }

    #[test]
    fn announcements_iter_covers_all() {
        let mut t = SymbolTable::new();
        t.intern(&inst("BTC/USD")).unwrap();
        t.intern(&inst("ETH/USD")).unwrap();
        let mut resolver = SymbolResolver::new();
        for a in t.announcements() {
            resolver.apply(&a).unwrap();
        }
        assert_eq!(resolver.resolve(SymbolId(0)), Some(&inst("BTC/USD")));
        assert_eq!(resolver.resolve(SymbolId(1)), Some(&inst("ETH/USD")));
    }
}
