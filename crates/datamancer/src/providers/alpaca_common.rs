//! What the two Alpaca providers share on the streaming side: the one
//! classifier of `oxidized_alpaca` failures the hub parks or backs off on,
//! and the symbol lists of an Alpaca subscription list the hub fills.

use datamancer_core::{BarInterval, EventKind, Instrument};
use oxidized_alpaca::streaming::StreamErrorCode;

use super::hub::{Classify, StreamFailure};

/// Classify a connect-time, subscribe or mid-stream error from
/// `oxidized_alpaca`.
///
/// `406 connection limit exceeded` is transient: another connection on the
/// same key pair holds the slot, which clears when it closes. So is
/// `407 slow client`, and a dropped socket. Only `402 auth failed` means the
/// credentials themselves are rejected, whether it arrives mid-stream
/// (`Error::StreamingError`) or as the server's refusal of a subscribe
/// (`Error::StreamingSubscribe`). Any other code on either variant,
/// `401 not authenticated` included, is transient.
///
/// Limitation to know about: at **connect** time `oxidized_alpaca` 0.0.10
/// (and 0.0.11) collapses every non-`authenticated` auth response into the
/// payload-less `Error::StreamingAuth` and only logs the envelope, so a 406
/// there is indistinguishable from a 402 and lands in the park. The hub
/// itself never opens a second connection, so a connect-time rejection is
/// treated as the credential case it is in practice; once upstream carries
/// the `StreamError` on that path, it classifies through the same arm as
/// the mid-stream case below with no further change here.
impl Classify for oxidized_alpaca::Error {
    fn classify(&self) -> StreamFailure {
        match self {
            Self::StreamingAuth => StreamFailure::Unauthenticated,
            Self::StreamingError(e) | Self::StreamingSubscribe(e) => match e.code {
                StreamErrorCode::AuthFailed => StreamFailure::Unauthenticated,
                _ => StreamFailure::Transient,
            },
            _ => StreamFailure::Transient,
        }
    }
}

/// The symbol lists of an Alpaca subscription list that the hub fills, one
/// per streamed [`EventKind`]. The stock and crypto lists both have these
/// four, as `Option<Vec<String>>` that is `None` when empty, and the hub
/// subscribes to nothing else, so one insert serves both.
pub(super) struct SymbolLists<'a> {
    pub(super) trades: &'a mut Option<Vec<String>>,
    pub(super) quotes: &'a mut Option<Vec<String>>,
    pub(super) bars: &'a mut Option<Vec<String>>,
    pub(super) daily_bars: &'a mut Option<Vec<String>>,
}

/// The list of a [`SymbolLists`] a streamed [`EventKind`] lands in: the one
/// classification of the kinds the stream carries, behind both `supports`
/// and the inserts, so a kind the providers promise is one the lists hold.
#[derive(Debug, Clone, Copy)]
enum Slot {
    Trades,
    Quotes,
    Bars,
    DailyBars,
}

impl Slot {
    /// The slot for `kind`; `None` for a bar interval the stream does not
    /// carry.
    const fn of(kind: EventKind) -> Option<Self> {
        match kind {
            EventKind::Trade => Some(Self::Trades),
            EventKind::Quote => Some(Self::Quotes),
            EventKind::Bar(BarInterval::OneMinute) => Some(Self::Bars),
            EventKind::Bar(BarInterval::OneDay) => Some(Self::DailyBars),
            EventKind::Bar(_) => None,
        }
    }
}

impl<'a> SymbolLists<'a> {
    /// Whether the stream carries `kind`: trades, quotes, and minute and
    /// daily bars. Both providers answer `supports` for the live surface
    /// from this.
    pub(super) const fn carries(kind: EventKind) -> bool {
        Slot::of(kind).is_some()
    }

    /// Add `instrument` to the list for `kind`, once.
    pub(super) fn insert(self, instrument: &Instrument, kind: EventKind) {
        if let Some(field) = self.list(kind) {
            let list = field.get_or_insert_with(Vec::new);
            if !list.iter().any(|s| s == instrument.symbol()) {
                list.push(instrument.symbol().to_string());
            }
        }
    }

    /// The list `kind` lands in.
    fn list(self, kind: EventKind) -> Option<&'a mut Option<Vec<String>>> {
        Some(match Slot::of(kind)? {
            Slot::Trades => self.trades,
            Slot::Quotes => self.quotes,
            Slot::Bars => self.bars,
            Slot::DailyBars => self.daily_bars,
        })
    }
}
