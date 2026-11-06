//! What a live subscription asks for and what it opened.

use serde::Serialize;

use crate::Timestamp;

/// What a live subscription covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(tag = "scope", rename_all = "snake_case")]
#[non_exhaustive]
pub enum LiveScope {
    /// Purely from "now".
    Live {
        /// Deliver the provider's latest value as the first data event.
        /// `true` promises an attempt, not a value: the provider may have
        /// none, and a live print that arrives first discards it.
        ///
        /// Fixed by whoever creates the session; a joiner takes the session
        /// as it is, whatever it asked for. On a seeding session, a joiner of
        /// either `seed` receives the seed if it joined before the seed was
        /// delivered, and otherwise starts at the next live print. On an
        /// unseeded session, a joiner receives no seed. [`Subscribed`] reports
        /// the session's choice, not whether a seed reached this subscriber.
        seed: bool,
    },
    /// History from `from` up to the live edge, then the live arrivals
    /// buffered during the backfill, then the live tail. `from` must precede
    /// the live edge: see [`Error::BackfillFromNotInPast`](crate::Error::BackfillFromNotInPast).
    LiveBackfill {
        /// Inclusive source-time start of the history.
        #[serde(rename = "backfill_from")]
        from: Timestamp,
    },
}

impl Default for LiveScope {
    /// [`LiveScope::Live`], seeded.
    fn default() -> Self {
        Self::Live { seed: true }
    }
}

impl LiveScope {
    /// The start of the backfill, if this scope has one.
    #[must_use]
    pub const fn backfill_from(&self) -> Option<Timestamp> {
        match self {
            Self::Live { .. } => None,
            Self::LiveBackfill { from } => Some(*from),
        }
    }
}

/// What a subscribe opened. A pair is served by one shared session whose scope
/// is fixed by the subscriber that created it, so this, not the request, says
/// what the stream carries.
///
/// A joiner's stream begins where it joined, with no
/// [`Gap`](crate::ControlKind::Gap) for what it missed. A backfill still
/// running when it joined reaches it from that point on: the remainder of the
/// history, then every buffered live arrival, then the live tail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Subscribed {
    /// This subscriber created the session, with this scope.
    Created(LiveScope),
    /// This subscriber joined a session already open.
    Joined {
        /// The scope the session was created with.
        session: LiveScope,
    },
}

impl Subscribed {
    /// The start of the history this subscriber asked for and created:
    /// `Some` only for a created backfill. A joiner reads `None`, even when
    /// the remainder of a running backfill reaches it.
    #[must_use]
    pub const fn history_from(&self) -> Option<Timestamp> {
        match self {
            Self::Created(scope) => scope.backfill_from(),
            Self::Joined { .. } => None,
        }
    }

    /// The scope of the session behind this subscription.
    #[must_use]
    pub const fn session_scope(&self) -> LiveScope {
        match self {
            Self::Created(scope) | Self::Joined { session: scope } => *scope,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{LiveScope, Subscribed};
    use crate::Timestamp;

    #[test]
    fn live_scope_keeps_its_wire_shape() {
        assert_eq!(
            serde_json::to_string(&LiveScope::default()).unwrap(),
            r#"{"scope":"live","seed":true}"#
        );
        assert_eq!(
            serde_json::to_string(&LiveScope::Live { seed: false }).unwrap(),
            r#"{"scope":"live","seed":false}"#
        );
        let backfill = LiveScope::LiveBackfill { from: Timestamp(9) };
        let json = serde_json::to_string(&backfill).unwrap();
        assert_eq!(json, r#"{"scope":"live_backfill","backfill_from":9}"#);
    }

    #[test]
    fn only_a_created_backfill_has_history_of_its_own() {
        let backfill = LiveScope::LiveBackfill { from: Timestamp(9) };
        assert_eq!(
            Subscribed::Created(backfill).history_from(),
            Some(Timestamp(9))
        );
        assert_eq!(
            Subscribed::Created(LiveScope::default()).history_from(),
            None
        );
        let joined = Subscribed::Joined { session: backfill };
        assert_eq!(joined.history_from(), None);
        assert_eq!(joined.session_scope(), backfill);
    }
}
