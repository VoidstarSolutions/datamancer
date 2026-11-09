//! What a live subscription asks for and what it opened.

use std::time::Duration;

use serde::{Deserialize, Deserializer, Serialize};

use crate::Timestamp;

/// How long a replay waits on one consumer: since it last freed a slot, or in
/// all while another consumer had room. Past it the fan-out stops pacing that
/// consumer and its loss folds into a
/// [`ControlKind::Gap`](crate::ControlKind::Gap); a WS sink gives the
/// connection up.
pub const REPLAY_STALL_LIMIT: Duration = Duration::from_secs(30);

/// What a live subscription covers. On the wire it is a `scope` name and,
/// for a backfill, a `backfill_from`; no `scope` reads as live.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "ScopeFields", try_from = "ScopeFields")]
#[non_exhaustive]
pub enum LiveScope {
    /// Purely from "now".
    #[default]
    Live,
    /// History from `from` up to the live edge, then the live arrivals
    /// buffered during the backfill, then the live tail. `from` must precede
    /// the live edge: see [`Error::BackfillFromNotInPast`](crate::Error::BackfillFromNotInPast).
    ///
    /// The live edge is the local clock read after the provider's subscribe
    /// returns, and nothing dedupes at the seam. An event between the start of
    /// live delivery and that reading can arrive twice, once in the history and
    /// once live. With clock skew, a lagging history endpoint or a provider
    /// that returns before the venue acknowledges the pair, events at the edge
    /// can be missing with no [`Gap`](crate::ControlKind::Gap). A `Gap` reports
    /// a failed fetch, a buffer overflow or a subscriber that stops pacing the
    /// replay.
    ///
    /// With a tap log, live arrivals reach it at the seam. An arrival the seam
    /// ring evicts, or one still buffered when the session closes before the
    /// seam, is not logged and leaves no mark in the log.
    LiveBackfill {
        /// Inclusive source-time start of the history.
        from: Timestamp,
    },
}

/// The name of a [`LiveScope`], without its data.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ScopeName {
    /// Names [`LiveScope::Live`].
    #[default]
    Live,
    /// Names [`LiveScope::LiveBackfill`].
    LiveBackfill,
}

/// A scope name and a backfill start that do not make a [`LiveScope`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ScopeMismatch {
    /// [`ScopeName::LiveBackfill`] with no start.
    #[error("scope live_backfill requires backfill_from")]
    MissingBackfillFrom,
    /// A start under [`ScopeName::Live`].
    #[error("backfill_from requires scope live_backfill")]
    UnexpectedBackfillFrom,
}

/// The JSON form of a [`LiveScope`].
#[derive(Serialize, Deserialize)]
struct ScopeFields {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    scope: Option<ScopeName>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    backfill_from: Option<Timestamp>,
}

impl From<LiveScope> for ScopeFields {
    fn from(scope: LiveScope) -> Self {
        let name = match scope {
            LiveScope::Live => ScopeName::Live,
            LiveScope::LiveBackfill { .. } => ScopeName::LiveBackfill,
        };
        Self {
            scope: Some(name),
            backfill_from: scope.backfill_from(),
        }
    }
}

impl TryFrom<ScopeFields> for LiveScope {
    type Error = ScopeMismatch;

    fn try_from(fields: ScopeFields) -> Result<Self, ScopeMismatch> {
        Self::from_parts(fields.scope.unwrap_or_default(), fields.backfill_from)
    }
}

impl LiveScope {
    /// Build a scope from its name and backfill start.
    ///
    /// # Errors
    ///
    /// [`ScopeMismatch::MissingBackfillFrom`] for a backfill with no start,
    /// [`ScopeMismatch::UnexpectedBackfillFrom`] for a start without one.
    pub const fn from_parts(
        name: ScopeName,
        backfill_from: Option<Timestamp>,
    ) -> Result<Self, ScopeMismatch> {
        match (name, backfill_from) {
            (ScopeName::Live, None) => Ok(Self::Live),
            (ScopeName::LiveBackfill, Some(from)) => Ok(Self::LiveBackfill { from }),
            (ScopeName::LiveBackfill, None) => Err(ScopeMismatch::MissingBackfillFrom),
            (ScopeName::Live, Some(_)) => Err(ScopeMismatch::UnexpectedBackfillFrom),
        }
    }

    /// Parse a scope that may be absent: no `scope` and no `backfill_from`
    /// read as `None`.
    ///
    /// # Errors
    ///
    /// The deserializer's error for an unknown name or a [`ScopeMismatch`].
    pub fn deserialize_optional<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Self>, D::Error> {
        let fields = ScopeFields::deserialize(deserializer)?;
        if fields.scope.is_none() && fields.backfill_from.is_none() {
            return Ok(None);
        }
        Self::try_from(fields)
            .map(Some)
            .map_err(serde::de::Error::custom)
    }

    /// The start of the backfill, if this scope has one.
    #[must_use]
    pub const fn backfill_from(&self) -> Option<Timestamp> {
        match self {
            Self::Live => None,
            Self::LiveBackfill { from } => Some(*from),
        }
    }
}

/// What a subscribe opened. A pair is served by one shared session whose scope
/// is fixed by the subscriber that created it, so this, not the request, says
/// what the stream carries.
///
/// A joiner's stream begins where it joined, with no
/// [`Gap`](crate::ControlKind::Gap) for what it missed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Subscribed {
    /// This subscriber created the session, with this scope.
    #[non_exhaustive]
    Created(LiveScope),
    /// This subscriber joined a session already open.
    #[non_exhaustive]
    Joined {
        /// The scope the session was created with.
        session: LiveScope,
        /// A backfill was running at the join, so the stream carries what is
        /// left of it, then the buffered live arrivals, then the live tail.
        backfill_in_flight: bool,
    },
}

impl Subscribed {
    /// A creation of a session with this scope.
    #[must_use]
    pub const fn created(scope: LiveScope) -> Self {
        Self::Created(scope)
    }

    /// A join of a session with this scope.
    #[must_use]
    pub const fn joined(session: LiveScope, backfill_in_flight: bool) -> Self {
        Self::Joined {
            session,
            backfill_in_flight,
        }
    }

    /// The start of the history this subscriber created: `Some` only for a
    /// created backfill.
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
            Self::Created(scope) | Self::Joined { session: scope, .. } => *scope,
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
            serde_json::to_string(&LiveScope::Live).unwrap(),
            r#"{"scope":"live"}"#
        );
        let backfill = LiveScope::LiveBackfill { from: Timestamp(9) };
        let json = serde_json::to_string(&backfill).unwrap();
        assert_eq!(json, r#"{"scope":"live_backfill","backfill_from":9}"#);
    }

    #[test]
    fn live_scope_parses_what_it_serializes() {
        for scope in [
            LiveScope::Live,
            LiveScope::LiveBackfill { from: Timestamp(9) },
        ] {
            let json = serde_json::to_string(&scope).unwrap();
            assert_eq!(serde_json::from_str::<LiveScope>(&json).unwrap(), scope);
        }
    }

    #[test]
    fn a_scope_and_start_that_disagree_do_not_parse() {
        for json in [
            r#"{"scope":"live_backfill"}"#,
            r#"{"scope":"live","backfill_from":9}"#,
            r#"{"backfill_from":9}"#,
            r#"{"scope":"history"}"#,
        ] {
            assert!(serde_json::from_str::<LiveScope>(json).is_err(), "{json}");
        }
    }

    #[test]
    fn only_a_created_backfill_has_history_of_its_own() {
        let backfill = LiveScope::LiveBackfill { from: Timestamp(9) };
        assert_eq!(
            Subscribed::Created(backfill).history_from(),
            Some(Timestamp(9))
        );
        assert_eq!(Subscribed::Created(LiveScope::Live).history_from(), None);
        let joined = Subscribed::Joined {
            session: backfill,
            backfill_in_flight: false,
        };
        assert_eq!(joined.history_from(), None);
        assert_eq!(joined.session_scope(), backfill);
    }
}
