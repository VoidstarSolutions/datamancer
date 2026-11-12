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

/// What a live subscription covers. On the wire it is a `scope` name, then
/// `"seed":false` under an unseeded live or `backfill_from` under a backfill;
/// no `scope` reads as live, and under live no `seed` reads as seeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "ScopeFields", try_from = "ScopeFields")]
#[non_exhaustive]
pub enum LiveScope {
    /// Purely from "now". Build it with [`LiveScope::live`] or
    /// [`LiveScope::unseeded`].
    #[non_exhaustive]
    Live {
        /// `true` on the subscribe that creates the session asks for the
        /// provider's latest value as the first data event: an attempt, not a
        /// value, since the provider may have none and a live print that
        /// arrives first discards it. `false` means no seed in this stream,
        /// for a creator and a joiner alike.
        seed: bool,
    },
    /// History from `from` up to the live edge, then the live arrivals
    /// buffered during the backfill, then the live tail. `from` must precede
    /// the live edge: see [`Error::BackfillFromNotInPast`](crate::Error::BackfillFromNotInPast).
    /// Build it with [`LiveScope::backfill`].
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
    #[non_exhaustive]
    LiveBackfill {
        /// Inclusive source-time start of the history.
        from: Timestamp,
    },
}

impl Default for LiveScope {
    /// [`LiveScope::DEFAULT`].
    fn default() -> Self {
        Self::DEFAULT
    }
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
    /// A seed choice anywhere but under [`ScopeName::Live`].
    #[error("seed requires scope live")]
    UnexpectedSeed,
}

/// The JSON form of a [`LiveScope`].
#[derive(Serialize, Deserialize)]
struct ScopeFields {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    scope: Option<ScopeName>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    backfill_from: Option<Timestamp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    seed: Option<bool>,
}

impl ScopeFields {
    /// The scope these fields name. An absent `scope` means live.
    fn parse(self) -> Result<LiveScope, ScopeMismatch> {
        match (
            self.scope.unwrap_or_default(),
            self.backfill_from,
            self.seed,
        ) {
            (ScopeName::Live, None, seed) => Ok(LiveScope::Live {
                seed: seed.unwrap_or(true),
            }),
            (ScopeName::Live, Some(_), _) => Err(ScopeMismatch::UnexpectedBackfillFrom),
            (ScopeName::LiveBackfill, Some(from), None) => Ok(LiveScope::LiveBackfill { from }),
            (ScopeName::LiveBackfill, Some(_), Some(_)) => Err(ScopeMismatch::UnexpectedSeed),
            (ScopeName::LiveBackfill, None, _) => Err(ScopeMismatch::MissingBackfillFrom),
        }
    }
}

impl From<LiveScope> for ScopeFields {
    fn from(scope: LiveScope) -> Self {
        match scope {
            LiveScope::Live { seed } => Self {
                scope: Some(ScopeName::Live),
                backfill_from: None,
                seed: (!seed).then_some(false),
            },
            LiveScope::LiveBackfill { from } => Self {
                scope: Some(ScopeName::LiveBackfill),
                backfill_from: Some(from),
                seed: None,
            },
        }
    }
}

impl TryFrom<ScopeFields> for LiveScope {
    type Error = ScopeMismatch;

    fn try_from(fields: ScopeFields) -> Result<Self, ScopeMismatch> {
        fields.parse()
    }
}

impl LiveScope {
    /// A live scope, seeded: what [`Default`] gives.
    pub const DEFAULT: Self = Self::live();

    /// [`LiveScope::Live`], seeded.
    #[must_use]
    pub const fn live() -> Self {
        Self::Live { seed: true }
    }

    /// [`LiveScope::Live`] with no latest-value seed.
    #[must_use]
    pub const fn unseeded() -> Self {
        Self::Live { seed: false }
    }

    /// [`LiveScope::LiveBackfill`] from `from`.
    #[must_use]
    pub const fn backfill(from: Timestamp) -> Self {
        Self::LiveBackfill { from }
    }

    /// Build a scope from its name, backfill start and seed choice, by the
    /// wire's rule: a live scope with no seed choice seeds.
    ///
    /// # Errors
    ///
    /// [`ScopeMismatch::MissingBackfillFrom`] for a backfill with no start,
    /// [`ScopeMismatch::UnexpectedBackfillFrom`] for a start without one, and
    /// [`ScopeMismatch::UnexpectedSeed`] for a seed choice on a backfill.
    pub fn from_parts(
        name: ScopeName,
        backfill_from: Option<Timestamp>,
        seed: Option<bool>,
    ) -> Result<Self, ScopeMismatch> {
        ScopeFields {
            scope: Some(name),
            backfill_from,
            seed,
        }
        .parse()
    }

    /// Parse a scope that may be absent: no `scope`, `backfill_from` or `seed`
    /// read as `None`.
    ///
    /// # Errors
    ///
    /// The deserializer's error for an unknown name or a [`ScopeMismatch`].
    pub fn deserialize_optional<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Self>, D::Error> {
        let fields = ScopeFields::deserialize(deserializer)?;
        if fields.scope.is_none() && fields.backfill_from.is_none() && fields.seed.is_none() {
            return Ok(None);
        }
        fields.parse().map(Some).map_err(serde::de::Error::custom)
    }

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
        /// The session's latest-value seed had not settled at the join. A
        /// seeded joiner's first data event is then the seed, unless it
        /// resolves empty or a live print comes first; an unseeded joiner
        /// starts after the seed's slot.
        seed_pending: bool,
    },
}

impl Subscribed {
    /// A creation of a session with this scope.
    #[must_use]
    pub const fn created(scope: LiveScope) -> Self {
        Self::Created(scope)
    }

    /// A join of a session with this scope, with no backfill in flight and
    /// the seed settled.
    #[must_use]
    pub const fn joined(session: LiveScope) -> Self {
        Self::Joined {
            session,
            backfill_in_flight: false,
            seed_pending: false,
        }
    }

    /// This join found a backfill running. No effect on a creation.
    #[must_use]
    pub const fn with_backfill_in_flight(mut self) -> Self {
        if let Self::Joined {
            backfill_in_flight, ..
        } = &mut self
        {
            *backfill_in_flight = true;
        }
        self
    }

    /// This join found the seed not yet settled. No effect on a creation.
    #[must_use]
    pub const fn with_seed_pending(mut self) -> Self {
        if let Self::Joined { seed_pending, .. } = &mut self {
            *seed_pending = true;
        }
        self
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
            serde_json::to_string(&LiveScope::default()).unwrap(),
            r#"{"scope":"live"}"#
        );
        assert_eq!(
            serde_json::to_string(&LiveScope::unseeded()).unwrap(),
            r#"{"scope":"live","seed":false}"#
        );
        let backfill = LiveScope::LiveBackfill { from: Timestamp(9) };
        let json = serde_json::to_string(&backfill).unwrap();
        assert_eq!(json, r#"{"scope":"live_backfill","backfill_from":9}"#);
    }

    #[test]
    fn live_scope_parses_what_it_serializes() {
        for scope in [
            LiveScope::default(),
            LiveScope::unseeded(),
            LiveScope::backfill(Timestamp(9)),
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
    fn from_parts_reads_by_the_wire_rule() {
        use super::{ScopeMismatch, ScopeName};
        assert_eq!(LiveScope::DEFAULT, LiveScope::default());
        assert_eq!(
            LiveScope::from_parts(ScopeName::Live, None, None),
            Ok(LiveScope::live())
        );
        assert_eq!(
            LiveScope::from_parts(ScopeName::Live, None, Some(false)),
            Ok(LiveScope::unseeded())
        );
        assert_eq!(
            LiveScope::from_parts(ScopeName::LiveBackfill, Some(Timestamp(9)), None),
            Ok(LiveScope::backfill(Timestamp(9)))
        );
        assert_eq!(
            LiveScope::from_parts(ScopeName::LiveBackfill, Some(Timestamp(9)), Some(true)),
            Err(ScopeMismatch::UnexpectedSeed)
        );
    }

    #[test]
    fn a_join_says_what_it_found_and_a_creation_finds_nothing() {
        let live = LiveScope::default();
        let found = |backfill_in_flight, seed_pending| Subscribed::Joined {
            session: live,
            backfill_in_flight,
            seed_pending,
        };
        let joined = Subscribed::joined(live);
        assert_eq!(joined, found(false, false));
        assert_eq!(joined.with_backfill_in_flight(), found(true, false));
        assert_eq!(joined.with_seed_pending(), found(false, true));
        assert_eq!(
            joined.with_seed_pending().with_backfill_in_flight(),
            found(true, true)
        );
        let created = Subscribed::created(live);
        assert_eq!(
            created.with_backfill_in_flight().with_seed_pending(),
            created
        );
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
        let joined = Subscribed::joined(backfill);
        assert_eq!(joined.history_from(), None);
        assert_eq!(joined.session_scope(), backfill);
    }
}
