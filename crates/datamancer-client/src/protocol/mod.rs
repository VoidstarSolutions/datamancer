//! Request/reply framings per control surface. One vocabulary
//! ([`crate::spec`], [`crate::codes`]), two framings: newline-JSON over UDS
//! and correlated JSON frames over WS.

pub mod uds;
pub mod ws;

use datamancer_core::{LiveScope, Subscribed};
use serde::{Deserialize, Serialize};

/// The fields a `subscribe` reply adds to a control reply, in both framings.
/// An absent `scope` means live; the fields must agree.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SubscribedFields {
    #[serde(flatten, deserialize_with = "LiveScope::deserialize_optional")]
    scope: Option<LiveScope>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    joined: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    backfill_in_flight: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    seed_pending: bool,
}

impl From<Subscribed> for SubscribedFields {
    fn from(subscribed: Subscribed) -> Self {
        Self {
            scope: Some(subscribed.session_scope()),
            joined: matches!(subscribed, Subscribed::Joined { .. }),
            backfill_in_flight: matches!(
                subscribed,
                Subscribed::Joined {
                    backfill_in_flight: true,
                    ..
                }
            ),
            seed_pending: matches!(
                subscribed,
                Subscribed::Joined {
                    seed_pending: true,
                    ..
                }
            ),
        }
    }
}

impl From<&SubscribedFields> for Subscribed {
    fn from(fields: &SubscribedFields) -> Self {
        let session = fields.scope.unwrap_or_default();
        if !fields.joined {
            return Self::created(session);
        }
        let mut joined = Self::joined(session);
        if fields.backfill_in_flight {
            joined = joined.with_backfill_in_flight();
        }
        if fields.seed_pending {
            joined = joined.with_seed_pending();
        }
        joined
    }
}
