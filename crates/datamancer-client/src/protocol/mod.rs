//! Request/reply framings per control surface. One vocabulary
//! ([`crate::spec`], [`crate::codes`]), two framings: newline-JSON over UDS
//! and correlated JSON frames over WS.

pub mod uds;
pub mod ws;

use datamancer_core::{LiveScope, Subscribed};
use serde::{Deserialize, Serialize};

/// The fields a `subscribe` reply adds to a control reply, in both framings.
/// A reply with no `scope` and no `seed` reads as [`LiveScope::default`], and
/// one with no `seed` under `live` as seeded; a `seed` with no `scope`, a
/// `scope` that disagrees with `backfill_from` or `seed`, or one this client
/// does not know, fails to parse.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SubscribedFields {
    #[serde(
        flatten,
        deserialize_with = "crate::spec::optional_live_scope_from_wire"
    )]
    scope: Option<LiveScope>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    joined: bool,
}

impl From<Subscribed> for SubscribedFields {
    fn from(subscribed: Subscribed) -> Self {
        Self {
            scope: Some(subscribed.session_scope()),
            joined: matches!(subscribed, Subscribed::Joined { .. }),
        }
    }
}

impl From<SubscribedFields> for Subscribed {
    fn from(fields: SubscribedFields) -> Self {
        let session = fields.scope.unwrap_or_default();
        if fields.joined {
            Self::Joined { session }
        } else {
            Self::Created(session)
        }
    }
}
