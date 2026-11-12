//! Request/reply framings per control surface. One vocabulary
//! ([`crate::spec`], [`crate::codes`]), two framings: newline-JSON over UDS
//! and correlated JSON frames over WS.

pub mod uds;
pub mod ws;

use datamancer_core::{LiveScope, Subscribed};
use serde::{Deserialize, Serialize};

/// The fields a `subscribe` reply adds to a control reply, in both framings.
/// A reply with no `scope` reads as pure-live; a `scope` that disagrees with
/// `backfill_from`, or is not one this client knows, fails to parse.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SubscribedFields {
    #[serde(flatten, deserialize_with = "LiveScope::deserialize_optional")]
    scope: Option<LiveScope>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    joined: bool,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    backfill_in_flight: bool,
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
        }
    }
}

impl From<&SubscribedFields> for Subscribed {
    fn from(fields: &SubscribedFields) -> Self {
        let session = fields.scope.unwrap_or_default();
        if fields.joined {
            Self::joined(session, fields.backfill_in_flight)
        } else {
            Self::created(session)
        }
    }
}
