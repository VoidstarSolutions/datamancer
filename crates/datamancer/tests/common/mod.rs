//! Helpers shared by the integration tests.

use datamancer::{LiveScope, Subscribed};

/// Whether `outcome` joined a session created with `session`, with that
/// backfill state. The seed state is not compared: it races the provider's
/// `latest`.
pub fn joined_as(outcome: Subscribed, session: LiveScope, backfill_in_flight: bool) -> bool {
    matches!(outcome, Subscribed::Joined { backfill_in_flight: b, .. } if b == backfill_in_flight)
        && outcome.session_scope() == session
}
