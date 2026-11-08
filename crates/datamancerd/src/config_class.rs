//! The hot/cold classification of every daemon config field — one table
//! shared by the control surface and the web UI (spec cycle 3). "Hot"
//! fields apply to the running daemon when changed through the config
//! service; "cold" fields persist but take effect at the next boot
//! (`restart_required`). The exhaustiveness test below fails when a config
//! field is added without a classification.

use crate::config::Config;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FieldClass {
    /// Applies live via the config hub's provider watches.
    Hot,
    /// Persisted; applied at next boot.
    Cold,
}

/// Classification by longest matching dotted-path prefix. Entries end with
/// `.` to classify a whole section; exact entries classify one leaf.
const CLASSIFICATION: &[(&str, FieldClass)] = &[
    // Provider sections: presence (enable/disable) and every setting apply
    // live through the per-provider settings watch.
    ("provider.", FieldClass::Hot),
    // Everything else is boot-time composition: storage backends, session
    // knobs, sockets/listeners, transport caps, cadences, anchors.
    ("cache.", FieldClass::Cold),
    ("tap_log.", FieldClass::Cold),
    ("session.", FieldClass::Cold),
    ("server.", FieldClass::Cold),
    ("diagnostics.", FieldClass::Cold),
    ("iceoryx2.", FieldClass::Cold),
    ("web_ui.", FieldClass::Cold),
    ("ws.", FieldClass::Cold),
    ("startup_session.", FieldClass::Cold),
    ("log.", FieldClass::Cold),
];

/// The class for a dotted config path, or `None` for an unknown path.
pub(crate) fn classify(path: &str) -> Option<FieldClass> {
    CLASSIFICATION
        .iter()
        .filter(|(prefix, _)| path.starts_with(prefix) || path == prefix.trim_end_matches('.'))
        .max_by_key(|(prefix, _)| prefix.len())
        .map(|&(_, class)| class)
}

/// Every leaf path (dotted) in a config's JSON form. Arrays are treated as
/// one leaf under their section path (element-level diffing adds noise
/// without changing any classification decision).
fn leaf_paths(value: &serde_json::Value, prefix: &str, out: &mut Vec<String>) {
    match value {
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                let path = if prefix.is_empty() {
                    k.clone()
                } else {
                    format!("{prefix}.{k}")
                };
                leaf_paths(v, &path, out);
            }
        }
        _ => out.push(prefix.to_string()),
    }
}

/// What keeps `current` from fully applying without a restart, against
/// `baseline` (the boot-applied config): each Cold-classified leaf that
/// differs, and `startup_session.<provider>.<symbol>.<kind>` for each
/// `always_on` startup session whose provider `baseline` lacks and `current`
/// enables. Sorted, without duplicates; empty when no restart is required.
pub(crate) fn restart_divergence(baseline: &Config, current: &Config) -> Vec<String> {
    let a = serde_json::to_value(baseline).expect("Config serializes");
    let b = serde_json::to_value(current).expect("Config serializes");
    let mut paths = Vec::new();
    leaf_paths(&a, "", &mut paths);
    leaf_paths(&b, "", &mut paths);
    paths.retain(|p| lookup(&a, p) != lookup(&b, p) && classify(p) == Some(FieldClass::Cold));
    let enabled_at_boot = baseline.provider_ids();
    let enabled_now = current.provider_ids();
    paths.extend(
        current
            .startup_session
            .iter()
            .filter(|s| {
                s.always_on
                    && !enabled_at_boot.contains(&s.provider.as_str())
                    && enabled_now.contains(&s.provider.as_str())
            })
            .map(|s| {
                let kind = serde_json::to_value(s.kind).expect("EventKindCfg serializes");
                let kind = kind.as_str().expect("EventKindCfg serializes as a string");
                format!("startup_session.{}.{}.{kind}", s.provider, s.symbol)
            }),
    );
    paths.sort();
    paths.dedup();
    paths
}

fn lookup<'v>(value: &'v serde_json::Value, path: &str) -> Option<&'v serde_json::Value> {
    let mut v = value;
    for seg in path.split('.') {
        v = v.get(seg)?;
    }
    Some(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Keep in sync with config.rs's FULL fixture: every section populated,
    // so every serializable field appears in the exhaustiveness walk.
    // IMPORTANT: Option fields with `skip_serializing_if` MUST be populated
    // here or the exhaustiveness gate cannot see them. This includes:
    // - tap_log.path (StorageConfig field)
    // - web_ui.assets_dir
    // - startup_session.backfill_from (when scope requires it)
    // If a new Option field is added, ensure it's set here.
    const FULL: &str = r#"
[provider.alpaca]
account_type = "paper"

[provider.alpaca_crypto]
account_type = "live"
venue = "us_kraken"

[cache]
backend = "embedded"
path = "/tmp/dmc-cache"

[tap_log]
backend = "embedded"
path = "/tmp/dmc-taplog"

[session]
resume_buffer_events = 1024
adjustment = "split"

[server]
admin_socket = "/tmp/dmc/admin.sock"
service_prefix = "dmc"
shutdown_timeout_secs = 5

[diagnostics]
publish_interval_ms = 500
cache_catalog_interval_ms = 10000

[iceoryx2]
max_clients = 8

[web_ui]
enabled = true
assets_dir = "/tmp/dmc-assets"

[ws]
enabled = true
auth_token = "t"

[log]
level = "debug"
format = "json"

[[startup_session]]
provider = "alpaca-crypto"
asset_class = "crypto"
symbol = "BTC/USD"
kind = "trade"
scope = "live_backfill"
backfill_from = "2026-06-01T00:00:00Z"
persistence = "none"
"#;

    /// Spec cycle 3: "a new field without a classification fails the
    /// build" — every leaf of a fully-populated config must classify.
    #[test]
    fn every_config_field_is_classified() {
        let config = Config::parse(FULL).expect("parse");
        let value = serde_json::to_value(&config).expect("serialize");
        let mut paths = Vec::new();
        leaf_paths(&value, "", &mut paths);
        assert!(!paths.is_empty());
        let unclassified: Vec<_> = paths
            .into_iter()
            .filter(|p| classify(p).is_none())
            .collect();
        assert!(
            unclassified.is_empty(),
            "config fields missing a hot/cold classification: {unclassified:?} — add them to CLASSIFICATION"
        );
    }

    #[test]
    fn provider_fields_are_hot_everything_else_cold() {
        assert_eq!(
            classify("provider.alpaca.account_type"),
            Some(FieldClass::Hot)
        );
        assert_eq!(
            classify("provider.alpaca_crypto.venue"),
            Some(FieldClass::Hot)
        );
        assert_eq!(classify("server.admin_socket"), Some(FieldClass::Cold));
        assert_eq!(
            classify("session.resume_buffer_events"),
            Some(FieldClass::Cold)
        );
        assert_eq!(classify("nonexistent.field"), None);
        assert_eq!(classify("log.format"), Some(FieldClass::Cold));
    }

    #[test]
    fn restart_divergence_ignores_hot_changes_and_flags_cold_ones() {
        let boot = Config::parse(FULL).expect("parse");
        let mut hot_changed = boot.clone();
        hot_changed.provider.alpaca = None; // hot: enable/disable
        assert!(restart_divergence(&boot, &hot_changed).is_empty());

        let mut cold_changed = boot.clone();
        cold_changed.session.resume_buffer_events = 42;
        let diverged = restart_divergence(&boot, &cold_changed);
        assert_eq!(diverged, vec!["session.resume_buffer_events".to_string()]);
    }

    #[test]
    fn enabling_the_provider_of_a_skipped_anchor_needs_a_restart() {
        let anchor = |always_on: bool| {
            format!(
                r#"
[[startup_session]]
provider = "alpaca"
asset_class = "equity"
symbol = "AAPL"
kind = "trade"
always_on = {always_on}
"#
            )
        };
        let boot = Config::parse(&anchor(true)).expect("parse");
        let mut enabled = boot.clone();
        enabled.provider = Config::parse(FULL).expect("parse").provider;
        assert_eq!(
            restart_divergence(&boot, &enabled),
            vec!["startup_session.alpaca.AAPL.trade".to_string()]
        );
        assert!(restart_divergence(&enabled, &enabled).is_empty());

        let refcounted = Config::parse(&anchor(false)).expect("parse");
        let mut enabled = refcounted.clone();
        enabled.provider = Config::parse(FULL).expect("parse").provider;
        assert!(restart_divergence(&refcounted, &enabled).is_empty());
    }

    #[test]
    fn quote_and_trade_anchors_stay_distinct() {
        let boot = Config::parse(
            r#"
[tap_log]
backend = "embedded"
path = "/tmp/dmc-taplog"

[[startup_session]]
provider = "alpaca"
asset_class = "equity"
symbol = "AAPL"
kind = "trade"
always_on = true

[[startup_session]]
provider = "alpaca"
asset_class = "equity"
symbol = "AAPL"
kind = "quote"
always_on = true

[[startup_session]]
provider = "alpaca"
asset_class = "equity"
symbol = "AAPL"
kind = "quote"
scope = "live_backfill"
backfill_from = "2020-01-01T00:00:00Z"
always_on = true
"#,
        )
        .expect("parse");
        let mut enabled = boot.clone();
        enabled.provider = Config::parse(FULL).expect("parse").provider;
        enabled.tap_log.as_mut().expect("tap_log").path = Some("/tmp/elsewhere".into());
        assert_eq!(
            restart_divergence(&boot, &enabled),
            vec![
                "startup_session.alpaca.AAPL.quote".to_string(),
                "startup_session.alpaca.AAPL.trade".to_string(),
                "tap_log.path".to_string(),
            ]
        );
    }
}
