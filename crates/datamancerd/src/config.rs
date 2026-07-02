//! Daemon configuration (TOML).
//!
//! The config is the daemon's primary operator contract. It selects providers
//! (by `account_type`; credentials resolve from the environment via
//! `oxidized_alpaca`, never the file), the persistence backends, session knobs,
//! the control socket, diagnostics cadence, the optional web UI, and an
//! optional set of boot-time `[[startup_session]]` anchors.
//!
//! `Config::load` reads and parses; `Config::validate` enforces cross-section
//! invariants (a startup session needing the cache requires a `[cache]`
//! section, etc.); `Config::into_datamancer` builds the live [`Datamancer`].

use std::path::{Path, PathBuf};

use datamancer::providers::AccountType;
use datamancer::{
    Adjustment, AssetClass, Datamancer, EventKind, Instrument, PersistenceOptions, ProviderId,
    Scope, Timestamp,
    providers::{
        AlpacaCryptoProvider, AlpacaCryptoProviderConfig, AlpacaCryptoVenue, AlpacaProvider,
        AlpacaProviderConfig,
    },
    storage::{SurrealCache, SurrealCacheConfig, SurrealTapLog, SurrealTapLogConfig},
};
use serde::{Deserialize, Serialize};

use crate::error::{DaemonError, Result};

/// Top-level daemon configuration.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Provider selection. At least one provider must be configured.
    pub provider: ProviderConfig,
    /// Historical-cache backend (optional unless a session uses the cache).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache: Option<StorageConfig>,
    /// Tap-log backend (optional unless a session writes the tap log).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tap_log: Option<StorageConfig>,
    /// Session knobs (resume buffer, adjustment).
    #[serde(default)]
    pub session: SessionConfig,
    /// Control surface + service-naming knobs.
    #[serde(default)]
    pub server: ServerConfig,
    /// Diagnostics-plane cadence.
    #[serde(default)]
    pub diagnostics: DiagnosticsConfig,
    /// iceoryx2 transport caps.
    #[serde(default)]
    pub iceoryx2: Iceoryx2Config,
    /// Optional web UI (Phase 6 drives it; config surface lives here). Always
    /// parsed so configs stay portable; only read by the `web-ui` feature.
    #[cfg_attr(not(feature = "web-ui"), allow(dead_code))]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub web_ui: Option<WebUiConfig>,
    /// Boot-time authoritative sessions held as lifecycle anchors.
    #[serde(default)]
    pub startup_session: Vec<StartupSession>,
}

/// Provider selection block. Each provider is optional, but at least one must
/// be present (enforced in [`Config::validate`]).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderConfig {
    #[serde(default)]
    pub alpaca: Option<AlpacaSection>,
    #[serde(default)]
    pub alpaca_crypto: Option<AlpacaCryptoSection>,
}

/// Alpaca equities provider section.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlpacaSection {
    #[serde(default)]
    pub account_type: AccountTypeCfg,
}

/// Alpaca crypto provider section.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlpacaCryptoSection {
    #[serde(default)]
    pub account_type: AccountTypeCfg,
    #[serde(default)]
    pub venue: CryptoVenueCfg,
}

/// Which environment credential pair `oxidized_alpaca` loads.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountTypeCfg {
    #[default]
    Paper,
    Live,
}

impl From<AccountTypeCfg> for AccountType {
    fn from(value: AccountTypeCfg) -> Self {
        match value {
            AccountTypeCfg::Paper => AccountType::Paper,
            AccountTypeCfg::Live => AccountType::Live,
        }
    }
}

/// Alpaca crypto venue selector.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CryptoVenueCfg {
    #[default]
    Us,
    UsKraken,
    EuKraken,
}

impl From<CryptoVenueCfg> for AlpacaCryptoVenue {
    fn from(value: CryptoVenueCfg) -> Self {
        match value {
            CryptoVenueCfg::Us => AlpacaCryptoVenue::Us,
            CryptoVenueCfg::UsKraken => AlpacaCryptoVenue::UsKraken,
            CryptoVenueCfg::EuKraken => AlpacaCryptoVenue::EuKraken,
        }
    }
}

/// A persistence backend (cache or tap log).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    pub backend: StorageBackend,
    /// Filesystem path for embedded backends; ignored for `surreal-memory`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<PathBuf>,
}

/// Supported storage backends.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum StorageBackend {
    SurrealEmbedded,
    SurrealMemory,
}

/// Session-level knobs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionConfig {
    #[serde(default = "default_resume_buffer")]
    pub resume_buffer_events: usize,
    #[serde(default)]
    pub adjustment: AdjustmentCfg,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            resume_buffer_events: default_resume_buffer(),
            adjustment: AdjustmentCfg::default(),
        }
    }
}

const fn default_resume_buffer() -> usize {
    65_536
}

/// Corporate-action adjustment mode.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdjustmentCfg {
    Raw,
    Split,
    Dividend,
    SpinOff,
    #[default]
    All,
}

impl From<AdjustmentCfg> for Adjustment {
    fn from(value: AdjustmentCfg) -> Self {
        match value {
            AdjustmentCfg::Raw => Adjustment::Raw,
            AdjustmentCfg::Split => Adjustment::Split,
            AdjustmentCfg::Dividend => Adjustment::Dividend,
            AdjustmentCfg::SpinOff => Adjustment::SpinOff,
            AdjustmentCfg::All => Adjustment::All,
        }
    }
}

/// Control surface + iceoryx2 naming.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default = "default_admin_socket")]
    pub admin_socket: PathBuf,
    #[serde(default = "default_service_prefix")]
    pub service_prefix: String,
    #[serde(default = "default_shutdown_timeout")]
    pub shutdown_timeout_secs: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            admin_socket: default_admin_socket(),
            service_prefix: default_service_prefix(),
            shutdown_timeout_secs: default_shutdown_timeout(),
        }
    }
}

fn default_admin_socket() -> PathBuf {
    PathBuf::from("/run/datamancerd/admin.sock")
}

fn default_service_prefix() -> String {
    "datamancerd".to_string()
}

const fn default_shutdown_timeout() -> u64 {
    30
}

/// Diagnostics-plane cadence.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticsConfig {
    #[serde(default = "default_live_cadence")]
    pub publish_interval_ms: u64,
    #[serde(default = "default_catalog_cadence")]
    pub cache_catalog_interval_ms: u64,
}

impl Default for DiagnosticsConfig {
    fn default() -> Self {
        Self {
            publish_interval_ms: default_live_cadence(),
            cache_catalog_interval_ms: default_catalog_cadence(),
        }
    }
}

const fn default_live_cadence() -> u64 {
    1000
}

const fn default_catalog_cadence() -> u64 {
    30_000
}

/// iceoryx2 transport caps. The per-client data-plane service is fixed-size at
/// creation; `max_clients` bounds how many per-client services the daemon will
/// create before rejecting `open-client` with a service-cap error.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Iceoryx2Config {
    #[serde(default = "default_max_clients")]
    pub max_clients: usize,
}

impl Default for Iceoryx2Config {
    fn default() -> Self {
        Self {
            max_clients: default_max_clients(),
        }
    }
}

const fn default_max_clients() -> usize {
    64
}

/// Optional web UI (Phase 6).
#[cfg_attr(not(feature = "web-ui"), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebUiConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_web_bind")]
    pub bind: String,
    #[serde(default = "default_web_port")]
    pub port: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assets_dir: Option<PathBuf>,
    #[serde(default = "default_live_cadence")]
    pub live_state_cadence_ms: u64,
    #[serde(default = "default_catalog_cadence")]
    pub cache_catalog_cadence_ms: u64,
}

fn default_web_bind() -> String {
    "127.0.0.1".to_string()
}

const fn default_web_port() -> u16 {
    8080
}

/// A boot-time authoritative session held as a lifecycle anchor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StartupSession {
    pub provider: String,
    pub asset_class: AssetClassCfg,
    pub symbol: String,
    pub kind: EventKindCfg,
    #[serde(default)]
    pub scope: ScopeCfg,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backfill_from: Option<String>,
    #[serde(default)]
    pub persistence: PersistenceCfg,
    /// `true` holds the anchor for the process lifetime regardless of clients;
    /// `false` (default) is refcount-driven warmth.
    #[serde(default)]
    pub always_on: bool,
}

/// Asset class selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AssetClassCfg {
    Equity,
    Crypto,
}

impl From<AssetClassCfg> for AssetClass {
    fn from(value: AssetClassCfg) -> Self {
        match value {
            AssetClassCfg::Equity => AssetClass::Equity,
            AssetClassCfg::Crypto => AssetClass::Crypto,
        }
    }
}

/// Event-kind selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKindCfg {
    Trade,
    Quote,
    Bar1s,
    Bar1m,
    Bar5m,
    Bar15m,
    Bar1h,
    Bar1d,
}

impl From<EventKindCfg> for EventKind {
    fn from(value: EventKindCfg) -> Self {
        use datamancer::BarInterval;
        match value {
            EventKindCfg::Trade => EventKind::Trade,
            EventKindCfg::Quote => EventKind::Quote,
            EventKindCfg::Bar1s => EventKind::Bar(BarInterval::OneSecond),
            EventKindCfg::Bar1m => EventKind::Bar(BarInterval::OneMinute),
            EventKindCfg::Bar5m => EventKind::Bar(BarInterval::FiveMinute),
            EventKindCfg::Bar15m => EventKind::Bar(BarInterval::FifteenMinute),
            EventKindCfg::Bar1h => EventKind::Bar(BarInterval::OneHour),
            EventKindCfg::Bar1d => EventKind::Bar(BarInterval::OneDay),
        }
    }
}

/// Startup-session scope selector.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeCfg {
    #[default]
    Live,
    LiveBackfill,
}

/// Persistence-preset selector for a startup session.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PersistenceCfg {
    #[default]
    None,
    Cached,
    CachedWithTap,
    ReadOnly,
    Refresh,
    TapOnly,
}

impl PersistenceCfg {
    /// Map the preset name to the library [`PersistenceOptions`].
    #[must_use]
    pub fn options(self) -> PersistenceOptions {
        match self {
            PersistenceCfg::None => PersistenceOptions::none(),
            PersistenceCfg::Cached => PersistenceOptions::cached(),
            PersistenceCfg::CachedWithTap => PersistenceOptions::cached().with_tap_log(true),
            PersistenceCfg::ReadOnly => PersistenceOptions::read_only(),
            PersistenceCfg::Refresh => PersistenceOptions::refresh(),
            PersistenceCfg::TapOnly => PersistenceOptions::none().with_tap_log(true),
        }
    }
}

impl StartupSession {
    /// The resolved [`Instrument`] for this anchor.
    #[must_use]
    pub fn instrument(&self) -> Instrument {
        Instrument::new(
            ProviderId::new(self.provider.clone()),
            self.asset_class.into(),
            self.symbol.clone(),
        )
    }

    /// Resolve the [`Scope`], rejecting `live_backfill` without `backfill_from`
    /// and an unparseable timestamp.
    ///
    /// # Errors
    ///
    /// [`DaemonError::ConfigInvalid`] when `live_backfill` lacks a valid
    /// `backfill_from`.
    pub fn resolve_scope(&self) -> Result<Scope> {
        match self.scope {
            ScopeCfg::Live => Ok(Scope::Live {
                backfill_from: None,
            }),
            ScopeCfg::LiveBackfill => {
                let raw = self.backfill_from.as_deref().ok_or_else(|| {
                    DaemonError::ConfigInvalid(format!(
                        "startup_session {}/{} uses scope=live_backfill but has no backfill_from",
                        self.symbol, self.provider
                    ))
                })?;
                let ts = parse_rfc3339_nanos(raw).ok_or_else(|| {
                    DaemonError::ConfigInvalid(format!(
                        "startup_session {} backfill_from {raw:?} is not an RFC3339 timestamp",
                        self.symbol
                    ))
                })?;
                Ok(Scope::Live {
                    backfill_from: Some(ts),
                })
            }
        }
    }
}

/// Parse an RFC3339 timestamp into epoch nanoseconds. Returns `None` on any
/// parse failure. Deliberately tiny (no `chrono` dep in the binary): accepts
/// the `YYYY-MM-DDTHH:MM:SS[.frac]Z` form the config schema documents.
fn parse_rfc3339_nanos(s: &str) -> Option<Timestamp> {
    // Defer to the library's own parser surface is unavailable; do a minimal
    // parse of the documented `...Z` form via a fixed-offset epoch computation.
    let s = s.strip_suffix('Z')?;
    let (date, time) = s.split_once('T')?;
    let mut date_parts = date.split('-');
    let year: i64 = date_parts.next()?.parse().ok()?;
    let month: i64 = date_parts.next()?.parse().ok()?;
    let day: i64 = date_parts.next()?.parse().ok()?;
    if date_parts.next().is_some() {
        return None;
    }
    let (hms, frac) = match time.split_once('.') {
        Some((hms, frac)) => (hms, Some(frac)),
        None => (time, None),
    };
    let mut t_parts = hms.split(':');
    let hour: i64 = t_parts.next()?.parse().ok()?;
    let minute: i64 = t_parts.next()?.parse().ok()?;
    let second: i64 = t_parts.next()?.parse().ok()?;
    if t_parts.next().is_some() {
        return None;
    }
    // Reject impossible clock components (e.g. `99:00:00`); a normalized-but-wrong
    // instant must not pass validation. (Leap second `:60` is not supported.)
    if !(0..=23).contains(&hour) || !(0..=59).contains(&minute) || !(0..=59).contains(&second) {
        return None;
    }
    let nanos_frac: i64 = match frac {
        Some(f) => {
            if f.is_empty() || f.len() > 9 || !f.bytes().all(|b| b.is_ascii_digit()) {
                return None;
            }
            let padded = format!("{f:0<9}");
            padded.parse().ok()?
        }
        None => 0,
    };
    let days = days_from_civil(year, month, day)?;
    let secs = days * 86_400 + hour * 3_600 + minute * 60 + second;
    Some(Timestamp(secs * 1_000_000_000 + nanos_frac))
}

/// Days since the Unix epoch for a civil (proleptic Gregorian) date. Returns
/// `None` for out-of-range month/day (per-month max, leap-year aware). Algorithm
/// from Howard Hinnant's `days_from_civil`.
fn days_from_civil(year: i64, month: i64, day: i64) -> Option<i64> {
    if !(1..=12).contains(&month) {
        return None;
    }
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if leap => 29,
        2 => 28,
        _ => return None,
    };
    if !(1..=max_day).contains(&day) {
        return None;
    }
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    Some(era * 146_097 + doe - 719_468)
}

impl Config {
    /// Read and parse a TOML config file, then validate it.
    ///
    /// # Errors
    ///
    /// - [`DaemonError::ConfigRead`] if the file cannot be read.
    /// - [`DaemonError::ConfigParse`] if it is not valid TOML.
    /// - [`DaemonError::ConfigInvalid`] if it fails validation.
    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|source| DaemonError::ConfigRead {
            path: path.to_path_buf(),
            source,
        })?;
        let config = Self::parse(&text)?;
        config.validate()?;
        Ok(config)
    }

    /// Parse a TOML string into a [`Config`] (no validation).
    ///
    /// # Errors
    ///
    /// [`DaemonError::ConfigParse`] if the text is not valid TOML.
    pub fn parse(text: &str) -> Result<Self> {
        Ok(toml::from_str(text)?)
    }

    /// Enforce cross-section invariants.
    ///
    /// # Errors
    ///
    /// [`DaemonError::ConfigInvalid`] when a required section is missing for a
    /// configured startup session, or a startup session is malformed.
    pub fn validate(&self) -> Result<()> {
        if self.provider.alpaca.is_none() && self.provider.alpaca_crypto.is_none() {
            return Err(DaemonError::ConfigInvalid(
                "no provider configured: set [provider.alpaca] and/or [provider.alpaca_crypto]"
                    .to_string(),
            ));
        }
        for s in &self.startup_session {
            let options = s.persistence.options();
            if options.uses_cache() && self.cache.is_none() {
                return Err(DaemonError::ConfigInvalid(format!(
                    "startup_session {} uses a cache persistence preset but no [cache] is configured",
                    s.symbol
                )));
            }
            if options.write_tap_log && self.tap_log.is_none() {
                return Err(DaemonError::ConfigInvalid(format!(
                    "startup_session {} writes the tap log but no [tap_log] is configured",
                    s.symbol
                )));
            }
            // Surface a scope/backfill mismatch at validate time.
            s.resolve_scope()?;
        }
        Ok(())
    }

    /// Build the full daemon runtime: construct the configured providers, open
    /// the cache + tap log, assemble the [`Datamancer`], and retain the tap-log
    /// `Arc` so the shutdown path can flush it (the builder takes ownership, so
    /// the handle must be cloned out here).
    ///
    /// # Errors
    ///
    /// Propagates storage-open and builder errors as [`DaemonError`].
    pub async fn build_runtime(self) -> Result<BuiltRuntime> {
        let mut builder = Datamancer::builder()
            .resume_buffer_events(self.session.resume_buffer_events)
            .adjustment(self.session.adjustment.into());

        if let Some(alpaca) = self.provider.alpaca {
            let provider = AlpacaProvider::new(AlpacaProviderConfig {
                account_type: alpaca.account_type.into(),
                ..Default::default()
            });
            builder = builder.provider(Box::new(provider));
        }
        if let Some(crypto) = self.provider.alpaca_crypto {
            let provider = AlpacaCryptoProvider::new(AlpacaCryptoProviderConfig {
                account_type: crypto.account_type.into(),
                venue: crypto.venue.into(),
                ..Default::default()
            });
            builder = builder.provider(Box::new(provider));
        }

        if let Some(cache_cfg) = &self.cache {
            let cache = SurrealCache::open(storage_to_cache_config(cache_cfg)?).await?;
            builder = builder.historical_cache(Box::new(cache));
        }
        let mut tap_log: Option<std::sync::Arc<dyn datamancer::TapLog>> = None;
        if let Some(tap_cfg) = &self.tap_log {
            let tap = SurrealTapLog::open(storage_to_tap_config(tap_cfg)?).await?;
            let tap: std::sync::Arc<dyn datamancer::TapLog> = std::sync::Arc::new(tap);
            builder = builder.tap_log_arc(tap.clone());
            tap_log = Some(tap);
        }

        Ok(BuiltRuntime {
            datamancer: builder.build()?,
            tap_log,
        })
    }
}

/// The assembled daemon runtime: the [`Datamancer`] plus the retained tap-log
/// handle (for the shutdown flush).
pub struct BuiltRuntime {
    pub datamancer: Datamancer,
    pub tap_log: Option<std::sync::Arc<dyn datamancer::TapLog>>,
}

fn storage_to_cache_config(cfg: &StorageConfig) -> Result<SurrealCacheConfig> {
    match cfg.backend {
        StorageBackend::SurrealMemory => Ok(SurrealCacheConfig::Memory),
        StorageBackend::SurrealEmbedded => {
            let path = cfg.path.as_ref().ok_or_else(|| {
                DaemonError::ConfigInvalid("[cache] surreal-embedded requires `path`".to_string())
            })?;
            Ok(SurrealCacheConfig::embedded(path))
        }
    }
}

fn storage_to_tap_config(cfg: &StorageConfig) -> Result<SurrealTapLogConfig> {
    match cfg.backend {
        StorageBackend::SurrealMemory => Ok(SurrealTapLogConfig::Memory),
        StorageBackend::SurrealEmbedded => {
            let path = cfg.path.as_ref().ok_or_else(|| {
                DaemonError::ConfigInvalid("[tap_log] surreal-embedded requires `path`".to_string())
            })?;
            Ok(SurrealTapLogConfig::embedded(path))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datamancer::BarInterval;

    const MINIMAL: &str = r#"
[provider.alpaca]
account_type = "paper"
"#;

    #[test]
    fn config_parses_minimal_toml() {
        let config = Config::parse(MINIMAL).expect("parse");
        config.validate().expect("validate");
        assert!(config.provider.alpaca.is_some());
        assert!(config.provider.alpaca_crypto.is_none());
        // Defaults apply.
        assert_eq!(config.session.resume_buffer_events, 65_536);
        assert_eq!(config.server.service_prefix, "datamancerd");
        assert_eq!(config.server.shutdown_timeout_secs, 30);
        assert_eq!(config.diagnostics.publish_interval_ms, 1000);
    }

    #[test]
    fn config_rejects_no_provider() {
        let config = Config::parse("[provider]\n").expect("parse");
        let err = config.validate().expect_err("must reject");
        assert!(matches!(err, DaemonError::ConfigInvalid(_)));
    }

    #[test]
    fn config_rejects_cache_session_without_cache() {
        let text = r#"
[provider.alpaca_crypto]
account_type = "paper"
venue = "us"

[[startup_session]]
provider = "alpaca-crypto"
asset_class = "crypto"
symbol = "BTC/USD"
kind = "trade"
scope = "live"
persistence = "cached"
"#;
        let config = Config::parse(text).expect("parse");
        let err = config.validate().expect_err("must reject");
        match err {
            DaemonError::ConfigInvalid(m) => assert!(m.contains("cache"), "{m}"),
            other => panic!("wrong error: {other:?}"),
        }
    }

    #[test]
    fn config_rejects_taplog_session_without_taplog() {
        let text = r#"
[provider.alpaca_crypto]
account_type = "paper"

[[startup_session]]
provider = "alpaca-crypto"
asset_class = "crypto"
symbol = "BTC/USD"
kind = "trade"
persistence = "tap_only"
"#;
        let config = Config::parse(text).expect("parse");
        let err = config.validate().expect_err("must reject");
        match err {
            DaemonError::ConfigInvalid(m) => assert!(m.contains("tap log"), "{m}"),
            other => panic!("wrong error: {other:?}"),
        }
    }

    #[test]
    fn config_rejects_live_backfill_without_from() {
        let text = r#"
[provider.alpaca_crypto]
account_type = "paper"

[[startup_session]]
provider = "alpaca-crypto"
asset_class = "crypto"
symbol = "BTC/USD"
kind = "trade"
scope = "live_backfill"
"#;
        let config = Config::parse(text).expect("parse");
        let err = config.validate().expect_err("must reject");
        match err {
            DaemonError::ConfigInvalid(m) => assert!(m.contains("backfill_from"), "{m}"),
            other => panic!("wrong error: {other:?}"),
        }
    }

    #[test]
    fn persistence_preset_maps() {
        assert_eq!(PersistenceCfg::None.options(), PersistenceOptions::none());
        assert_eq!(
            PersistenceCfg::Cached.options(),
            PersistenceOptions::cached()
        );
        assert_eq!(
            PersistenceCfg::CachedWithTap.options(),
            PersistenceOptions::cached().with_tap_log(true)
        );
        assert_eq!(
            PersistenceCfg::ReadOnly.options(),
            PersistenceOptions::read_only()
        );
        assert_eq!(
            PersistenceCfg::Refresh.options(),
            PersistenceOptions::refresh()
        );
        assert_eq!(
            PersistenceCfg::TapOnly.options(),
            PersistenceOptions::none().with_tap_log(true)
        );
    }

    #[test]
    fn event_kind_maps() {
        assert_eq!(EventKind::from(EventKindCfg::Trade), EventKind::Trade);
        assert_eq!(EventKind::from(EventKindCfg::Quote), EventKind::Quote);
        assert_eq!(
            EventKind::from(EventKindCfg::Bar1m),
            EventKind::Bar(BarInterval::OneMinute)
        );
        assert_eq!(
            EventKind::from(EventKindCfg::Bar1d),
            EventKind::Bar(BarInterval::OneDay)
        );
    }

    #[test]
    fn startup_session_resolves_scope_and_instrument() {
        let text = r#"
[provider.alpaca_crypto]
account_type = "paper"

[[startup_session]]
provider = "alpaca-crypto"
asset_class = "crypto"
symbol = "BTC/USD"
kind = "trade"
scope = "live_backfill"
backfill_from = "2026-06-01T00:00:00Z"
persistence = "none"
"#;
        let config = Config::parse(text).expect("parse");
        config.validate().expect("validate");
        let s = &config.startup_session[0];
        let inst = s.instrument();
        assert_eq!(inst.symbol(), "BTC/USD");
        assert_eq!(inst.provider().as_str(), "alpaca-crypto");
        let scope = s.resolve_scope().expect("scope");
        match scope {
            Scope::Live {
                backfill_from: Some(ts),
            } => {
                // 2026-06-01T00:00:00Z in epoch nanos.
                assert_eq!(ts.0, 1_780_272_000 * 1_000_000_000);
            }
            other => panic!("wrong scope: {other:?}"),
        }
    }

    #[test]
    fn rfc3339_parser_matches_known_epoch() {
        // 1970-01-01T00:00:00Z == 0
        assert_eq!(
            parse_rfc3339_nanos("1970-01-01T00:00:00Z"),
            Some(Timestamp(0))
        );
        // 2000-01-01T00:00:00Z == 946684800 s
        assert_eq!(
            parse_rfc3339_nanos("2000-01-01T00:00:00Z"),
            Some(Timestamp(946_684_800 * 1_000_000_000))
        );
        // fractional seconds
        assert_eq!(
            parse_rfc3339_nanos("1970-01-01T00:00:00.5Z"),
            Some(Timestamp(500_000_000))
        );
        assert_eq!(parse_rfc3339_nanos("not-a-date"), None);
        assert_eq!(parse_rfc3339_nanos("1970-01-01 00:00:00Z"), None);
        // Impossible components are rejected, not silently normalized.
        assert_eq!(parse_rfc3339_nanos("2026-02-31T00:00:00Z"), None); // Feb 31
        assert_eq!(parse_rfc3339_nanos("2026-13-01T00:00:00Z"), None); // month 13
        assert_eq!(parse_rfc3339_nanos("2026-06-01T99:00:00Z"), None); // hour 99
        assert_eq!(parse_rfc3339_nanos("2026-06-01T00:60:00Z"), None); // minute 60
        assert_eq!(parse_rfc3339_nanos("2025-02-29T00:00:00Z"), None); // not a leap year
        assert!(parse_rfc3339_nanos("2024-02-29T00:00:00Z").is_some()); // leap year
    }

    #[test]
    fn unknown_field_is_rejected() {
        let text = r#"
[provider.alpaca]
account_type = "paper"
bogus = true
"#;
        assert!(Config::parse(text).is_err());
    }

    const FULL: &str = r#"
[provider.alpaca]
account_type = "paper"

[provider.alpaca_crypto]
account_type = "live"
venue = "us_kraken"

[cache]
backend = "surreal-embedded"
path = "/tmp/dmc-cache"

[tap_log]
backend = "surreal-memory"

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
bind = "127.0.0.1"
port = 8091

[[startup_session]]
provider = "alpaca-crypto"
asset_class = "crypto"
symbol = "BTC/USD"
kind = "trade"
scope = "live_backfill"
backfill_from = "2026-06-01T00:00:00Z"
persistence = "cached_with_tap"
always_on = true
"#;

    #[test]
    fn config_round_trips_through_toml() {
        let config = Config::parse(FULL).expect("parse");
        config.validate().expect("validate");
        let text = toml::to_string_pretty(&config).expect("serialize");
        let back = Config::parse(&text).expect("reparse");
        assert_eq!(config, back);
    }

    #[test]
    fn minimal_config_round_trips_without_none_fields() {
        // `None` options must be skipped, not serialized (TOML has no null).
        let config = Config::parse(MINIMAL).expect("parse");
        let text = toml::to_string_pretty(&config).expect("serialize");
        assert!(!text.contains("[cache]"), "absent [cache] must not serialize: {text}");
        assert!(!text.contains("[tap_log]"), "absent [tap_log] must not serialize: {text}");
        assert!(!text.contains("[web_ui]"), "absent [web_ui] must not serialize: {text}");
        let back = Config::parse(&text).expect("reparse");
        assert_eq!(config, back);
    }
}
