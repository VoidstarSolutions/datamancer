//! The daemon-side config service: the authoritative in-memory [`Config`],
//! the config file, and one settings watch per compiled-in provider.
//! Mutating ops follow the credential hub's discipline — one hub lock
//! serializes validate → **persist** → **apply**, so a store failure leaves
//! the running daemon untouched and concurrent writers can never tear the
//! file or leave the file and the live watches on different values.

use std::path::PathBuf;
use std::sync::Arc;

use datamancer::providers::{
    AlpacaCryptoSettings, AlpacaSettings, SettingsSource, alpaca, alpaca_crypto,
};
use tokio::sync::watch;

use crate::config::{AlpacaCryptoSection, AlpacaSection, Config};
use crate::config_class::cold_divergence;
use crate::control::{Reply, codes};
use crate::error::Result;

/// The per-provider settings sources handed to `build_runtime`.
pub(crate) struct ProviderSettingsSources {
    pub alpaca: SettingsSource<AlpacaSettings>,
    pub alpaca_crypto: SettingsSource<AlpacaCryptoSettings>,
}

struct HubState {
    /// The authoritative current config (starts as the boot config; every
    /// accepted mutation updates it in the same critical section as the
    /// persist + apply).
    #[allow(
        dead_code,
        reason = "read by get_config/apply_full, consumed by the config-service dispatch task"
    )]
    current: Config,
}

pub(crate) struct ConfigHub {
    #[allow(dead_code, reason = "consumed by the config-service dispatch task")]
    path: PathBuf,
    /// Boot-applied config: `cold_divergence(&boot, &current)` non-empty ⇒
    /// restart required.
    #[allow(dead_code, reason = "consumed by the config-service dispatch task")]
    boot: Config,
    #[allow(dead_code, reason = "consumed by the config-service dispatch task")]
    state: tokio::sync::Mutex<HubState>,
    #[allow(dead_code, reason = "consumed by the config-service dispatch task")]
    alpaca_tx: watch::Sender<Option<AlpacaSettings>>,
    #[allow(dead_code, reason = "consumed by the config-service dispatch task")]
    alpaca_crypto_tx: watch::Sender<Option<AlpacaCryptoSettings>>,
}

fn alpaca_settings(section: &AlpacaSection) -> AlpacaSettings {
    AlpacaSettings {
        account_type: section.account_type.into(),
    }
}

fn alpaca_crypto_settings(section: &AlpacaCryptoSection) -> AlpacaCryptoSettings {
    AlpacaCryptoSettings {
        account_type: section.account_type.into(),
        venue: section.venue.into(),
    }
}

impl ConfigHub {
    /// Seed one settings watch per compiled-in provider from the boot
    /// config (section present ⇒ enabled, absent ⇒ disabled) and return the
    /// sources for `build_runtime`.
    pub(crate) fn bootstrap(config: Config, path: PathBuf) -> (Arc<Self>, ProviderSettingsSources) {
        let (alpaca_tx, alpaca_rx) =
            watch::channel(config.provider.alpaca.as_ref().map(alpaca_settings));
        let (crypto_tx, crypto_rx) = watch::channel(
            config
                .provider
                .alpaca_crypto
                .as_ref()
                .map(alpaca_crypto_settings),
        );
        let hub = Self {
            path,
            boot: config.clone(),
            state: tokio::sync::Mutex::new(HubState { current: config }),
            alpaca_tx,
            alpaca_crypto_tx: crypto_tx,
        };
        (
            Arc::new(hub),
            ProviderSettingsSources {
                alpaca: SettingsSource::Watch(alpaca_rx),
                alpaca_crypto: SettingsSource::Watch(crypto_rx),
            },
        )
    }

    /// The current config as JSON plus the cold-field divergence flag.
    #[allow(dead_code, reason = "consumed by the config-service dispatch task")]
    pub(crate) async fn get_config(&self) -> Reply {
        let state = self.state.lock().await;
        let restart_required = !cold_divergence(&self.boot, &state.current).is_empty();
        match serde_json::to_value(&state.current) {
            Ok(value) => Reply::config(value, restart_required),
            Err(e) => Reply::error(codes::INTERNAL, format!("config serialize: {e}")),
        }
    }

    /// Enable or re-configure a provider: validate → persist → apply.
    #[allow(dead_code, reason = "consumed by the config-service dispatch task")]
    pub(crate) async fn configure_provider(
        &self,
        provider: &str,
        settings: serde_json::Value,
    ) -> Reply {
        // An omitted/null settings payload means "defaults".
        let settings = if settings.is_null() {
            serde_json::Value::Object(serde_json::Map::new())
        } else {
            settings
        };
        let mut state = self.state.lock().await;
        let mut candidate = state.current.clone();
        match provider {
            alpaca::PROVIDER_ID => match serde_json::from_value::<AlpacaSection>(settings) {
                Ok(section) => candidate.provider.alpaca = Some(section),
                Err(e) => return settings_error(&e),
            },
            alpaca_crypto::PROVIDER_ID => {
                match serde_json::from_value::<AlpacaCryptoSection>(settings) {
                    Ok(section) => candidate.provider.alpaca_crypto = Some(section),
                    Err(e) => return settings_error(&e),
                }
            }
            _ => return unknown_provider(provider),
        }
        match self.persist(&mut state, candidate).await {
            Ok(()) => {
                self.apply(&state.current);
                tracing::info!(provider, "provider configured and hot-applied");
                Reply::applied_live()
            }
            Err(reply) => reply,
        }
    }

    /// Disable a provider (remove its section; stored credentials are
    /// untouched — re-enabling reuses them).
    #[allow(dead_code, reason = "consumed by the config-service dispatch task")]
    pub(crate) async fn remove_provider(&self, provider: &str) -> Reply {
        let mut state = self.state.lock().await;
        let mut candidate = state.current.clone();
        match provider {
            alpaca::PROVIDER_ID => candidate.provider.alpaca = None,
            alpaca_crypto::PROVIDER_ID => candidate.provider.alpaca_crypto = None,
            _ => return unknown_provider(provider),
        }
        match self.persist(&mut state, candidate).await {
            Ok(()) => {
                self.apply(&state.current);
                tracing::info!(provider, "provider disabled");
                Reply::applied_live()
            }
            Err(reply) => reply,
        }
    }

    /// Replace the whole config (the web UI's PUT path). Hot provider
    /// changes apply live; returns whether cold fields now diverge from the
    /// boot config.
    #[allow(dead_code, reason = "consumed by the web-ui config delegation task")]
    pub(crate) async fn apply_full(&self, new: Config) -> Result<bool> {
        let mut state = self.state.lock().await;
        let path = self.path.clone();
        let candidate = new.clone();
        tokio::task::spawn_blocking(move || candidate.save(&path))
            .await
            .map_err(|e| {
                crate::error::DaemonError::ConfigInvalid(format!("config task failed: {e}"))
            })??;
        state.current = new;
        self.apply(&state.current);
        Ok(!cold_divergence(&self.boot, &state.current).is_empty())
    }

    /// Validate + atomically persist `candidate`; commit it to `state`
    /// only on success. Persist-then-apply: callers apply after this
    /// returns `Ok`.
    #[allow(dead_code, reason = "consumed by the config-service dispatch task")]
    async fn persist(
        &self,
        state: &mut tokio::sync::MutexGuard<'_, HubState>,
        candidate: Config,
    ) -> std::result::Result<(), Reply> {
        let path = self.path.clone();
        let to_write = candidate.clone();
        match tokio::task::spawn_blocking(move || to_write.save(&path)).await {
            Ok(Ok(())) => {
                state.current = candidate;
                Ok(())
            }
            Ok(Err(e)) => Err(Reply::error(
                codes::BAD_REQUEST,
                format!("config rejected: {e}"),
            )),
            Err(e) => Err(Reply::error(
                codes::INTERNAL,
                format!("config task failed: {e}"),
            )),
        }
    }

    /// Push the current provider sections onto the settings watches, but
    /// only when the value actually changed: `watch::Sender::send_replace`
    /// on an unchanged value still wakes receivers, which would force a
    /// needless provider reconnect on a no-op reconfigure.
    #[allow(dead_code, reason = "consumed by the config-service dispatch task")]
    fn apply(&self, current: &Config) {
        let alpaca = current.provider.alpaca.as_ref().map(alpaca_settings);
        if *self.alpaca_tx.borrow() != alpaca {
            self.alpaca_tx.send_replace(alpaca);
        }
        let crypto = current
            .provider
            .alpaca_crypto
            .as_ref()
            .map(alpaca_crypto_settings);
        if *self.alpaca_crypto_tx.borrow() != crypto {
            self.alpaca_crypto_tx.send_replace(crypto);
        }
    }
}

/// Map a settings-payload deserialization failure to a stable code:
/// unknown keys are the operator-contract `unknown_config_field`; anything
/// else (bad enum value, wrong type) is `bad_request`.
#[allow(dead_code, reason = "consumed by the config-service dispatch task")]
fn settings_error(e: &serde_json::Error) -> Reply {
    let msg = e.to_string();
    if msg.contains("unknown field") {
        Reply::error(codes::UNKNOWN_CONFIG_FIELD, msg)
    } else {
        Reply::error(codes::BAD_REQUEST, format!("invalid settings: {msg}"))
    }
}

#[allow(dead_code, reason = "consumed by the config-service dispatch task")]
fn unknown_provider(provider: &str) -> Reply {
    Reply::error(
        codes::UNKNOWN_PROVIDER,
        format!("no compiled-in provider {provider:?}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    fn hub_with(
        toml: &str,
    ) -> (
        std::sync::Arc<ConfigHub>,
        ProviderSettingsSources,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.toml");
        let config = Config::parse(toml).expect("parse");
        config.save(&path).expect("seed file");
        let (hub, sources) = ConfigHub::bootstrap(config, path);
        (hub, sources, dir)
    }

    #[tokio::test]
    async fn boot_seeds_watches_from_sections() {
        let (_hub, sources, _dir) = hub_with("[provider.alpaca]\naccount_type = \"live\"\n");
        assert_eq!(
            sources.alpaca.current(),
            Some(datamancer::providers::AlpacaSettings {
                account_type: datamancer::providers::AccountType::Live
            })
        );
        // Unconfigured provider: constructed but disabled.
        assert_eq!(sources.alpaca_crypto.current(), None);
    }

    #[tokio::test]
    async fn configure_provider_persists_then_applies() {
        let (hub, sources, dir) = hub_with("[provider]\n");
        assert_eq!(sources.alpaca.current(), None);
        let reply = hub
            .configure_provider("alpaca", serde_json::json!({"account_type": "live"}))
            .await;
        assert!(reply.ok, "{reply:?}");
        assert_eq!(reply.applied.as_deref(), Some("live"));
        // Applied live on the watch:
        assert_eq!(
            sources.alpaca.current().map(|s| s.account_type),
            Some(datamancer::providers::AccountType::Live)
        );
        // Persisted atomically:
        let on_disk = Config::load(dir.path().join("config.toml")).expect("reload");
        assert!(on_disk.provider.alpaca.is_some());
    }

    #[tokio::test]
    async fn configure_provider_rejects_unknown_field_without_applying() {
        let (hub, sources, dir) = hub_with("[provider]\n");
        let reply = hub
            .configure_provider(
                "alpaca",
                serde_json::json!({"account_type": "live", "bogus": 1}),
            )
            .await;
        assert!(!reply.ok);
        assert_eq!(reply.code.as_deref(), Some("unknown_config_field"));
        assert_eq!(sources.alpaca.current(), None, "must not apply");
        let on_disk = Config::load(dir.path().join("config.toml")).expect("reload");
        assert!(on_disk.provider.alpaca.is_none(), "must not persist");
    }

    #[tokio::test]
    async fn configure_provider_rejects_unknown_provider() {
        let (hub, _sources, _dir) = hub_with("[provider]\n");
        let reply = hub.configure_provider("nope", serde_json::json!({})).await;
        assert_eq!(reply.code.as_deref(), Some("unknown_provider"));
    }

    #[tokio::test]
    async fn remove_provider_disables_and_persists() {
        let (hub, sources, dir) = hub_with("[provider.alpaca]\naccount_type = \"paper\"\n");
        assert!(sources.alpaca.current().is_some());
        let reply = hub.remove_provider("alpaca").await;
        assert!(reply.ok, "{reply:?}");
        assert_eq!(reply.applied.as_deref(), Some("live"));
        assert_eq!(sources.alpaca.current(), None);
        let on_disk = Config::load(dir.path().join("config.toml")).expect("reload");
        assert!(on_disk.provider.alpaca.is_none());
    }

    #[tokio::test]
    async fn get_config_reports_no_divergence_at_boot_and_after_hot_ops() {
        let (hub, _sources, _dir) = hub_with("[provider]\n");
        let reply = hub.get_config().await;
        assert!(reply.ok);
        assert_eq!(reply.restart_required, Some(false));
        hub.configure_provider("alpaca", serde_json::json!({"account_type": "paper"}))
            .await;
        let reply = hub.get_config().await;
        assert_eq!(
            reply.restart_required,
            Some(false),
            "hot ops never require restart"
        );
        assert!(reply.config.unwrap()["provider"]["alpaca"].is_object());
    }

    #[tokio::test]
    async fn reconfiguring_with_identical_settings_does_not_bump_the_watch() {
        let (hub, sources, _dir) = hub_with("[provider.alpaca]\naccount_type = \"paper\"\n");
        let rx = match &sources.alpaca {
            SettingsSource::Watch(rx) => rx.clone(),
            SettingsSource::Static(_) => unreachable!("bootstrap always seeds a Watch source"),
        };
        // Mark the current value as seen.
        assert!(!rx.has_changed().unwrap());

        let reply = hub
            .configure_provider("alpaca", serde_json::json!({"account_type": "paper"}))
            .await;
        assert!(reply.ok, "{reply:?}");
        assert!(
            !rx.has_changed().unwrap(),
            "a no-op reconfigure must not bump the watch and force a reconnect"
        );
    }
}
