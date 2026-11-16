//! Injectable runtime-settings sources for providers (spec 2026-07-05,
//! cycle-3 revision). This is the enable/disable + hot-settings seam: the
//! daemon hands a provider a `Watch` source; `None` = disabled (the provider
//! parks), `Some(settings)` = enabled with those settings. `Static` is the
//! embedder default: always enabled, settings fixed at construction.

use datamancer_core::Error;
use tokio::sync::watch;

/// The error for a REST call with no client, decided from the settings read
/// that left it unbuilt: [`Error::ProviderDisabled`] when that read found the
/// provider disabled, else [`Error::Unauthenticated`].
pub(super) fn no_rest_client(provider: &str, enabled: bool) -> Error {
    let provider = provider.to_string();
    if enabled {
        Error::Unauthenticated {
            provider,
            message: "credentials missing".to_string(),
        }
    } else {
        Error::ProviderDisabled { provider }
    }
}

/// Where a provider gets its runtime settings, resolved fresh at every
/// (re)connect. `None` from a `Watch` source means the provider is disabled.
#[derive(Clone, Debug)]
pub enum SettingsSource<T> {
    /// Fixed settings; the provider is always enabled. The embedder default.
    Static(T),
    /// Live-updatable source (the daemon's config service). `None` =
    /// disabled: the streaming task parks and REST calls fail with
    /// [`Error::ProviderDisabled`].
    Watch(watch::Receiver<Option<T>>),
}

/// A [`SettingsSource`] read for one connect: the settings to connect with
/// and, for a `Watch` source, the receiver on which a change arrives after
/// this read; or, disabled, the receiver that delivers the re-enable. A read
/// never leaves a caller holding a value without the receiver that goes
/// with it.
pub(crate) enum SettingsRead<T> {
    /// Enabled with these settings; `rx` is `Some` for a `Watch` source.
    Enabled {
        settings: T,
        rx: Option<watch::Receiver<Option<T>>>,
    },
    /// A `Watch` source that is disabled.
    Disabled(watch::Receiver<Option<T>>),
}

/// One read of a watch for a connect: the value with the receiver it came
/// from, or the receiver alone when there is no value yet.
pub(crate) enum WatchRead<T> {
    /// A value; `rx` is `Some` while the sender can still change it.
    Value {
        value: T,
        rx: Option<watch::Receiver<Option<T>>>,
    },
    /// No value yet, and the receiver that delivers one.
    Empty(watch::Receiver<Option<T>>),
}

/// Read `rx` for one connect. The value and the seen-marker come from one
/// `borrow_and_update` on a clone of `rx`, so the receiver handed back
/// reports exactly the changes that land after this read. A sender that is
/// gone can change nothing more, so a value then comes with no receiver, as
/// a static source's does; a missing value keeps its receiver, since nothing
/// else could ever wake the park on it.
pub(crate) fn read_watch<T: Clone>(rx: &watch::Receiver<Option<T>>) -> WatchRead<T> {
    let mut rx = rx.clone();
    let value = rx.borrow_and_update().clone();
    // With the value just marked seen, `has_changed` errs only when the
    // sender is gone.
    let can_change = rx.has_changed().is_ok();
    match value {
        Some(value) => WatchRead::Value {
            value,
            rx: can_change.then_some(rx),
        },
        None => WatchRead::Empty(rx),
    }
}

impl<T: Clone> SettingsSource<T> {
    /// The current settings, or `None` when a `Watch` source is disabled.
    pub fn current(&self) -> Option<T> {
        match self {
            Self::Static(s) => Some(s.clone()),
            Self::Watch(rx) => rx.borrow().clone(),
        }
    }

    /// Read the source for one connect (see [`SettingsRead`]); a `Watch`
    /// source is read as [`read_watch`] says.
    pub(crate) fn read(&self) -> SettingsRead<T> {
        match self {
            Self::Static(s) => SettingsRead::Enabled {
                settings: s.clone(),
                rx: None,
            },
            Self::Watch(rx) => match read_watch(rx) {
                WatchRead::Value { value, rx } => SettingsRead::Enabled {
                    settings: value,
                    rx,
                },
                WatchRead::Empty(rx) => SettingsRead::Disabled(rx),
            },
        }
    }

    /// The watch receiver, when this source is watchable. The clone is
    /// returned with the current value marked seen (tokio's
    /// `Receiver::clone` copies the *original* receiver's seen version;
    /// without `mark_unchanged` every clone handed out after the first
    /// change would report `has_changed` immediately — the reconnect-storm
    /// bug from cycle 2).
    pub(crate) fn watch(&self) -> Option<watch::Receiver<Option<T>>> {
        match self {
            Self::Watch(rx) => {
                let mut rx = rx.clone();
                rx.mark_unchanged();
                Some(rx)
            }
            Self::Static(_) => None,
        }
    }
}

/// Whether a cached watch receiver has an unseen change, consuming the
/// marker. Shared by the providers' REST rebuild-on-use guards for both the
/// credential and the settings receivers.
///
/// On a closed channel (sender dropped) tokio's `has_changed` returns `Err`
/// even when a final unseen value is pending, so `Err` counts as changed —
/// the caller rebuilds once with the last value — and the receiver is
/// dropped so subsequent calls return `false` instead of rebuilding forever.
pub(crate) fn watch_changed<T>(rx_slot: &mut Option<watch::Receiver<T>>) -> bool {
    let Some(rx) = rx_slot.as_mut() else {
        return false;
    };
    match rx.has_changed() {
        Ok(true) => {
            let _ = rx.borrow_and_update();
            true
        }
        Ok(false) => false,
        Err(_) => {
            *rx_slot = None;
            true
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{SettingsRead, SettingsSource, watch_changed};

    /// A read of a watched source carries the receiver with its value, and
    /// that receiver reports only a change after the read; a disabled source
    /// hands over the receiver alone.
    #[test]
    fn a_read_carries_the_receiver_its_value_came_from() {
        let (tx, rx) = tokio::sync::watch::channel(None);
        let source = SettingsSource::Watch(rx);
        let SettingsRead::Disabled(disabled_rx) = source.read() else {
            panic!("disabled");
        };
        assert!(!disabled_rx.has_changed().unwrap());
        tx.send(Some(1_u32)).unwrap();
        assert!(disabled_rx.has_changed().unwrap());

        let SettingsRead::Enabled {
            settings,
            rx: Some(enabled_rx),
        } = source.read()
        else {
            panic!("enabled, with a receiver");
        };
        assert_eq!(settings, 1);
        assert!(!enabled_rx.has_changed().unwrap(), "the read value is seen");
        tx.send(Some(2)).unwrap();
        assert!(enabled_rx.has_changed().unwrap());

        assert!(matches!(
            SettingsSource::Static(7_u32).read(),
            SettingsRead::Enabled {
                settings: 7,
                rx: None
            }
        ));

        drop(tx);
        assert!(
            matches!(
                source.read(),
                SettingsRead::Enabled {
                    settings: 2,
                    rx: None
                }
            ),
            "with the sender gone the settings come with no receiver"
        );
    }

    #[test]
    fn static_source_is_always_enabled() {
        let src = SettingsSource::Static(7_u32);
        assert_eq!(src.current(), Some(7));
        assert!(src.watch().is_none());
    }

    #[test]
    fn watch_source_none_is_disabled_and_tracks_updates() {
        let (tx, rx) = tokio::sync::watch::channel(None);
        let src = SettingsSource::Watch(rx);
        assert_eq!(src.current(), None);
        tx.send(Some(7_u32)).unwrap();
        assert_eq!(src.current(), Some(7));
    }

    #[test]
    fn watch_clone_does_not_see_pre_clone_sends() {
        let (tx, rx) = tokio::sync::watch::channel(None);
        let src = SettingsSource::Watch(rx);
        tx.send(Some(1_u32)).unwrap();
        let fresh = src.watch().expect("watchable");
        assert_eq!(fresh.has_changed().ok(), Some(false));
    }

    #[test]
    fn watch_clone_sees_post_clone_sends() {
        let (tx, rx) = tokio::sync::watch::channel(None);
        let src = SettingsSource::Watch(rx);
        let fresh = src.watch().expect("watchable");
        tx.send(Some(1_u32)).unwrap();
        assert_eq!(fresh.has_changed().ok(), Some(true));
    }

    #[test]
    fn watch_changed_consumes_the_marker() {
        let (tx, rx) = tokio::sync::watch::channel(None);
        let src = SettingsSource::Watch(rx);
        let mut cached = src.watch();
        assert!(!watch_changed(&mut cached));
        tx.send(Some(1_u32)).unwrap();
        assert!(watch_changed(&mut cached));
        assert!(!watch_changed(&mut cached));
    }

    #[test]
    fn watch_changed_syncs_once_on_closed_channel() {
        let (tx, rx) = tokio::sync::watch::channel(None);
        let src = SettingsSource::Watch(rx);
        let mut cached = src.watch();
        tx.send(Some(1_u32)).unwrap();
        drop(tx);
        assert!(watch_changed(&mut cached));
        assert!(!watch_changed(&mut cached));
        assert!(!watch_changed(&mut cached));
    }

    #[test]
    fn watch_changed_ignores_static_sources() {
        let mut cached = SettingsSource::Static(1_u32).watch();
        assert!(!watch_changed(&mut cached));
    }
}
