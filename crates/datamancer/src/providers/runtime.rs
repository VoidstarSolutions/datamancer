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

/// A watch receiver a reader may not have: a static source has none, and a
/// source whose sender is gone can deliver nothing more. [`changed`] pends
/// in either case, so a `select!` arm on it needs no guard.
///
/// [`changed`]: Self::changed
pub(crate) struct Watched<T>(pub(crate) Option<watch::Receiver<T>>);

impl<T> Watched<T> {
    /// Whether a change can still arrive, or one has arrived unseen: there is
    /// a receiver, and its sender is still there or left it a value this
    /// watch has not yet handed back.
    pub(crate) fn may_change(&self) -> bool {
        self.0
            .as_ref()
            .is_some_and(|rx| rx.has_changed().is_ok() || rx.borrow().has_changed())
    }

    /// Resolves to the new value once one has landed, marked seen. With
    /// nothing to wait on it pends, and a sender that goes away leaves
    /// nothing to wait on: the receiver is dropped and the wait goes on
    /// pending, where a closed receiver on its own would resolve at once and
    /// spin a `select!`.
    pub(crate) async fn changed(&mut self) -> T
    where
        T: Clone,
    {
        if let Some(rx) = &mut self.0 {
            if rx.changed().await.is_ok() {
                return rx.borrow_and_update().clone();
            }
            self.0 = None;
        }
        std::future::pending().await
    }
}

/// One read of a source for a connect: its value with the watch on which the
/// next change arrives, or, with no value yet (a disabled provider, no
/// credentials provisioned), that watch alone. A read never leaves a caller
/// holding a value without the watch that goes with it. The watch carries
/// `W`, which is the value's own type unless a source names a value it does
/// not hold, as an environment credential source does.
pub(crate) enum WatchRead<V, W = V> {
    /// A value; the watch has a receiver while the sender can still change
    /// it, and none for a static source.
    Value { value: V, rx: Watched<Option<W>> },
    /// No value yet, and the watch that delivers one.
    Empty(Watched<Option<W>>),
}

/// Read `rx` for one connect. The value and the seen-marker come from one
/// `borrow_and_update` on a clone of `rx`, so the watch handed back reports
/// exactly the changes that land after this read. A sender that is gone can
/// change nothing more, so the watch then has no receiver, as a static
/// source's does, and a wait on it never ends.
pub(crate) fn read_watch<T: Clone>(rx: &watch::Receiver<Option<T>>) -> WatchRead<T> {
    let mut rx = rx.clone();
    let value = rx.borrow_and_update().clone();
    // With the value just marked seen, `has_changed` errs only when the
    // sender is gone.
    let rx = Watched(rx.has_changed().is_ok().then_some(rx));
    match value {
        Some(value) => WatchRead::Value { value, rx },
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

    /// Read the source for one connect (see [`WatchRead`]): the settings, or
    /// `Empty` while disabled. A `Watch` source is read as [`read_watch`]
    /// says.
    pub(crate) fn read(&self) -> WatchRead<T> {
        match self {
            Self::Static(s) => WatchRead::Value {
                value: s.clone(),
                rx: Watched(None),
            },
            Self::Watch(rx) => read_watch(rx),
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
    use super::{SettingsSource, WatchRead, Watched, watch_changed};

    /// A read of a watched source carries the receiver with its value, and
    /// that receiver reports only a change after the read; a disabled source
    /// hands over the receiver alone.
    #[test]
    fn a_read_carries_the_receiver_its_value_came_from() {
        let (tx, rx) = tokio::sync::watch::channel(None);
        let source = SettingsSource::Watch(rx);
        let WatchRead::Empty(Watched(Some(disabled_rx))) = source.read() else {
            panic!("disabled, with a receiver");
        };
        assert!(!disabled_rx.has_changed().unwrap());
        tx.send(Some(1_u32)).unwrap();
        assert!(disabled_rx.has_changed().unwrap());

        let WatchRead::Value {
            value,
            rx: Watched(Some(enabled_rx)),
        } = source.read()
        else {
            panic!("enabled, with a receiver");
        };
        assert_eq!(value, 1);
        assert!(!enabled_rx.has_changed().unwrap(), "the read value is seen");
        tx.send(Some(2)).unwrap();
        assert!(enabled_rx.has_changed().unwrap());

        assert!(matches!(
            SettingsSource::Static(7_u32).read(),
            WatchRead::Value {
                value: 7,
                rx: Watched(None)
            }
        ));

        drop(tx);
        assert!(
            matches!(
                source.read(),
                WatchRead::Value {
                    value: 2,
                    rx: Watched(None)
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
