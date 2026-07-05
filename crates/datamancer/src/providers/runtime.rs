//! Injectable runtime-settings sources for providers (spec 2026-07-05,
//! cycle-3 revision). This is the enable/disable + hot-settings seam: the
//! daemon hands a provider a `Watch` source; `None` = disabled (the provider
//! parks), `Some(settings)` = enabled with those settings. `Static` is the
//! embedder default: always enabled, settings fixed at construction.

use tokio::sync::watch;

/// Where a provider gets its runtime settings, resolved fresh at every
/// (re)connect. `None` from a `Watch` source means the provider is disabled.
#[derive(Clone, Debug)]
pub enum SettingsSource<T> {
    /// Fixed settings; the provider is always enabled. The embedder default.
    Static(T),
    /// Live-updatable source (the daemon's config service). `None` =
    /// disabled: the streaming task parks and REST calls fail unavailable.
    Watch(watch::Receiver<Option<T>>),
}

impl<T: Clone> SettingsSource<T> {
    /// The current settings, or `None` when a `Watch` source is disabled.
    pub fn current(&self) -> Option<T> {
        match self {
            Self::Static(s) => Some(s.clone()),
            Self::Watch(rx) => rx.borrow().clone(),
        }
    }

    /// The watch receiver, when this source is watchable. The clone is
    /// returned with the current value marked seen (tokio's
    /// `Receiver::clone` copies the *original* receiver's seen version;
    /// without `mark_unchanged` every clone handed out after the first
    /// change would report `has_changed` immediately — the reconnect-storm
    /// bug from cycle 2).
    #[allow(dead_code)]
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
    use super::{SettingsSource, watch_changed};

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
