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
        match self.changed_or_gone().await {
            Some(value) => value,
            None => std::future::pending().await,
        }
    }

    /// As [`changed`](Self::changed), but a sender that goes away is
    /// reported, once, as `None`, for a waiter whose wait it ends: the
    /// receiver is dropped with it, so a later call pends.
    pub(crate) async fn changed_or_gone(&mut self) -> Option<T>
    where
        T: Clone,
    {
        if let Some(rx) = &mut self.0 {
            if rx.changed().await.is_ok() {
                return Some(rx.borrow_and_update().clone());
            }
            self.0 = None;
            return None;
        }
        std::future::pending().await
    }

    /// Whether a change has landed since the read this watch came from, or
    /// since the last call, marking it seen. A sender that has gone is
    /// asked once more for a value it left unseen, and the receiver goes
    /// with it, so every later call is `false`.
    pub(crate) fn take_change(&mut self) -> bool {
        let Some(rx) = &mut self.0 else {
            return false;
        };
        match rx.has_changed() {
            Ok(true) => {
                rx.borrow_and_update();
                true
            }
            Ok(false) => false,
            Err(_) => {
                let changed = rx.borrow_and_update().has_changed();
                self.0 = None;
                changed
            }
        }
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

impl<V, W> WatchRead<V, W> {
    /// The watch alone, for a reader that takes its value elsewhere.
    pub(crate) fn into_watch(self) -> Watched<Option<W>> {
        match self {
            Self::Value { rx, .. } | Self::Empty(rx) => rx,
        }
    }
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
}

#[cfg(test)]
mod tests {
    use super::{SettingsSource, WatchRead, Watched};

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
        assert!(src.read().into_watch().0.is_none());
    }

    #[test]
    fn watch_source_none_is_disabled_and_tracks_updates() {
        let (tx, rx) = tokio::sync::watch::channel(None);
        let src = SettingsSource::Watch(rx);
        assert_eq!(src.current(), None);
        tx.send(Some(7_u32)).unwrap();
        assert_eq!(src.current(), Some(7));
    }

    /// A watch taken from a read reports only what lands after the read,
    /// once each.
    #[test]
    fn take_change_reports_each_change_after_the_read_once() {
        let (tx, rx) = tokio::sync::watch::channel(None);
        let src = SettingsSource::Watch(rx);
        tx.send(Some(1_u32)).unwrap();
        let mut watched = src.read().into_watch();
        assert!(!watched.take_change(), "a send before the read is seen");
        tx.send(Some(2)).unwrap();
        assert!(watched.take_change());
        assert!(!watched.take_change());
    }

    /// A value the sender left before going is reported once; after that,
    /// with nothing left to change, never again.
    #[test]
    fn take_change_reports_a_last_value_once_the_sender_goes() {
        let (tx, rx) = tokio::sync::watch::channel(None);
        let src = SettingsSource::Watch(rx);
        let mut watched = src.read().into_watch();
        tx.send(Some(1_u32)).unwrap();
        drop(tx);
        assert!(watched.take_change());
        assert!(watched.0.is_none(), "the receiver is dropped");
        assert!(!watched.take_change());
    }

    /// A sender that goes having left nothing unseen is no change.
    #[test]
    fn take_change_ignores_a_sender_gone_with_nothing_new() {
        let (tx, rx) = tokio::sync::watch::channel(Some(1_u32));
        let mut watched = Watched(Some(rx));
        drop(tx);
        assert!(!watched.take_change());
        assert!(watched.0.is_none(), "the receiver is dropped");
    }

    /// A send landing while `take_change` runs is never marked seen
    /// without being reported: once a send has completed, a later call
    /// reports it unless an earlier one already did.
    #[test]
    fn take_change_never_swallows_a_concurrent_send() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::{Arc, Barrier};
        for iteration in 0..20_000 {
            let (tx, rx) = tokio::sync::watch::channel(Some(0_u32));
            let mut watched = SettingsSource::Watch(rx).read().into_watch();
            let sent = Arc::new(AtomicBool::new(false));
            let start = Arc::new(Barrier::new(2));
            let writer = {
                let sent = Arc::clone(&sent);
                let start = Arc::clone(&start);
                std::thread::spawn(move || {
                    start.wait();
                    for _ in 0..fastrand::u32(0..200) {
                        std::hint::spin_loop();
                    }
                    tx.send(Some(1)).unwrap();
                    sent.store(true, Ordering::SeqCst);
                    tx
                })
            };
            start.wait();
            let reported = loop {
                if watched.take_change() {
                    break true;
                }
                if sent.load(Ordering::SeqCst) {
                    break watched.take_change();
                }
            };
            let _tx = writer.join().unwrap();
            assert!(reported, "send swallowed on iteration {iteration}");
        }
    }

    #[test]
    fn take_change_ignores_static_sources() {
        assert!(
            !SettingsSource::Static(1_u32)
                .read()
                .into_watch()
                .take_change()
        );
    }

    /// A value the sender left comes first, then its going, once; after that
    /// the wait pends rather than resolving again.
    #[tokio::test(start_paused = true)]
    async fn changed_or_gone_reports_a_gone_sender_once() {
        let (tx, rx) = tokio::sync::watch::channel(0_u32);
        let mut watched = Watched(Some(rx));
        tx.send(1).unwrap();
        drop(tx);
        assert_eq!(watched.changed_or_gone().await, Some(1));
        assert_eq!(watched.changed_or_gone().await, None);
        let later =
            tokio::time::timeout(std::time::Duration::from_secs(1), watched.changed_or_gone())
                .await;
        assert!(later.is_err(), "pends once the sender's going is reported");
    }
}
