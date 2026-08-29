//! The generic client-transport trait: one multiplexed consumer handle,
//! transport chosen at compile time.

use datamancer_core::{InstrumentEntry, InstrumentInfo, MarketEvent, ProviderId, SystemSnapshot};
use futures::Stream;

use crate::error::ClientError;
use crate::spec::{QueryId, QuerySpec, SubscriptionSpec, UnsubscribeSpec};

/// A connected datamancerd client, generic over transport.
///
/// # Contract (upheld by every implementation)
///
/// - **One connection = one client = one multiplexed stream**, ordered by
///   `(instrument, seq)`; per-instrument demux is the consumer's concern.
/// - The timestamp triple (`source_ts`, `seq`, `rx_ts`) crosses verbatim;
///   `rx_ts` is observability-only and never synthesized client-side.
/// - Control rejections surface as [`ClientError::Control`] with the stable
///   [`crate::codes`] strings — identical across transports.
/// - **Loss is never silent.** On iceoryx2, resume-buffer overflow surfaces
///   in-band as `Control::Gap` (a numbered `seq` hole). On WebSocket, a slow
///   consumer is disconnected — the stream ends. A stream that ends after a
///   `SessionClosing` control closed gracefully; one that ends without it
///   lost its connection. Reconnect policy is the consumer's choice.
/// - Connection-scoped provider controls are suppressed from the stream;
///   read connectivity from [`Client::snapshot`].
pub trait Client: Sized + Send {
    /// Per-transport connection parameters (URL/token vs socket-path/name).
    type Config;
    /// Transport-layer failure type. Control rejections are **not** this —
    /// they are [`ClientError::Control`]. `Sync` so generic consumers can
    /// convert into `anyhow::Error`/`Box<dyn Error + Send + Sync>`.
    type Error: std::error::Error + Send + Sync + 'static;
    /// The multiplexed event stream, yielded in delivery order. `'static` so
    /// generic consumers can `tokio::spawn` the drain task — the point of
    /// the split `(handle, events)` pair.
    type Events: Stream<Item = MarketEvent> + Send + Unpin + 'static;

    /// Connect and return the split pair: the control handle and the owned
    /// event stream, separate values so a consumer can drain events while
    /// issuing control calls.
    fn connect(
        cfg: Self::Config,
    ) -> impl Future<Output = Result<(Self, Self::Events), ClientError<Self::Error>>> + Send;

    /// Add a subscription to this client's set.
    fn subscribe(
        &mut self,
        spec: &SubscriptionSpec,
    ) -> impl Future<Output = Result<(), ClientError<Self::Error>>> + Send;

    /// Remove a subscription from this client's set.
    fn unsubscribe(
        &mut self,
        spec: &UnsubscribeSpec,
    ) -> impl Future<Output = Result<(), ClientError<Self::Error>>> + Send;

    /// The daemon's current diagnostics snapshot (provider connectivity,
    /// latency, gap counts). This is where connection-scoped provider state
    /// lives — it is deliberately not on the event stream.
    fn snapshot(
        &mut self,
    ) -> impl Future<Output = Result<SystemSnapshot, ClientError<Self::Error>>> + Send;

    /// The instrument catalog: which instruments each provider serves and
    /// which event kinds each supports. Pass `provider` to bound the reply
    /// (a full equities catalog is ~10k rows).
    fn instruments(
        &mut self,
        provider: Option<&ProviderId>,
    ) -> impl Future<Output = Result<Vec<InstrumentInfo>, ClientError<Self::Error>>> + Send;

    /// On-demand per-instrument capabilities for a named provider's symbols.
    fn capabilities(
        &mut self,
        provider: &ProviderId,
        symbols: &[String],
    ) -> impl Future<Output = Result<Vec<InstrumentEntry>, ClientError<Self::Error>>> + Send;

    /// Graceful close: the daemon emits a terminal `SessionClosing` on the
    /// event stream and tears the client down.
    fn close(self) -> impl Future<Output = Result<(), ClientError<Self::Error>>> + Send;

    /// A bounded historical event stream. Yields the query's events in source
    /// order, then ends. A terminal `Control::SessionClosing` marks a clean
    /// finish; provider failures arrive in-band as `Control::ProviderError` /
    /// `Control::ProviderDisconnected` before the stream ends.
    type Query: Stream<Item = MarketEvent> + Send + Unpin + 'static;

    /// Open a bounded historical query. Unlike [`Client::subscribe`], this is
    /// not part of the multiplexed live stream: the query owns its own channel
    /// and its own `seq` space.
    ///
    /// **Dropping the returned stream cancels the query**, so an abandoned
    /// backtest does not keep consuming provider rate limit. Call
    /// [`Client::cancel_query`] to abort one whose stream is still held.
    ///
    /// # Errors
    ///
    /// - `unknown_provider` / `unsupported_event_kind` — no provider serves the
    ///   pair on its history surface.
    /// - `bad_request` — the range is inverted (`from > to`).
    /// - `service_cap_exceeded` — the daemon's `max_queries` cap is reached.
    /// - `unsupported_transport` — this transport cannot serve queries (WS).
    fn query(
        &mut self,
        spec: &QuerySpec,
    ) -> impl Future<Output = Result<(QueryId, Self::Query), ClientError<Self::Error>>> + Send;

    /// Abort an in-flight query. Answering `unknown_query` means it had already
    /// finished or been cancelled — which is not an error for most callers.
    ///
    /// # Errors
    ///
    /// `unknown_query` when the daemon has no such query in flight.
    fn cancel_query(
        &mut self,
        query: QueryId,
    ) -> impl Future<Output = Result<(), ClientError<Self::Error>>> + Send;
}

#[cfg(test)]
mod tests {
    use super::Client;
    use crate::error::ClientError;
    use crate::spec::{QueryId, QuerySpec, SubscriptionSpec, UnsubscribeSpec};
    use datamancer_core::{
        InstrumentEntry, InstrumentInfo, MarketEvent, ProviderId, SystemSnapshot,
    };
    use futures::stream::{self, Empty};

    #[derive(Debug, thiserror::Error)]
    #[error("never")]
    struct NeverError;

    struct FakeClient;

    impl Client for FakeClient {
        type Config = ();
        type Error = NeverError;
        type Events = Empty<MarketEvent>;

        async fn connect(
            (): Self::Config,
        ) -> Result<(Self, Self::Events), ClientError<Self::Error>> {
            Ok((FakeClient, stream::empty()))
        }
        async fn subscribe(
            &mut self,
            _spec: &SubscriptionSpec,
        ) -> Result<(), ClientError<Self::Error>> {
            Ok(())
        }
        async fn unsubscribe(
            &mut self,
            _spec: &UnsubscribeSpec,
        ) -> Result<(), ClientError<Self::Error>> {
            Err(ClientError::Control {
                code: crate::codes::NOT_SUBSCRIBED.to_string(),
                message: "not subscribed".to_string(),
            })
        }
        async fn snapshot(&mut self) -> Result<SystemSnapshot, ClientError<Self::Error>> {
            Err(ClientError::Transport(NeverError))
        }
        async fn instruments(
            &mut self,
            _provider: Option<&ProviderId>,
        ) -> Result<Vec<InstrumentInfo>, ClientError<Self::Error>> {
            Ok(Vec::new())
        }
        async fn capabilities(
            &mut self,
            _provider: &ProviderId,
            _symbols: &[String],
        ) -> Result<Vec<InstrumentEntry>, ClientError<Self::Error>> {
            Ok(Vec::new())
        }
        async fn close(self) -> Result<(), ClientError<Self::Error>> {
            Ok(())
        }

        type Query = Empty<MarketEvent>;

        // `WsClient` has no cheap constructor for a disconnected instance (its
        // fields all come from a live socket split), so the
        // `unsupported_transport` regression the WS stub exists to prove is
        // asserted here instead, against these bodies mirroring `WsClient`'s
        // (brief-authorized fallback). Every other `FakeClient` method
        // succeeds; these two deliberately do not, so
        // `query_reports_unsupported_transport_code` below has something to
        // assert on.
        async fn query(
            &mut self,
            _spec: &QuerySpec,
        ) -> Result<(QueryId, Self::Query), ClientError<Self::Error>> {
            Err(ClientError::Control {
                code: crate::codes::UNSUPPORTED_TRANSPORT.to_string(),
                message: "historical queries are not available on this transport".to_string(),
            })
        }

        async fn cancel_query(&mut self, _query: QueryId) -> Result<(), ClientError<Self::Error>> {
            Err(ClientError::Control {
                code: crate::codes::UNSUPPORTED_TRANSPORT.to_string(),
                message: "historical queries are not available on this transport".to_string(),
            })
        }
    }

    /// The generic consumer shape the trait exists to make possible: code
    /// written once against `C: Client`, transport chosen by type.
    async fn generic_consumer<C: Client>(cfg: C::Config) -> Result<(), ClientError<C::Error>> {
        let (mut client, _events) = C::connect(cfg).await?;
        client.instruments(None).await?;
        client.close().await
    }

    #[tokio::test]
    async fn trait_supports_generic_consumers() {
        generic_consumer::<FakeClient>(()).await.expect("fake ok");
    }

    #[tokio::test]
    async fn control_errors_carry_the_stable_code() {
        let (mut client, _events) = FakeClient::connect(()).await.unwrap();
        match client
            .unsubscribe(
                &serde_json::from_str::<UnsubscribeSpec>(
                    r#"{"provider":"p","asset_class":"crypto","symbol":"BTC/USD","kind":"trade"}"#,
                )
                .unwrap(),
            )
            .await
        {
            Err(ClientError::Control { code, .. }) => {
                assert_eq!(code, crate::codes::NOT_SUBSCRIBED);
            }
            other => panic!("expected Control error, got {other:?}"),
        }
    }

    /// Stand-in for `ws_query_is_unsupported_on_this_transport`: `WsClient`
    /// has no cheap disconnected constructor, so this asserts the same
    /// `unsupported_transport` control code against `FakeClient`, whose
    /// `query`/`cancel_query` bodies mirror `WsClient`'s stubs.
    #[tokio::test]
    async fn query_reports_unsupported_transport_code() {
        let spec: QuerySpec = serde_json::from_str(
            r#"{"provider":"alpaca","asset_class":"equity","symbol":"AAPL","kind":"bar1d","from":1,"to":2}"#,
        )
        .expect("parse");
        let mut client = FakeClient;
        match client.query(&spec).await {
            Err(ClientError::Control { code, .. }) => {
                assert_eq!(code, crate::codes::UNSUPPORTED_TRANSPORT);
            }
            other => panic!("expected an unsupported-transport control error, got {other:?}"),
        }
        match client.cancel_query(1).await {
            Err(ClientError::Control { code, .. }) => {
                assert_eq!(code, crate::codes::UNSUPPORTED_TRANSPORT);
            }
            other => panic!("expected an unsupported-transport control error, got {other:?}"),
        }
    }
}
