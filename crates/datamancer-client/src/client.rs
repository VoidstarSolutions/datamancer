//! The generic client-transport trait: one multiplexed consumer handle,
//! transport chosen at compile time.

use datamancer_core::{InstrumentInfo, MarketEvent, ProviderId, SystemSnapshot};
use futures::Stream;

use crate::error::ClientError;
use crate::spec::{SubscriptionSpec, UnsubscribeSpec};

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
    /// they are [`ClientError::Control`].
    type Error: std::error::Error + Send + 'static;
    /// The multiplexed event stream, yielded in delivery order.
    type Events: Stream<Item = MarketEvent> + Send + Unpin;

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

    /// Graceful close: the daemon emits a terminal `SessionClosing` on the
    /// event stream and tears the client down.
    fn close(self) -> impl Future<Output = Result<(), ClientError<Self::Error>>> + Send;
}

#[cfg(test)]
mod tests {
    use super::Client;
    use crate::error::ClientError;
    use crate::spec::{SubscriptionSpec, UnsubscribeSpec};
    use datamancer_core::{InstrumentInfo, MarketEvent, ProviderId, SystemSnapshot};
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
        async fn close(self) -> Result<(), ClientError<Self::Error>> {
            Ok(())
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
}
