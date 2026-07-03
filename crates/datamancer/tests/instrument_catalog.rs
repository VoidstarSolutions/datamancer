//! `Datamancer::instrument_catalog`: per-instrument kind derivation from
//! `list_instruments` + `supports`, with an optional provider filter.

use async_trait::async_trait;
use datamancer::{
    AssetClass, BarInterval, Datamancer, Error, EventKind, Instrument, InstrumentInfo, LiveHandle,
    MarketEvent, Provider, ProviderId,
};
use datamancer_core::HistoryRequest;
use tokio::sync::mpsc;

/// Fake provider whose kind support varies **by instrument** — guards the
/// per-instrument catalog shape (a provider-wide kinds list would collapse
/// this distinction).
struct VaryingFake {
    id: &'static str,
}

#[async_trait]
impl Provider for VaryingFake {
    fn id(&self) -> &str {
        self.id
    }

    fn supports(&self, instrument: &Instrument, kind: EventKind) -> bool {
        match instrument.symbol() {
            // Full-service symbol.
            "BTC/USD" => matches!(
                kind,
                EventKind::Trade | EventKind::Quote | EventKind::Bar(BarInterval::OneDay)
            ),
            // Bars-only symbol.
            "IDX" => matches!(kind, EventKind::Bar(BarInterval::OneDay)),
            _ => false,
        }
    }

    async fn start_live(
        &self,
        _sink: mpsc::Sender<MarketEvent>,
    ) -> datamancer::Result<Box<dyn LiveHandle>> {
        Err(Error::Provider {
            provider: self.id.to_string(),
            message: "not live-capable".to_string(),
        })
    }

    async fn fetch_history(
        &self,
        _request: HistoryRequest,
        _sink: mpsc::Sender<MarketEvent>,
    ) -> datamancer::Result<()> {
        Ok(())
    }

    async fn list_instruments(&self) -> datamancer::Result<Vec<Instrument>> {
        Ok(vec![
            Instrument::new(
                ProviderId::from_static(self.id),
                AssetClass::Crypto,
                "BTC/USD",
            ),
            Instrument::new(ProviderId::from_static(self.id), AssetClass::Crypto, "IDX"),
        ])
    }
}

fn dm() -> Datamancer {
    Datamancer::builder()
        .provider(Box::new(VaryingFake { id: "fake-a" }))
        .provider(Box::new(VaryingFake { id: "fake-b" }))
        .build()
        .expect("build")
}

#[tokio::test]
async fn catalog_derives_kinds_per_instrument() {
    let catalog = dm()
        .instrument_catalog(Some(&ProviderId::from_static("fake-a")))
        .await
        .expect("catalog");
    assert_eq!(
        catalog,
        vec![
            InstrumentInfo {
                instrument: Instrument::new(
                    ProviderId::from_static("fake-a"),
                    AssetClass::Crypto,
                    "BTC/USD"
                ),
                kinds: vec![
                    EventKind::Trade,
                    EventKind::Quote,
                    EventKind::Bar(BarInterval::OneDay),
                ],
            },
            InstrumentInfo {
                instrument: Instrument::new(
                    ProviderId::from_static("fake-a"),
                    AssetClass::Crypto,
                    "IDX"
                ),
                kinds: vec![EventKind::Bar(BarInterval::OneDay)],
            },
        ]
    );
}

#[tokio::test]
async fn catalog_without_filter_fans_over_all_providers() {
    let catalog = dm().instrument_catalog(None).await.expect("catalog");
    // Two providers x two instruments each.
    assert_eq!(catalog.len(), 4);
    assert!(
        catalog
            .iter()
            .any(|i| i.instrument.provider().as_str() == "fake-a")
    );
    assert!(
        catalog
            .iter()
            .any(|i| i.instrument.provider().as_str() == "fake-b")
    );
}

#[tokio::test]
async fn unknown_provider_filter_is_an_error() {
    let err = dm()
        .instrument_catalog(Some(&ProviderId::from_static("nope")))
        .await
        .expect_err("unknown provider");
    assert!(matches!(err, Error::UnknownProvider(p) if p == "nope"));
}
