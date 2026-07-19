//! `Datamancer::instrument_catalog`: per-instrument kind derivation from
//! `list_instruments` + `supports`, with an optional provider filter.

use async_trait::async_trait;
use datamancer::Surface;
use datamancer::{
    AssetClass, BarInterval, Datamancer, Error, EventKind, Instrument, InstrumentEntry,
    InstrumentInfo, LiveHandle, MarketEvent, Provider, ProviderId,
};
use datamancer_core::HistoryRequest;
use tokio::sync::mpsc;

/// Fake provider whose kind support varies **by instrument and by surface** —
/// guards the per-instrument catalog shape (a provider-wide kinds list would
/// collapse the first distinction, a single `kinds` field the second).
///
/// The two symbols model the two real asymmetries, in opposite directions:
/// `BTC/USD` streams ticks it cannot backfill, `IDX` backfills a finer bar than
/// it streams (which is what Alpaca equities actually do).
struct VaryingFake {
    id: &'static str,
}

#[async_trait]
impl Provider for VaryingFake {
    fn id(&self) -> &str {
        self.id
    }

    fn supports(&self, instrument: &Instrument, kind: EventKind, surface: Surface) -> bool {
        match (instrument.symbol(), surface) {
            // Full-service live symbol, but only its daily bar is backfillable.
            ("BTC/USD", Surface::Live) => matches!(
                kind,
                EventKind::Trade | EventKind::Quote | EventKind::Bar(BarInterval::OneDay)
            ),
            ("BTC/USD", Surface::History) => {
                matches!(kind, EventKind::Bar(BarInterval::OneDay))
            }
            // Bars-only symbol whose history reaches an interval it never streams.
            ("IDX", Surface::Live) => matches!(kind, EventKind::Bar(BarInterval::OneDay)),
            ("IDX", Surface::History) => matches!(
                kind,
                EventKind::Bar(BarInterval::OneMinute | BarInterval::OneDay)
            ),
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

    async fn list_instruments(&self) -> datamancer::Result<Vec<InstrumentEntry>> {
        let mut btc = InstrumentEntry::bare(Instrument::new(
            ProviderId::from_static(self.id),
            AssetClass::Crypto,
            "BTC/USD",
        ));
        if self.id == "fake" {
            let mut caps = datamancer::InstrumentCapabilities::default();
            caps.fractionable = Some(true);
            btc.capabilities = Some(caps);
        }
        Ok(vec![
            btc,
            InstrumentEntry::bare(Instrument::new(
                ProviderId::from_static(self.id),
                AssetClass::Crypto,
                "IDX",
            )),
        ])
    }

    async fn capabilities(
        &self,
        instrument: &Instrument,
    ) -> datamancer::Result<Option<InstrumentEntry>> {
        if self.id == "fake" && instrument.symbol() == "BTC/USD" {
            // Return the provider's authoritative instrument (Crypto), NOT the
            // caller's — this is where a daemon placeholder class gets corrected.
            let mut entry = InstrumentEntry::bare(Instrument::new(
                ProviderId::from_static(self.id),
                AssetClass::Crypto,
                instrument.symbol().to_string(),
            ));
            let mut caps = datamancer::InstrumentCapabilities::default();
            caps.fractionable = Some(true);
            caps.supports_notional_orders = Some(true);
            entry.capabilities = Some(caps);
            Ok(Some(entry))
        } else {
            Ok(None)
        }
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
            InstrumentInfo::new(
                Instrument::new(
                    ProviderId::from_static("fake-a"),
                    AssetClass::Crypto,
                    "BTC/USD"
                ),
                vec![
                    EventKind::Trade,
                    EventKind::Quote,
                    EventKind::Bar(BarInterval::OneDay),
                ],
                vec![EventKind::Bar(BarInterval::OneDay)],
            ),
            InstrumentInfo::new(
                Instrument::new(ProviderId::from_static("fake-a"), AssetClass::Crypto, "IDX"),
                vec![EventKind::Bar(BarInterval::OneDay)],
                // In `EventKind::enumerate` order, so the minute bar precedes
                // the daily one.
                vec![
                    EventKind::Bar(BarInterval::OneMinute),
                    EventKind::Bar(BarInterval::OneDay),
                ],
            ),
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

#[tokio::test]
async fn catalog_carries_capabilities_and_enrichment_works() {
    let dm = Datamancer::builder()
        .provider(Box::new(VaryingFake { id: "fake" }))
        .build()
        .expect("build");
    let pid = ProviderId::from_static("fake");

    let catalog = dm.instrument_catalog(Some(&pid)).await.unwrap();
    let btc = catalog
        .iter()
        .find(|i| i.instrument.symbol() == "BTC/USD")
        .unwrap();
    assert_eq!(btc.capabilities.as_ref().unwrap().fractionable, Some(true));
    let idx = catalog
        .iter()
        .find(|i| i.instrument.symbol() == "IDX")
        .unwrap();
    assert!(idx.capabilities.is_none());

    // The daemon builds lookups with a placeholder class; the provider must
    // correct it on the returned entry (#1). Pass a deliberately-wrong Equity
    // placeholder for both and assert what happens to each.
    let want = vec![
        Instrument::new(
            ProviderId::from_static("fake"),
            AssetClass::Equity, // wrong on purpose — a daemon-style placeholder
            "BTC/USD",
        ),
        Instrument::new(ProviderId::from_static("fake"), AssetClass::Equity, "IDX"),
    ];
    let enriched = dm.instrument_capabilities(&pid, &want).await.unwrap();
    assert_eq!(enriched.len(), 2);
    // BTC/USD resolved: provider stamped the authoritative Crypto class and caps.
    assert_eq!(enriched[0].instrument.asset_class(), AssetClass::Crypto);
    assert_eq!(
        enriched[0]
            .capabilities
            .as_ref()
            .unwrap()
            .supports_notional_orders,
        Some(true)
    );
    // IDX unresolved (provider returned None): the input is echoed back
    // unchanged — placeholder class and all — since no authority can correct it.
    assert_eq!(enriched[1].instrument.asset_class(), AssetClass::Equity);
    assert!(enriched[1].capabilities.is_none());
}
