//! Fetch the trailing year of AAPL end-of-day bars from Alpaca, replay
//! them through `citadel_core::pf::ColumnBuilder` with a `$2 box,
//! 3-reversal, anchor=first-price` configuration, and materialise the
//! result as a real-world validation vector at
//! `vectors/assets/pf_column_builder/aapl_one_year_eod.json`.
//!
//! This is the first real-data test vector. The five existing fixtures
//! are tiny scenario stubs (≤7 ticks each); AAPL EOD over a year drops
//! ~252 ticks through the column builder and locks in the
//! `ColumnDelta` sequence the analysis emits — a much bigger surface
//! for catching regressions in the engine.
//!
//! Requires `ALPACA_PAPER_API_KEY_ID` / `ALPACA_PAPER_API_SECRET_KEY`
//! (paper) or `ALPACA_LIVE_*` (live) in the environment. Defaults to
//! paper. Run with:
//!
//! ```sh
//! cargo run --example fetch_aapl_eod -p datamancer
//! ```
//!
//! Re-running the example overwrites the JSON file, so committing it
//! freezes the snapshot. To refresh against fresh data, delete the
//! file and re-run.

use std::path::PathBuf;
use std::sync::Arc;

use chrono::{Duration, Utc};
use citadel_core::pf::{Anchor, BoxSize, ColumnBuilder, PfParams, Reversal};
use citadel_core::{Actor, InstrumentId, Tick, Timestamp as CitadelTimestamp, Timestamps};
use citadel_vectors::VectorId;
use citadel_vectors::pf::{LoadedPfVector, PfVectorFile};
use datamancer::providers::{AlpacaProvider, AlpacaProviderConfig};
use datamancer::{
    BarInterval, EventKind, HistoryRequest, Instrument, MarketEvent, Provider, Timestamp,
};
use oxidized_alpaca::AccountType;
use tokio::sync::mpsc;

/// Static instrument id used for replay. Matches the
/// hardcoded `InstrumentId(1)` the server uses in v0.
const INSTRUMENT_ID: InstrumentId = InstrumentId(1);

const VECTOR_ID: &str = "pf.column_builder.aapl_one_year_eod";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("RUST_LOG").is_some() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .init();
    }

    let to = Utc::now();
    let from = to - Duration::days(365);
    let from_ns = from
        .timestamp_nanos_opt()
        .expect("timestamp fits in i64 nanos");
    let to_ns = to
        .timestamp_nanos_opt()
        .expect("timestamp fits in i64 nanos");

    println!(
        "fetching AAPL OneDay bars from {} to {} (1 year)",
        from.format("%Y-%m-%d"),
        to.format("%Y-%m-%d"),
    );

    let provider = Arc::new(AlpacaProvider::new(AlpacaProviderConfig {
        account_type: AccountType::Paper,
        ..Default::default()
    }));

    let (tx, mut rx) = mpsc::channel::<MarketEvent>(512);
    let request = HistoryRequest {
        instrument: Instrument::new("AAPL"),
        kind: EventKind::Bar(BarInterval::OneDay),
        from: Timestamp(from_ns),
        to: Timestamp(to_ns),
    };

    let fetch_handle = {
        let provider = provider.clone();
        tokio::spawn(async move { provider.fetch_history(request, tx).await })
    };

    let mut bars: Vec<(i64, citadel_core::Price)> = Vec::new();
    while let Some(event) = rx.recv().await {
        if let MarketEvent::Bar(bar) = event {
            // `datamancer::Price` and `citadel_core::Price` share the
            // exact same `i64`-nanos representation; lift the raw value
            // across the crate boundary.
            let close = citadel_core::Price::from_raw(bar.close.0);
            bars.push((bar.source_ts.0, close));
        }
    }
    fetch_handle.await??;

    // Bars come in chronological order from Alpaca, but be paranoid
    // about the contract — the validation-vector test asserts strict
    // monotonicity of timestamps.
    bars.sort_by_key(|(ts, _)| *ts);

    if bars.is_empty() {
        return Err("Alpaca returned no bars for the requested window".into());
    }

    println!("fetched {} bars", bars.len());

    // P&F params chosen for AAPL's ~$200 price level: $2 box ≈ 1%,
    // 3-reversal is the textbook default, anchor at the first observed
    // price keeps the box grid stable.
    let params = PfParams {
        box_size: BoxSize(citadel_core::Price::from_units(2)),
        reversal: Reversal(3),
        anchor: Anchor::FirstPrice,
    };

    let ticks: Vec<Tick> = bars
        .into_iter()
        .map(|(source_ts, close)| Tick {
            timestamps: Timestamps {
                event: CitadelTimestamp(source_ts),
                receive: None,
                ingest: None,
            },
            price: close,
            size: None,
            side: None,
        })
        .collect();

    let mut actor = Actor::new(INSTRUMENT_ID, ColumnBuilder::new(params));
    for tick in &ticks {
        actor.ingest(tick);
    }
    let expected = actor.outputs().to_vec();
    println!("ColumnBuilder emitted {} column deltas", expected.len());

    let envelope = LoadedPfVector::from_file(PfVectorFile {
        id: VectorId::from_static(VECTOR_ID),
        name: "AAPL one-year end-of-day".to_string(),
        description:
            "Trailing year of AAPL daily close prices replayed through ColumnBuilder with \
             a $2 box, 3-reversal chart anchored at the first observed price. The first \
             real-data validation vector; size and price range are representative of a \
             liquid US equity at the ~$200 level."
                .to_string(),
        target: citadel_core::AnalysisKind::PfColumnBuilder,
        params,
        ticks,
        expected,
    })
    .to_file();

    let manifest_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let assets_dir = manifest_dir.join("../vectors/assets/pf_column_builder");
    std::fs::create_dir_all(&assets_dir)?;
    let out_path = assets_dir.join("aapl_one_year_eod.json");
    let mut bytes = serde_json::to_string_pretty(&envelope)?.into_bytes();
    bytes.push(b'\n');
    std::fs::write(&out_path, bytes)?;
    println!("wrote {}", out_path.display());

    Ok(())
}
