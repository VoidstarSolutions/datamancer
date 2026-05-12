//! Fetch the trailing year of AAPL end-of-day bars from Alpaca, replay
//! them through `citadel_core::pf::ColumnBuilder` under three box-size
//! configurations, and materialise the result as real-world validation
//! vectors under `vectors/assets/pf_column_builder/`.
//!
//! Three variants are emitted from a single fetch so they share the
//! same trading-day window — only the box size differs:
//!
//! - `aapl_one_year_eod.json`         — `$2 box, 3-reversal`. Coarse;
//!   ~31 columns over the year, the "what does P&F look like in
//!   practice" baseline.
//! - `aapl_one_year_eod_1usd_box.json` — `$1 box, 3-reversal`. Finer;
//!   roughly doubles the column count and exercises tighter
//!   reversals.
//! - `aapl_one_year_eod_50cent_box.json` — `$0.50 box, 3-reversal`.
//!   Finest; pushes the renderer and the engine with the most columns
//!   and the densest delta stream we can plausibly defend for a
//!   $200-ish stock.
//!
//! Requires `ALPACA_PAPER_API_KEY_ID` / `ALPACA_PAPER_API_SECRET_KEY`
//! (paper) or `ALPACA_LIVE_*` (live) in the environment. Defaults to
//! paper. Run with:
//!
//! ```sh
//! cargo run --example fetch_aapl_eod -p datamancer
//! ```
//!
//! Re-running the example overwrites the JSON files, so committing
//! them freezes the snapshot. To refresh against fresh data, re-run.

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

/// Per-vector configuration emitted from a single fetch. Each entry
/// produces one JSON asset file using the shared tick stream and the
/// listed box size; reversal stays at 3 and anchor at first-price
/// across all variants so the only knob that changes is grain.
struct BoxConfig {
    /// `Price::from_raw` value for the box size, in nanos of a dollar.
    box_size_nanos: i64,
    /// Stable [`VectorId`] for this variant.
    vector_id: &'static str,
    /// On-disk filename under `vectors/assets/pf_column_builder/`.
    filename: &'static str,
    /// Short human-readable label for the panel.
    name: &'static str,
    /// Long-form description embedded in the vector envelope.
    description: &'static str,
}

const CONFIGS: &[BoxConfig] = &[
    BoxConfig {
        box_size_nanos: 2_000_000_000,
        vector_id: "pf.column_builder.aapl_one_year_eod",
        filename: "aapl_one_year_eod.json",
        name: "AAPL one-year end-of-day ($2 box)",
        description: "Trailing year of AAPL daily close prices replayed through ColumnBuilder with a $2 \
             box, 3-reversal chart anchored at the first observed price. The coarse baseline \
             real-data vector — ~31 columns over the year, representative of textbook P&F \
             practice on a ~$200 stock.",
    },
    BoxConfig {
        box_size_nanos: 1_000_000_000,
        vector_id: "pf.column_builder.aapl_one_year_eod_1usd_box",
        filename: "aapl_one_year_eod_1usd_box.json",
        name: "AAPL one-year end-of-day ($1 box)",
        description: "Same trading-day window and reversal threshold as the baseline AAPL vector, but \
             with a $1 box. Roughly doubles the column count and exercises tighter reversal \
             behaviour.",
    },
    BoxConfig {
        box_size_nanos: 500_000_000,
        vector_id: "pf.column_builder.aapl_one_year_eod_50cent_box",
        filename: "aapl_one_year_eod_50cent_box.json",
        name: "AAPL one-year end-of-day ($0.50 box)",
        description: "Finest-grain AAPL EOD variant: $0.50 box, 3-reversal. Designed to push the \
             renderer and the engine with the densest delta stream we can defend for a $200 \
             stock — many columns, frequent reversals.",
    },
];

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

    // Build the canonical tick stream once. Each variant clones it
    // into its own envelope so the on-disk JSON is self-contained.
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

    let manifest_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let assets_dir = manifest_dir.join("../vectors/assets/pf_column_builder");
    std::fs::create_dir_all(&assets_dir)?;

    for config in CONFIGS {
        let params = PfParams {
            box_size: BoxSize(citadel_core::Price::from_raw(config.box_size_nanos)),
            reversal: Reversal(3),
            anchor: Anchor::FirstPrice,
        };

        let mut actor = Actor::new(INSTRUMENT_ID, ColumnBuilder::new(params));
        for tick in &ticks {
            actor.ingest(tick);
        }
        let expected = actor.outputs().to_vec();

        let envelope = LoadedPfVector::from_file(PfVectorFile {
            id: VectorId::from_static(config.vector_id),
            name: config.name.to_string(),
            description: config.description.to_string(),
            target: citadel_core::AnalysisKind::PfColumnBuilder,
            params,
            ticks: ticks.clone(),
            expected,
        })
        .to_file();

        let out_path = assets_dir.join(config.filename);
        let mut bytes = serde_json::to_string_pretty(&envelope)?.into_bytes();
        bytes.push(b'\n');
        std::fs::write(&out_path, bytes)?;
        println!(
            "wrote {} ({} column deltas)",
            out_path.display(),
            envelope.expected.len(),
        );
    }

    Ok(())
}
