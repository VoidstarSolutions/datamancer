//! Multiplexed client session over two instruments.
//!
//! Opens a single [`datamancer::ClientSession`], subscribes to the Trade stream
//! for two crypto pairs, and drains the **one multiplexed stream** the client
//! presents. Demonstrates the Phase 2 primary consumer handle: a mutable
//! subscription set behind one interleaved stream (per-symbol deterministic,
//! arrival-order across symbols — no merge-sort).
//!
//! Run with:
//!
//! ```text
//! cargo run --example client_session
//! ```
//!
//! Requires `ALPACA_PAPER_API_KEY_ID` / `ALPACA_PAPER_API_SECRET_KEY` (paper)
//! or the live equivalents in the environment. Defaults to paper.
//!
//! Press Ctrl-C to exit.

use std::sync::Arc;

use datamancer::{
    AssetClass, ControlKind, Datamancer, EventKind, Instrument, MarketEvent, PersistenceOptions,
    ProviderId, Scope,
    providers::{AlpacaCryptoProvider, AlpacaCryptoProviderConfig, AlpacaCryptoVenue},
};
use futures::StreamExt;
use oxidized_alpaca::AccountType;

const SYMBOLS: &[&str] = &["BTC/USD", "ETH/USD"];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("RUST_LOG").is_some() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .init();
    }

    let provider = Arc::new(AlpacaCryptoProvider::new(AlpacaCryptoProviderConfig {
        account_type: AccountType::Paper,
        venue: AlpacaCryptoVenue::Us,
        ..Default::default()
    }));
    let dm = Datamancer::builder().provider_arc(provider).build()?;

    // One client session, multiple subscriptions, one multiplexed stream.
    let client = dm.client_session();
    for sym in SYMBOLS {
        client
            .subscribe(
                Instrument::new(
                    ProviderId::from_static("alpaca-crypto"),
                    AssetClass::Crypto,
                    *sym,
                ),
                EventKind::Trade,
                Scope::Live {
                    backfill_from: None,
                },
                PersistenceOptions::none(),
            )
            .await?;
    }
    let mut stream = client.take_events().await?;

    let drain = tokio::spawn(async move {
        while let Some(ev) = stream.next().await {
            match ev {
                MarketEvent::Trade(t) => {
                    println!(
                        "{:<9} trade {:>12.2} (seq {})",
                        t.instrument.symbol(),
                        t.price.to_f64(),
                        t.seq.0
                    );
                }
                MarketEvent::Control(c) => match c.kind {
                    ControlKind::SubscriptionChanged {
                        instrument, active, ..
                    } => println!("subscription {}: active={active}", instrument.symbol()),
                    ControlKind::ProviderConnected { provider } => {
                        println!("connected: {provider}");
                    }
                    ControlKind::ProviderError { provider, message } => {
                        eprintln!("provider {provider} error: {message}");
                    }
                    _ => {}
                },
                _ => {}
            }
        }
    });

    tokio::signal::ctrl_c().await?;
    drain.abort();
    client.close().await?;
    Ok(())
}
