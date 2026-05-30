//! Real-time crypto ticker.
//!
//! Subscribes to Trade + Quote streams for a few major crypto pairs via the
//! Alpaca crypto streaming feed and prints a compact, in-place table showing
//! last trade, best bid/ask, and the current spread.
//!
//! Run with:
//!
//! ```text
//! cargo run --example crypto_ticker
//! ```
//!
//! Requires `ALPACA_PAPER_API_KEY_ID` / `ALPACA_PAPER_API_SECRET_KEY` (paper)
//! or `ALPACA_LIVE_API_KEY_ID` / `ALPACA_LIVE_API_SECRET_KEY` (live) in the
//! environment. Defaults to paper.
//!
//! Press Ctrl-C to exit.

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Local, TimeZone, Utc};
use datamancer::{
    AssetClass, ControlKind, Datamancer, EventKind, Instrument, MarketEvent, PersistenceOptions,
    Price, ProviderId, Scope, Session, Timestamp,
    providers::{AlpacaCryptoProvider, AlpacaCryptoProviderConfig, AlpacaCryptoVenue},
};
use futures::StreamExt;
use oxidized_alpaca::AccountType;
use tokio::sync::Mutex;

/// Pairs we subscribe to. Symbols use Alpaca's `BASE/QUOTE` format.
const SYMBOLS: &[&str] = &["BTC/USD", "ETH/USD", "SOL/USD", "DOGE/USD"];

#[derive(Default, Clone, Copy)]
struct Ticker {
    last_trade: Option<Price>,
    last_trade_ts: Option<Timestamp>,
    bid: Option<Price>,
    ask: Option<Price>,
    quote_ts: Option<Timestamp>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os("RUST_LOG").is_some() {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_writer(std::io::stderr)
            .init();
    }
    // Build datamancer with the Alpaca crypto provider.
    let provider = Arc::new(AlpacaCryptoProvider::new(AlpacaCryptoProviderConfig {
        account_type: AccountType::Paper,
        venue: AlpacaCryptoVenue::Us,
        ..Default::default()
    }));
    let dm = Datamancer::builder().provider_arc(provider).build()?;

    // Open one session per (instrument, kind) pair. Two kinds × N symbols.
    // Each session is single-owner; we pin them in a Vec so they stay alive
    // for the lifetime of the program.
    let mut sessions: Vec<Session> = Vec::with_capacity(SYMBOLS.len() * 2);
    let state: Arc<Mutex<HashMap<String, Ticker>>> = Arc::new(Mutex::new(HashMap::new()));

    for sym in SYMBOLS {
        for kind in [EventKind::Trade, EventKind::Quote] {
            let mut session = dm
                .session(
                    Instrument::new(
                        ProviderId::from_static("alpaca-crypto"),
                        AssetClass::Crypto,
                        *sym,
                    ),
                    kind,
                    Scope::Live {
                        backfill_from: None,
                    },
                    PersistenceOptions::none(),
                )
                .await?;
            let mut stream = session.take_events()?;
            sessions.push(session);

            let state = state.clone();
            let symbol = (*sym).to_string();
            tokio::spawn(async move {
                while let Some(ev) = stream.next().await {
                    match ev {
                        MarketEvent::Trade(t) => {
                            let mut map = state.lock().await;
                            let entry = map.entry(symbol.clone()).or_default();
                            entry.last_trade = Some(t.price);
                            entry.last_trade_ts = Some(t.source_ts);
                        }
                        MarketEvent::Quote(q) => {
                            let mut map = state.lock().await;
                            let entry = map.entry(symbol.clone()).or_default();
                            entry.bid = Some(q.bid);
                            entry.ask = Some(q.ask);
                            entry.quote_ts = Some(q.source_ts);
                        }
                        MarketEvent::Control(c) => {
                            // Surface provider-level state changes to stderr so the
                            // table itself stays clean; useful when debugging with
                            // `RUST_LOG=…` since the table redraws over stdout.
                            if let ControlKind::ProviderError { provider, message } = &c.kind {
                                eprintln!("provider {provider} error: {message}");
                            }
                        }
                        _ => {}
                    }
                }
            });
        }
    }

    // Hide the cursor while we redraw. Restore it on Ctrl-C.
    print!("\x1b[?25l");
    let render_state = state.clone();
    let render = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(250));
        // Reserve N rows + header so we're not flicker-clearing the whole
        // screen each tick.
        for _ in 0..(SYMBOLS.len() + 2) {
            println!();
        }
        loop {
            interval.tick().await;
            let snapshot: HashMap<String, Ticker> = render_state.lock().await.clone();
            render_table(&snapshot);
            let _ = std::io::stdout().flush();
        }
    });

    tokio::signal::ctrl_c().await?;
    render.abort();
    // Restore cursor and drop down past the table.
    print!("\x1b[?25h");
    println!();
    Ok(())
}

fn render_table(state: &HashMap<String, Ticker>) {
    // Move cursor up by (rows + header + blank) to the top of our reserved
    // region, then redraw. `\x1b[<n>A` moves up; each line ends with
    // `\x1b[K` to clear to end-of-line so a shrunken value doesn't leave
    // stale chars behind.
    let rows = SYMBOLS.len() + 2;
    print!("\x1b[{rows}A");
    println!(
        "{:<10} {:>14} {:>14} {:>14} {:>10} {:>12}\x1b[K",
        "Symbol", "Last", "Bid", "Ask", "Spread", "Updated"
    );
    println!("{}\x1b[K", "-".repeat(78));
    for sym in SYMBOLS {
        let t = state.get(*sym).copied().unwrap_or_default();
        let last = t
            .last_trade
            .map(price_to_f64)
            .map_or_else(|| "-".into(), fmt_price);
        let bid = t
            .bid
            .map(price_to_f64)
            .map_or_else(|| "-".into(), fmt_price);
        let ask = t
            .ask
            .map(price_to_f64)
            .map_or_else(|| "-".into(), fmt_price);
        let spread = match (t.bid, t.ask) {
            (Some(b), Some(a)) => fmt_price(price_to_f64(a) - price_to_f64(b)),
            _ => "-".into(),
        };
        let updated = latest_ts(&t).map_or_else(|| "-".into(), format_local_time);
        println!("{sym:<10} {last:>14} {bid:>14} {ask:>14} {spread:>10} {updated:>12}\x1b[K");
    }
}

fn price_to_f64(p: Price) -> f64 {
    p.to_f64()
}

fn fmt_price(v: f64) -> String {
    // Format with thousands separators and 2-4 fractional digits depending
    // on magnitude. Crypto prices span ~$0.01 (memecoins) to ~$100k (BTC).
    let frac = if v.abs() >= 100.0 {
        2
    } else if v.abs() >= 1.0 {
        3
    } else {
        4
    };
    let formatted = format!("{v:.frac$}");
    let (int_part, frac_part) = match formatted.split_once('.') {
        Some((i, f)) => (i, Some(f)),
        None => (formatted.as_str(), None),
    };
    let with_commas = insert_thousands(int_part);
    match frac_part {
        Some(f) => format!("{with_commas}.{f}"),
        None => with_commas,
    }
}

fn insert_thousands(s: &str) -> String {
    let (sign, digits) = match s.strip_prefix('-') {
        Some(rest) => ("-", rest),
        None => ("", s),
    };
    let bytes = digits.as_bytes();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3 + sign.len());
    out.push_str(sign);
    let first_group = bytes.len() % 3;
    if first_group > 0 {
        out.push_str(std::str::from_utf8(&bytes[..first_group]).unwrap());
        if bytes.len() > first_group {
            out.push(',');
        }
    }
    for (i, chunk) in bytes[first_group..].chunks(3).enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(std::str::from_utf8(chunk).unwrap());
    }
    out
}

fn latest_ts(t: &Ticker) -> Option<Timestamp> {
    match (t.last_trade_ts, t.quote_ts) {
        (Some(a), Some(b)) => Some(if a.0 > b.0 { a } else { b }),
        (Some(a), None) | (None, Some(a)) => Some(a),
        _ => None,
    }
}

fn format_local_time(ts: Timestamp) -> String {
    let secs = ts.0 / 1_000_000_000;
    // rem_euclid guarantees the result is in [0, 1_000_000_000), which fits u32.
    let nanos = u32::try_from(ts.0.rem_euclid(1_000_000_000)).unwrap_or(0);
    let dt: DateTime<Utc> = match Utc.timestamp_opt(secs, nanos) {
        chrono::LocalResult::Single(d) => d,
        _ => return "-".into(),
    };
    dt.with_timezone(&Local).format("%H:%M:%S").to_string()
}
