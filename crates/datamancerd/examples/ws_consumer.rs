//! Live WS consumer for the `datamancerd` data plane — the client counterpart
//! to the in-process [`crypto_ticker`](./crypto_ticker.rs) example.
//!
//! Connects to an **already-running** daemon's WS surface, subscribes to one
//! `(provider, asset_class, symbol, kind)`, and prints each live event frame the
//! daemon serves. This is the exact path validated in the Windows live bring-up:
//! the daemon boots WS-only on Windows and serves real market data to an
//! external client over the `datamancer.v2` subprotocol.
//!
//! Requires the `ws` feature. Run against a daemon started with `--features ws`
//! and `[ws] enabled = true` (see the "Live bring-up (WS)" section of
//! `crates/datamancerd/README.md`):
//!
//! ```text
//! # terminal 1 — the daemon
//! cargo run -p datamancerd --features ws -- --config datamancerd.toml
//!
//! # terminal 2 — this consumer
//! cargo run -p datamancerd --features ws --example ws_consumer -- \
//!     --port 9001 --symbol BTC/USD --kind trade
//! ```
//!
//! Prices and sizes cross the wire as raw `i64`/`u64` in datamancer-core's
//! fixed-point `1e9` scale; this example divides to render decimals. Exits after
//! `--count` frames (default 20) or on Ctrl-C.

// The wire carries fixed-point `i64`/`u64`; rendering them as decimals is an
// intentional lossy display cast (values here are far within f64's exact range).
#![allow(clippy::cast_precision_loss)]

use futures::{SinkExt as _, StreamExt as _};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;

/// datamancer-core `Price`/`Quantity` fixed-point scale.
const SCALE: f64 = 1_000_000_000.0;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = Args::parse();

    // The daemon requires the event-frame wire version as a WS subprotocol; a
    // client that omits it is rejected at the handshake (400).
    let url = format!("ws://127.0.0.1:{}/", cfg.port);
    let mut request = url.as_str().into_client_request()?;
    request.headers_mut().insert(
        "sec-websocket-protocol",
        datamancer::transport_ws::WS_SUBPROTOCOL.parse()?,
    );

    let (mut ws, _resp) = connect_async(request).await?;
    eprintln!("[ws_consumer] connected to {url}");

    let subscribe = serde_json::json!({
        "id": 1,
        "op": "subscribe",
        "provider": cfg.provider,
        "asset_class": cfg.asset_class,
        "symbol": cfg.symbol,
        "kind": cfg.kind,
    });
    ws.send(Message::text(subscribe.to_string())).await?;
    eprintln!(
        "[ws_consumer] subscribed {} {} {} — waiting for live frames (Ctrl-C to stop)",
        cfg.provider, cfg.symbol, cfg.kind
    );

    let mut seen = 0usize;
    while let Some(message) = ws.next().await {
        let Message::Text(text) = message? else {
            continue;
        };
        let Ok(frame) = serde_json::from_str::<serde_json::Value>(&text) else {
            continue;
        };

        // Control replies (subscribe ack, etc.) carry `id`; event frames carry
        // `type`. See `datamancer-transport-ws::wire::EventFrame`.
        if frame.get("id").is_some() {
            let ok = frame.get("ok").and_then(serde_json::Value::as_bool);
            eprintln!("[ws_consumer] control reply: ok={ok:?}");
            continue;
        }

        match frame.get("type").and_then(serde_json::Value::as_str) {
            Some("trade") => {
                let sym = frame["instrument"]["symbol"].as_str().unwrap_or("?");
                let price = frame["price"].as_i64().unwrap_or(0) as f64 / SCALE;
                let size = frame["size"].as_u64().unwrap_or(0) as f64 / SCALE;
                let seq = frame["seq"].as_u64().unwrap_or(0);
                println!("trade  {sym:<10}  ${price:>12.2}  size={size:<12}  seq={seq}");
                seen += 1;
            }
            Some("quote") => {
                let sym = frame["instrument"]["symbol"].as_str().unwrap_or("?");
                let bid = frame["bid"].as_i64().unwrap_or(0) as f64 / SCALE;
                let ask = frame["ask"].as_i64().unwrap_or(0) as f64 / SCALE;
                let seq = frame["seq"].as_u64().unwrap_or(0);
                println!("quote  {sym:<10}  bid ${bid:>12.2}  ask ${ask:>12.2}  seq={seq}");
                seen += 1;
            }
            // In-band controls (subscription_changed, gap, session_closing) and
            // any other frame kind: surface to stderr, keep the data on stdout.
            Some(other) => eprintln!("[ws_consumer] control frame: {other}"),
            None => {}
        }

        if cfg.count != 0 && seen >= cfg.count {
            eprintln!("[ws_consumer] received {seen} frame(s); closing");
            break;
        }
    }

    let _ = ws.close(None).await;
    Ok(())
}

/// Minimal flag parsing (no `clap` dependency for an example).
struct Args {
    port: u16,
    provider: String,
    asset_class: String,
    symbol: String,
    kind: String,
    count: usize,
}

impl Args {
    fn parse() -> Self {
        let mut cfg = Self {
            port: 9001,
            provider: "alpaca-crypto".to_string(),
            asset_class: "crypto".to_string(),
            symbol: "BTC/USD".to_string(),
            kind: "trade".to_string(),
            count: 20,
        };
        let args: Vec<String> = std::env::args().skip(1).collect();
        let mut i = 0;
        while i < args.len() {
            let value = args.get(i + 1).cloned().unwrap_or_default();
            match args[i].as_str() {
                "--port" => cfg.port = value.parse().unwrap_or(cfg.port),
                "--provider" => cfg.provider = value,
                "--asset-class" => cfg.asset_class = value,
                "--symbol" => cfg.symbol = value,
                "--kind" => cfg.kind = value,
                "--count" => cfg.count = value.parse().unwrap_or(cfg.count),
                "-h" | "--help" => {
                    eprintln!(
                        "usage: ws_consumer [--port N] [--provider P] [--asset-class C] \
                         [--symbol S] [--kind trade|quote|bar] [--count N (0 = unbounded)]"
                    );
                    std::process::exit(0);
                }
                other => eprintln!("[ws_consumer] ignoring unknown flag: {other}"),
            }
            // Advance past the flag; value-bearing flags consumed one more above.
            i += if args[i].starts_with("--") && args[i] != "--help" {
                2
            } else {
                1
            };
        }
        cfg
    }
}
