//! A `live_backfill` tape against a running `datamancerd` — the consumer-side
//! view of the backfill seam, over the same-host iceoryx2 transport.
//!
//! Connects to an **already-running** daemon's control socket, subscribes one
//! `(provider, asset_class, symbol, kind)` with `scope: live_backfill` and
//! `backfill_from = now - lookback`, and prints what arrives: each event
//! compactly, tagged `hist` or `LIVE` by its `source_ts` against the time the
//! daemon's reply arrived, every in-band control, every `seq` hole as it is
//! seen, and a summary at the end (`hist` and `LIVE` counts, the newest `hist`
//! timestamp, the oldest `LIVE` timestamp, controls, holes).
//!
//! Requires the `iceoryx2` feature. Run against a daemon whose provider is
//! configured (see `crates/datamancerd/README.md`):
//!
//! ```text
//! # terminal 1 — the daemon
//! cargo run -p datamancerd -- --config datamancerd.toml
//!
//! # terminal 2 — this tape (every argument is positional and optional)
//! cargo run -p datamancer-client --features iceoryx2 --example backfill_tape -- \
//!     /path/to/control.sock alpaca equity AAPL trade 90 25
//! ```
//!
//! Arguments, in order: the control socket path (default: the platform's
//! well-known endpoint), provider id (`alpaca`), asset class (`equity` or
//! `crypto`), symbol (`AAPL`), event kind (`trade`, `quote`, `bar1s`,
//! `bar1m`, `bar5m`, `bar15m`, `bar1h`, `bar1d`), lookback in seconds (`90`)
//! and run time in seconds (`25`).
//!
//! Reading the output:
//!
//! - `hist` events carry a `source_ts` before the reply arrived, `LIVE` events
//!   one at or after it. The split approximates the seam: the daemon fixes the
//!   backfill's live edge before it replies, and the reply does not carry the
//!   edge, so a live print between the edge and the reply reads as `hist`, and
//!   the summary's newest `hist` timestamp can be such a print. Against a real
//!   provider the newest history print usually precedes the edge by the history
//!   endpoint's own lag.
//! - Offsets are relative to the request, so on a cold socket (the reply waits
//!   for the connect, auth and acknowledgement; the reply latency is printed)
//!   the last history prints can read as positive offsets.
//! - A subscriber that joined an open session begins where it joined. A backfill
//!   still running reaches it from that point on: the remainder of the history,
//!   then every buffered live arrival, then the live tail. It has no `Gap` for
//!   what preceded it. A joiner of a seeding live session can receive the seed
//!   if it joined before the seed was delivered; the seed's older `source_ts`
//!   tags it `hist`, so it can be the summary's newest `hist` timestamp.
//! - A `seq` hole with no `Gap` is not loss: connection-scoped controls
//!   (`ProviderConnected`, `ProviderDisconnected`, `ProviderError`) take a
//!   `seq` slot at the source but ride the daemon's diagnostics plane, not the
//!   data plane this tape reads, so a provider that connects during the backfill
//!   leaves a hole at the seam. Real loss is announced in-band as a `Gap`
//!   control whose span names the missing range.
//! - Prices and sizes are datamancer-core fixed-point values rendered as
//!   decimals.

// Rendering fixed-point prices, sizes and nanosecond offsets as decimals is a
// deliberate display cast; every value here is far inside f64's exact range.
#![allow(clippy::cast_precision_loss)]

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use datamancer_client::Client;
use datamancer_client::iceoryx2::{Iceoryx2Client, Iceoryx2Config};
use datamancer_client::spec::{AssetClassCfg, EventKindCfg, SubscriptionSpec};
use datamancer_core::{ControlKind, Instrument, MarketEvent, Seq, Subscribed, Timestamp};
use futures::StreamExt as _;

/// How many history and how many live events to print before going quiet
/// (controls and holes are always printed).
const SHOW_HISTORY: u64 = 12;
const SHOW_LIVE: u64 = 8;

const NANOS_PER_SECOND: i64 = 1_000_000_000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse()?;

    let (mut client, mut events) = Iceoryx2Client::connect(Iceoryx2Config {
        control_socket: args.socket.clone(),
        client_name: format!("backfill-tape-{}", std::process::id()),
        poll_interval: Duration::from_millis(5),
        event_buffer: 65_536,
    })
    .await?;

    let requested_at = wall_clock_ns();
    let from = args
        .lookback_s
        .checked_mul(NANOS_PER_SECOND)
        .and_then(|ns| requested_at.checked_sub(ns))
        .map(Timestamp)
        .ok_or_else(|| format!("lookback of {} s is out of range\n{USAGE}", args.lookback_s))?;
    let instrument = Instrument::new(
        args.provider.clone(),
        args.asset_class.into(),
        args.symbol.as_str(),
    );
    let spec = SubscriptionSpec::new(&instrument, args.kind.into())?.with_backfill_from(from);
    let started = Instant::now();
    let subscribed = client.subscribe(&spec).await?;
    let replied_at = wall_clock_ns();
    println!(
        "subscribed {} {:?} {} {:?}: {subscribed:?} (asked {}) reply in {:.3?}",
        args.provider,
        args.asset_class,
        args.symbol,
        args.kind,
        from.0,
        started.elapsed()
    );
    if matches!(subscribed, Subscribed::Joined { .. }) {
        println!(
            "  joined an open session: the stream begins here, with the remainder of \
             any backfill still running"
        );
    }

    let mut tape = Tape::new(requested_at, replied_at);
    let deadline = tokio::time::sleep(Duration::from_secs(args.run_s));
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            () = &mut deadline => break,
            next = events.next() => {
                let Some(event) = next else {
                    println!("  stream ended");
                    break;
                };
                tape.record(&event, started.elapsed());
            }
        }
    }

    tape.summary(started.elapsed());
    Ok(())
}

/// Running counts and bounds for the summary, plus the print budget.
struct Tape {
    requested_at: i64,
    /// When the daemon's reply arrived.
    boundary: i64,
    history: u64,
    live: u64,
    controls: u64,
    holes: u64,
    shown_history: u64,
    shown_live: u64,
    last_seq: Option<u64>,
    newest_history_ts: Option<i64>,
    oldest_live_ts: Option<i64>,
    first_live_arrival: Option<Duration>,
}

impl Tape {
    fn new(requested_at: i64, boundary: i64) -> Self {
        Self {
            requested_at,
            boundary,
            history: 0,
            live: 0,
            controls: 0,
            holes: 0,
            shown_history: 0,
            shown_live: 0,
            last_seq: None,
            newest_history_ts: None,
            oldest_live_ts: None,
            first_live_arrival: None,
        }
    }

    /// Seconds relative to the request, signed, for a `source_ts`.
    fn offset(&self, ts: Timestamp) -> f64 {
        (ts.0 - self.requested_at) as f64 / NANOS_PER_SECOND as f64
    }

    /// Track `seq` continuity. Synthetic controls (`Seq::SYNTHETIC`) are
    /// out-of-band and exempt.
    fn note_seq(&mut self, seq: Seq) {
        if seq == Seq::SYNTHETIC {
            return;
        }
        if let Some(last) = self.last_seq
            && seq.0 != last + 1
        {
            self.holes += 1;
            println!("  seq hole: {last} -> {}", seq.0);
        }
        self.last_seq = Some(seq.0);
    }

    fn record(&mut self, event: &MarketEvent, arrived: Duration) {
        let (seq, source_ts, line) = match event {
            MarketEvent::Trade(t) => (
                t.seq,
                t.source_ts,
                format!("price={:.4} size={}", t.price.to_f64(), t.size.to_f64()),
            ),
            MarketEvent::Quote(q) => (
                q.seq,
                q.source_ts,
                format!(
                    "bid={:.4}x{} ask={:.4}x{}",
                    q.bid.to_f64(),
                    q.bid_size.to_f64(),
                    q.ask.to_f64(),
                    q.ask_size.to_f64()
                ),
            ),
            MarketEvent::Bar(b) => (
                b.seq,
                b.source_ts,
                format!(
                    "{:?} o={:.4} h={:.4} l={:.4} c={:.4} v={}",
                    b.interval,
                    b.open.to_f64(),
                    b.high.to_f64(),
                    b.low.to_f64(),
                    b.close.to_f64(),
                    b.volume.to_f64()
                ),
            ),
            MarketEvent::Control(c) => {
                self.controls += 1;
                self.note_seq(c.seq);
                println!(
                    "  CONTROL seq={} src={:+.3}s {}",
                    seq_label(c.seq),
                    self.offset(c.source_ts),
                    describe_control(&c.kind)
                );
                return;
            }
            other => {
                println!("  other event: {other:?}");
                return;
            }
        };
        self.note_seq(seq);
        let is_history = source_ts.0 < self.boundary;
        if is_history {
            self.history += 1;
            self.newest_history_ts = self.newest_history_ts.max(Some(source_ts.0));
        } else {
            self.live += 1;
            self.oldest_live_ts = Some(
                self.oldest_live_ts
                    .map_or(source_ts.0, |t| t.min(source_ts.0)),
            );
            if self.first_live_arrival.is_none() {
                self.first_live_arrival = Some(arrived);
            }
        }
        let (budget, shown, tag, what) = if is_history {
            (SHOW_HISTORY, self.shown_history, "hist", "history")
        } else {
            (SHOW_LIVE, self.shown_live, "LIVE", "live")
        };
        if shown < budget {
            println!(
                "  {tag} seq={} src={:+.3}s {line}",
                seq_label(seq),
                self.offset(source_ts)
            );
            if shown + 1 == budget {
                println!("  ... (further {what} events go unprinted; the summary counts them)");
            }
            if is_history {
                self.shown_history += 1;
            } else {
                self.shown_live += 1;
            }
        }
    }

    fn summary(&self, elapsed: Duration) {
        println!("--- summary after {elapsed:.1?} ---");
        println!(
            "hist events (source_ts before the reply):  {}, newest at {}",
            self.history,
            self.newest_history_ts.map_or_else(
                || "n/a".to_string(),
                |t| format!("{:+.3}s", self.offset(Timestamp(t)))
            )
        );
        println!(
            "LIVE events (source_ts at or after the reply): {}, oldest at {}, first arrived {} after the request",
            self.live,
            self.oldest_live_ts.map_or_else(
                || "n/a".to_string(),
                |t| format!("{:+.3}s", self.offset(Timestamp(t)))
            ),
            self.first_live_arrival
                .map_or_else(|| "never".to_string(), |d| format!("{d:.3?}"))
        );
        println!(
            "controls: {}; seq holes: {}; last seq: {}",
            self.controls,
            self.holes,
            self.last_seq
                .map_or_else(|| "none".to_string(), |s| s.to_string())
        );
    }
}

fn seq_label(seq: Seq) -> String {
    if seq == Seq::SYNTHETIC {
        "synthetic".to_string()
    } else {
        seq.0.to_string()
    }
}

/// One line per control kind; the Gap span is the part that matters.
fn describe_control(kind: &ControlKind) -> String {
    match kind {
        ControlKind::Gap { provider, span, .. } => format!(
            "Gap provider={provider} span=[{}, {})",
            span.from_source_ts.0, span.to_source_ts.0
        ),
        ControlKind::SubscriptionChanged {
            provider,
            instrument,
            kind,
            active,
        } => format!(
            "SubscriptionChanged provider={provider} {} {kind:?} active={active}",
            instrument.symbol()
        ),
        ControlKind::ProviderConnected { provider } => {
            format!("ProviderConnected provider={provider}")
        }
        ControlKind::ProviderDisconnected {
            provider,
            reason,
            cause,
        } => format!("ProviderDisconnected provider={provider} cause={cause:?} reason={reason}"),
        ControlKind::ProviderError { provider, message } => {
            format!("ProviderError provider={provider} message={message}")
        }
        ControlKind::SessionClosing => "SessionClosing".to_string(),
    }
}

fn wall_clock_ns() -> i64 {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock before the epoch")
            .as_nanos(),
    )
    .expect("fits an i64 until 2262")
}

/// Positional arguments, every one optional (no `clap` dependency for an
/// example; `datamancerd`'s `ws_consumer` takes the same approach).
struct Args {
    socket: std::path::PathBuf,
    provider: String,
    asset_class: AssetClassCfg,
    symbol: String,
    kind: EventKindCfg,
    lookback_s: i64,
    run_s: u64,
}

const USAGE: &str = "usage: backfill_tape [SOCKET [PROVIDER [ASSET_CLASS [SYMBOL [KIND [LOOKBACK_S [RUN_S]]]]]]]\n\
    defaults: <platform control socket> alpaca equity AAPL trade 90 25\n\
    asset classes: equity, crypto; kinds: trade, quote, bar1s, bar1m, bar5m, bar15m, bar1h, bar1d";

impl Args {
    fn parse() -> Result<Self, Box<dyn std::error::Error>> {
        let argv: Vec<String> = std::env::args().skip(1).collect();
        if argv.iter().any(|a| a == "-h" || a == "--help") {
            println!("{USAGE}");
            std::process::exit(0);
        }
        let socket = match argv.first() {
            Some(s) => std::path::PathBuf::from(s),
            None => datamancer_client::default_control_socket()
                .ok_or("no default control socket on this platform; pass one")?,
        };
        let provider = argv.get(1).cloned().unwrap_or_else(|| "alpaca".to_string());
        let asset_class = parse_wire(argv.get(2).map_or("equity", String::as_str), "asset class")?;
        let symbol = argv.get(3).cloned().unwrap_or_else(|| "AAPL".to_string());
        let kind = parse_wire(argv.get(4).map_or("trade", String::as_str), "event kind")?;
        let lookback_s: i64 = argv.get(5).map_or(Ok(90), |s| s.parse())?;
        let run_s: u64 = argv.get(6).map_or(Ok(25), |s| s.parse())?;
        if lookback_s <= 0 || lookback_s.checked_mul(NANOS_PER_SECOND).is_none() {
            return Err(format!(
                "lookback must be a positive number of seconds that fits in nanoseconds, \
                 got {lookback_s}\n{USAGE}"
            )
            .into());
        }
        Ok(Self {
            socket,
            provider,
            asset_class,
            symbol,
            kind,
            lookback_s,
            run_s,
        })
    }
}

/// Parse a wire-named enum (`equity`, `bar1m`, …) through its serde form, so
/// the accepted names are exactly the control protocol's.
fn parse_wire<T: serde::de::DeserializeOwned>(
    name: &str,
    what: &str,
) -> Result<T, Box<dyn std::error::Error>> {
    serde_json::from_value(serde_json::Value::String(name.to_string()))
        .map_err(|_| format!("unknown {what} {name:?}\n{USAGE}").into())
}
