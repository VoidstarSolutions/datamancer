# IBKR Provider: TWS/Gateway ingestion, futures identity, and per-surface health

**Date:** 2026-09-23
**Status:** Draft design — not yet approved, not yet planned

## Motivation

Datamancer serves Alpaca equities and Alpaca crypto. Every downstream consumer
in the suite (the signal engine, the execution app)
is therefore limited to what Alpaca covers, which excludes the entire futures
complex. IBKR is the second provider, and it has been the *planned* second
provider since 2026-07-05 — the app-facing daemon design reserved its shapes in
core rather than building it
([appendix](2026-07-05-app-facing-daemon-design.md#appendix-ibkr-constraints-recorded-not-built)).

The goal is narrow and should stay narrow: **IBKR becomes an option alongside
Alpaca.** Nothing a consumer observes changes except that more instruments are
reachable. That framing is what makes most of the decisions below fall out — see
[Consumers](#consumers) and [No breaking changes required](#no-breaking-changes-required).

Those reservations already exist and are golden-tested:

- `ProviderCredentials::Gateway { host, port, client_id }` — a secret-free
  credential shape (`datamancer-core/src/credentials.rs`).
- `ProviderState::{CompanionUnreachable, Unauthenticated}` — companion-process
  health states nothing currently produces (`datamancer-core/src/health.rs`).
- `Provider::capabilities`' doc comment already names `reqContractDetails` as
  the per-contract lookup this method exists for.

This design turns those reservations into a working provider. It is also a
deliberate transfer of hard-won operational knowledge: a prior Python project (private) has run IBKR ingestion in production against
a live account, and its incident reports encode failure modes that are cheap to
inherit and expensive to rediscover. Where a rule below looks over-specified,
it is because something already broke that way.

## Upstream library

**`ibapi` (`rust-ibapi`) 4.2.0**, MIT, `github.com/wboayue/rust-ibapi`.

| Property | Value | Why it matters |
| --- | --- | --- |
| Execution model | **async by default** (tokio tasks + broadcast channels); `sync` feature for blocking | Matches datamancer's tokio architecture with no bridging layer |
| Subscriptions | streams (async) / iterators (sync), auto-cancelled on drop | Maps onto `LiveHandle` drop semantics directly |
| Connect | `Client::connect(address, client_id)` | Maps 1:1 onto `ProviderCredentials::Gateway` |
| Reconnect | built in, 20 attempts by default | Supplements — does not replace — our own policy (see [Reconnect](#reconnect-and-liveness)) |
| Release cadence | 4.0.0 on 2026-09-04, 4.2.0 on 2026-09-21 | Actively maintained, but a *young* major — pin exactly and treat upgrades as reviewed changes |

The Python prior art uses `ib_async`, not this crate. The two speak the same
TWS socket protocol, so the *protocol-level* lessons below transfer verbatim
while the *library-level* ones (`ib_async`'s `RequestTimeout = 0`, its
`hash(contract)` ticker dedup) must be re-verified against `ibapi` rather than
assumed.

## Decisions

| # | Question | Decision |
|---|----------|----------|
| 1 | Connection target | **Attach only.** Datamancer connects to a user-run TWS or IB Gateway at a configured `host:port`. It never launches, logs into, or authenticates a gateway, and never handles an IBKR username/password. |
| 2 | Order capability | **None, structurally.** Datamancer implements no order path — no method, no wire message, nothing to misroute. That is the guarantee datamancer itself can make. It **cannot** make IBKR refuse orders per connection: `rust-ibapi` has no read-only connect option, and TWS's "Read-Only API" is a *global* gateway setting the operator owns (on by default). See [Security](#security). |
| 3 | `Instrument` identity | **Stays opaque**, per the recorded constraint. Symbol→contract resolution (conId, exchange, currency, expiry, multiplier) lives entirely inside the provider. Adds `AssetClass::Future` (additive: the enum is `#[non_exhaustive]`). |
| 4 | Continuous contracts | **Provider-resolved, provenance-reported.** A bare root symbol (`"ES"`) resolves to the front month by one rule; an explicit contract (`"ESZ6"`) is used as given. Which contract answered is reported, never inferred by the consumer. |
| 5 | Live data surface | `reqTickByTick` (Trade + BidAsk) → `MarketEvent::{Trade, Quote}`. |
| 6 | Five-second realtime bars | **Deferred.** IBKR's realtime bar is 5 s and nothing else, and `BarInterval` has no `FiveSecond`. Adding one is a declared break — and it is **not needed** for IBKR to be a complete option (see [No breaking changes required](#no-breaking-changes-required)). |
| 7 | Market depth (L2) | **Out of scope.** No consumer has asked for it; `reqMktDepth` has its own subscription accounting and a book-maintenance model the event model does not express. Revisit when a consumer needs it. |
| 8 | Health granularity | **Per-surface, additively.** `Surface::Live` and `Surface::History` fail independently on IBKR and must be reported independently — as a new optional field on the already-`#[non_exhaustive]` `ProviderHealth`, with the existing aggregate `state` retained unchanged (see [Per-surface health](#per-surface-health-the-hmds-lesson)). |
| 9 | Pacing | **Inside the provider**, as a real pacer, not ad-hoc sleeps. `Provider`'s doc comment already sanctions this ("the provider MAY hold an internal pacer so rate-limiting stays inside the provider"). |
| 10 | Client-id policy | Configured, not assigned. Datamancer validates uniqueness within its own process and surfaces a collision as a distinct state, but cannot see other programs on the host (see [Client-id lease](#client-id-is-an-exclusive-lease)). |
| 11 | Paper vs live | Enforced by a port↔flag interlock that fails **before** the first connect. |

## No breaking changes required

**IBKR ships as a purely additive option.** This is a deliberate constraint, not
a happy accident: datamancer is a standalone product whose contract is to feed
data out according to its specs, and adding a source must not perturb that
contract for anyone already consuming it.

Everything IBKR needs is already additive:

| Need | Mechanism | Breaking? |
| --- | --- | --- |
| Futures asset class | `AssetClass::Future` — the enum is `#[non_exhaustive]` | No |
| Gateway credentials | `ProviderCredentials::Gateway` — already exists | No |
| Companion health states | `ProviderState::{CompanionUnreachable, Unauthenticated}` — already exist, gain producers | No |
| Per-surface health | new optional field on `ProviderHealth`, which is `#[non_exhaustive]`, with `#[serde(default, skip_serializing_if = …)]` | No |
| Live trades and quotes | `reqTickByTick` → existing `Trade` / `Quote` | No |
| Historical bars | `reqHistoricalData` at intervals `BarInterval` already has | No |

The one thing that *would* break — `BarInterval::FiveSecond` for IBKR's
realtime bars — is **cut from scope**, because IBKR is a complete and useful
option without it. Tick-by-tick carries the live surface at finer granularity
than a 5-second bar, and the six existing historical intervals (`OneSecond`,
`OneMinute`, `FiveMinute`, `FifteenMinute`, `OneHour`, `OneDay`) cover the
common cases — with two real gaps recorded under [Scope](#scope), neither of
which changes the additive conclusion. `Surface`'s split is exactly what lets the provider
under-promise cleanly here: it answers `true` for Live on `Trade`/`Quote` and
`true` for History on the intervals it serves, and simply never advertises a
realtime-bar kind it cannot express.

### The breaking batch, when it comes

`BarInterval::FiveSecond` and the perpetual-futures kinds (funding rate, open
interest, mark/index price) are both real future extensions, both breaking, and
both should land in **one** release rather than separately — each one costs both downstream consumers a re-pin. But that batch is now **decoupled from IBKR**
and should be driven by actual demand rather than by this provider.

Worth knowing when it does come due: both downstream consumers are well behind — one tracks `branch = "main"`
with a lock from 2026-07-22 (45 commits back), the other pins 0.8.0 from
2026-07-19 (91 commits back). Neither has picked up the historical-query
surface. A coordinated re-pin is owed regardless of IBKR.

## Instrument identity and the roll rule

`Instrument` stays the opaque `(provider, asset_class, symbol)` triple. IBKR's
structured contract is resolved inside the provider and never leaks into the
event stream — that is what keeps output source-agnostic.

Symbol grammar for `AssetClass::Future`:

| Form | Meaning |
| --- | --- |
| `"ES"` | root symbol → resolve to the **front month** by the provider's single roll rule |
| `"ESZ6"` | explicit contract → use as given |

**One roll rule, in one place.** The Python project ran three independent
"front month" definitions and spent a roll week quoting `MNQU6` against
`MNQZ6` brackets, which also produced a false "the broker missed our fill"
diagnosis. The fix there was a single `front_month.py` imported by every
satellite. Here the equivalent is: exactly one resolution function inside the
provider, and the resolved contract is **reported with its provenance** (which
rule chose it) rather than being invisible.

Roll rules are per-instrument-family, not universal — the Python project needed
five (`quarterly` with a 7-day buffer, `monthly` forward-walking to a delivery
month ≥20 days out, `bimonthly`, an irregular 8-month cycle, and an FND-based
one). A naive "third Friday, roll if past the 15th" heuristic returned an
*expired* contract for roughly five days a month. Whatever subset we ship, the
cycle must be an explicit, validated per-instrument field — never inferred.

**Qualification is not tradeability.** IBKR qualified a contract two days after
it expired, with `lastTradeDateOrContractMonth` still populated; the failure
surfaced much later as "no historical data." Every resolved contract gets a
post-resolution expiry check, which **fails open** on an unparseable date (a
false positive would reject a legitimate instrument).

## Ingestion correctness rules

These are the rules that decide whether the data is right. Each one has already
caused a production incident in the prior art.

### Timestamps

Datamancer's model structurally prevents the worst of it: `source_ts` is
`Timestamp(i64)` nanoseconds since the Unix epoch, and `rx_ts` is separate and
observability-only. There is no place to put an OS-local, zone-tagged
datetime — which is exactly the bug that cost the Python project four
false-triggered bracket orders when its library returned OS-local-tagged bar
times that downstream code read as New York time.

The remaining obligations are at the parse boundary:

- Parse IBKR timestamps as **UTC**, explicitly, at the decode site. Never via a
  local-timezone conversion, and never as a naive value.
- **`endDateTime` on a historical request must be UTC**, never a named zone. A
  named zone is accepted for some contracts and *silently returns an empty bar
  list* for others.

### Bars are start-stamped, and the last one is a lie

Two rules, both non-obvious:

1. **Drop the in-progress bar.** A historical request with an empty
   `endDateTime` still returns the currently-forming bar. A bar is complete iff
   `open_time + interval <= now`; a bar closing exactly now is kept. Without
   this, datamancer emits a bar that later changes value — which is
   unrepresentable in a stream whose `seq` is stamped once and never revised.
2. **Select by the bar's own timestamp, never by request parameters.** Asking
   for data up to a cutoff returned data well past it. Because bars are
   *start-stamped*, the filter is `start + interval <= cutoff` — filtering on
   `start` keeps a bar whose close is after the cutoff. And compare real
   timestamps, not formatted strings: a bar ending at "00:00" is the
   lexicographic minimum and passes every string-compared morning cutoff.

Datamancer's cache-coverage model already refuses to record a range as covered
unless its fetch completed, marking the remainder with an in-band
`Control::Gap`. That aligns with the prior art's strongest rule — *refuse to
cache a gap* — and should not be weakened for IBKR.

### Never substitute a value from a different time

The prior art's most insidious bug class: after a session close, a live quote
field goes `NaN`, code falls back to a "close" field, and that value is
rendered as a live price — or worse, used as *both* the current and the prior
close, making every computed delta exactly zero. A whole display looked
plausible and was meaningless.

For datamancer this is a hard rule with a clean home: `Provider::latest`
returns `Option<MarketEvent>`, so a provider with nothing current to report
returns **`None`**. It never substitutes a stale value to avoid an empty
answer. `NaN` never reaches an event — a non-finite field is a decode failure,
not a value. (Fixed-point `Price`/`Quantity` means `NaN` cannot survive
conversion anyway; the check belongs at the decode boundary, before
conversion.)

### Pacing

The Python project had no central pacer, and paid for it: a sleep computed from
the client id turned an *identity number* into a *latency dial*, producing a
40-second pre-fetch delay on high-numbered ids and IBKR error 202 order
rejections on a live account — twice.

The IBKR provider gets a real pacer: a token-bucket over historical requests
sized to IBKR's documented limits (identical-request and per-window), with
per-request timeouts armed on **every** request. Backoff on a pacing violation
is longer than on an empty result, which is longer than the inter-page delay.
None of this is visible outside the provider; `Provider::metrics`'
`record_rate_limit` is how it surfaces.

## Per-surface health (the HMDS lesson)

This is the one place IBKR forces a change beyond additive variants, and it is
worth taking seriously.

On 2026-08-12 the prior art's IB Gateway came back from its nightly restart
**half-alive**: authenticated, API socket serving, order routing fine, account
data fine, live market data fine, contract lookups fine — and the historical
data farm never connected. Every historical request timed out. The result was a
full-day data blackout that ran **14+ hours undetected**, because the health
check was "socket open + server time responds + dashboard renders," all of
which a Gateway in that state passes.

The lesson is general: **liveness is not data-path health; probe the path you
actually depend on.**

Datamancer already has the right axis — `Surface::{Live, History}` exists
precisely because the two data paths genuinely differ and neither contains the
other. But `ProviderState` is currently a *single* state per provider, so a
provider whose history surface is dead and whose live surface is healthy has no
way to say so.

**The fix is additive.** `HealthView`, `DaemonHealth`, `ProviderHealth`, and
`StreamHealth` are all `#[non_exhaustive]`, so external crates can neither
construct them by literal nor destructure them exhaustively — which makes adding
a field a non-breaking change. `InstrumentInfo.capabilities` is the existing
precedent for exactly this pattern:

```rust
#[non_exhaustive]
pub struct ProviderHealth {
    pub provider: ProviderId,
    /// Unchanged: the aggregate, worst-of across surfaces. Existing consumers
    /// keep working with no edit.
    pub state: ProviderState,
    pub detail: Option<String>,
    /// NEW. `None` = the provider does not distinguish its surfaces (Alpaca) or
    /// the daemon predates this field. Never means "healthy".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub surfaces: Option<SurfaceHealth>,
}
```

Retaining `state` as the worst-of aggregate is what keeps this non-breaking: a
consumer that only renders one indicator per provider sees the history outage
reflected there, and a consumer that wants to distinguish reads `surfaces`.
`schema_version` stays **2** — an additive optional field is not a breaking
change to the reduction, so no consumer is forced to act.

Two further obligations:

- The IBKR provider's health check for `Surface::History` is an **actual
  bounded historical request**, not a connection check. Anything less does not
  distinguish the failure that motivated this.
- `CompanionUnreachable` and `Unauthenticated` finally get producers:
  unreachable gateway and lapsed daily re-auth / 2FA respectively.

## Security

Datamancer never sees an IBKR credential — the gateway is logged in
out-of-band. What it does own:

| Control | Rule |
| --- | --- |
| Order capability — ours | No order code path exists in the provider. `ibapi::Client` always exposes `place_order` / `submit_order` / `cancel_order` / `global_cancel`; the provider wraps the client and never calls them, and a test guards that no order method is reachable. |
| Order capability — gateway | TWS "Read-Only API" (Global Configuration → API → Settings) is **on by default** and blocks every API order from every client. Keep it on for a gateway that serves only datamancer. A gateway shared with an execution app necessarily has it **off** — there, datamancer's guarantee is structural only, never broker-enforced. |
| Network | Loopback by default. A non-loopback gateway host is permitted but warns loudly: the TWS API socket has **no authentication and no TLS**, so reaching it across a network exposes an unauthenticated trading interface. |
| Paper vs live | A `paper`/`live` flag cross-checked against the configured port (paper 7497/4002, live 7496/4001). A mismatch is a hard startup failure **before the first connect**, never an interactive prompt. |
| Account assertion | Optionally assert the connected account matches an expected prefix or value, so a misconfigured port that happens to be open cannot silently deliver live-account data. |
| Credential shape | `ProviderCredentials::Gateway { host, port, client_id }` — already secret-free, so the broker stores no secret for IBKR and `Debug` has nothing to redact. |

One belief from the prior art does **not** transfer and is recorded here so it is
not re-inherited: its satellites connect with `ib_async`'s `readonly=True` and
describe that as a structural guarantee that "IBKR itself" refuses their
orders. That flag is a client-side hint (it skips order-related startup
requests); the broker-side enforcement is the TWS global setting above, and
`rust-ibapi` exposes no equivalent flag at all. Read-only is an **operator
property of the gateway**, not something a connection can assert.

### Client id is an exclusive lease

IBKR permits one session per client id. On a duplicate connect the
**incumbent survives** and the newcomer is refused with error 326. The symptom
is therefore a *silent no-show of whoever connected second*, with the cause
nowhere near the symptom — the prior art built a whole cross-repo registry and
a test that reads the neighbouring program's registry to prevent exactly this.

Datamancer cannot see other programs on the host, so it cannot own the
allocation. What it must do:

- Validate that no two configured IBKR providers within one datamancer process
  share a client id — a fast, local, complete check.
- Surface error 326 as a **distinct, actionable state** (`Unauthenticated` is
  wrong; this is a collision) with the offending client id in the detail field.
  Folding it into a generic disconnect-and-retry reproduces the original
  failure: an endless quiet retry loop against an id that will never be free.
- Document the id as an operator-allocated resource, and document the range
  datamancer uses, so a host shared with other IBKR programs can be fenced.

## Reconnect and liveness

`ibapi` reconnects on its own (20 attempts by default). That is not sufficient
on its own, for two reasons the prior art documents:

1. **Half-open sockets.** After a gateway-side reset, the connection reported
   healthy, no exception was raised, and a single request hung for **five-plus
   hours** because the library's default request timeout meant "wait forever."
   Every request needs an armed timeout; a request that times out while the
   connection claims health is itself a health signal.
2. **Giving up is worse than retrying.** The prior art's mature loop retries
   forever with a capped backoff (quick twice, then 10/10/20/30 s), on the
   explicit reasoning that "giving up at 3 a.m. recreates the failure it exists
   to prevent." A nightly gateway restart is a *standing* kill window, not an
   exception.

Both map onto existing datamancer machinery: reconnects surface in-band as
`Control::ProviderDisconnected` / `ProviderConnected` and are counted in
`ProviderSnapshot.reconnects`. No new mechanism is needed — only the policy.

## Consumers

Datamancer is a standalone, independently compilable and testable product. Its
obligation is to retrieve, validate, normalize, and serve data according to its
published contract — the per-symbol `(instrument, seq)` ordering, the timestamp
triple, the in-band `Control` vocabulary, and the stable error codes. What is
done with the data afterwards is not its concern: **the signal engine** owns processing
and signal generation, **the execution app** owns execution. Adding IBKR must change
nothing any of them observe except that more instruments become available.

That is the real reason this spec cuts `BarInterval::FiveSecond`: "IBKR is now
an option alongside Alpaca" is a statement about the *provider registry*, not
about the event contract. A new source that perturbs the contract for existing
consumers has failed at being an option.

A third consumer shape is planned: a **native desktop UI for datamancer itself**,
built from `void_ui`'s Linebender/xilem/masonry/Vello elements, displaying
datamancer's data independently of the signal engine. Two things follow:

- That UI is *not* the existing `web-ui` feature (axum + maud over loopback
  HTTP). They are different stacks with different purposes; `void_ui` is
  explicitly not-web and its elements do not reuse into a browser product. The
  two should be expected to coexist, not converge.
- Its integration path already exists and needs no new protocol:
  `datamancer-client`'s `app` feature — `AppHandle::ensure` for
  find-or-spawn-and-connect, `watch_health()` for the health push plane. That
  facade was designed for exactly this consumer.

The Windows query gap matters most here. A UI that renders a chart wants
history, and on Windows `AppHandle` cannot run one.

## Scope

**In:** futures (`AssetClass::Future`) and the equities IBKR already serves,
live trades and quotes via tick-by-tick, historical bars with paging and pacing
at the intervals `BarInterval` already carries, contract resolution with a roll
rule, `list_instruments` / `capabilities` via `reqContractDetails`, per-surface
health, the security controls above.

**Out:** order placement of any kind (structurally — no order path exists); market
depth / level 2; options; fundamentals; news; account and position data;
anything that would make datamancer an execution path. The execution app owns
execution; datamancer produces events.

**Deliberately deferred:** five-second realtime bars (`reqRealTimeBars`), which
would need a breaking `BarInterval::FiveSecond`; and historical *ticks*
(`reqHistoricalTicks`). Both are real IBKR capabilities with no consumer demand
yet.

**Two honest gaps that follow from shipping additive:**

- **No live bars in v1.** Alpaca streams minute and daily bars on
  `Surface::Live`; under this spec IBKR answers `false` for every `Bar(_)` on
  that surface, so a consumer subscribed to *live bars* cannot switch providers
  even though one subscribed to trades or quotes can. The clean resolution is
  provider-internal aggregation of IBKR's 5-second realtime bars into the
  existing intervals — that is normalization at the edge, not semantic
  enrichment, it exposes no new kind, and it stays additive. Whether cycle 3
  takes it on is a scope decision, not an architecture one.
- **Missing historical intervals.** IBKR serves 30-minute, 4-hour, and weekly
  bars that `BarInterval` cannot express, and the prior art actually traded on
  4-hour bars. A consumer wanting those
  gets nothing. Adding intervals is breaking for the same reason `FiveSecond`
  is, so they belong in the same deferred batch.

## Delivery

Every cycle is additive and independently shippable; nothing here blocks on a
cross-consumer coordination step.

1. **Cycle 1 — per-surface health.** The additive `ProviderHealth.surfaces`
   field, its reduction, and golden tests. Provider-agnostic and useful on its
   own (Alpaca reports `None`), so it lands before any IBKR code and is not
   held hostage by it.
2. **Cycle 2 — connect, identity, capability.** Attach to a gateway, the
   security interlocks, client-id validation, contract resolution and the roll
   rule, `list_instruments` / `capabilities`, and the real historical health
   probe. No market data yet — this cycle's deliverable is "datamancer can tell
   you truthfully what it can and cannot serve," which is the part of the data
   contract that must be right before any bytes flow.
3. **Cycle 3 — live.** Tick-by-tick trades and quotes, the reconnect policy,
   `latest`.
4. **Cycle 4 — history.** Paged historical bars, the pacer, cache integration,
   the in-progress-bar and cutoff rules, the backfill→live seam.
5. **Cycle 5 — daemon integration.** `compiled_provider_ids()`, the
   `[provider.ibkr]` config section, `ConfigHub` wiring, the `gateway`
   credential path through the broker, and daemon e2e coverage.

`AssetClass::Future` lands in cycle 2 with the contract resolution that needs
it.

## Testing

The prior art's 735 tests run **fully offline** by mocking the broker library
at import time, with business logic deliberately isolated from it. The same
split applies here, and datamancer's existing structure already supports it:
`Provider` is the seam, and the test fakes in `datamancer-core` show the shape.

- **Offline by default.** Decode, contract resolution, roll rules, the
  in-progress-bar rule, cutoff filtering, expiry checks, and pacing are pure
  functions over fixtures and need no gateway.
- **Fixture-driven decode.** Captured IBKR payloads, including the pathological
  ones: the expired-but-qualified contract, the named-zone empty response, a
  bar set whose last entry is in progress.
- **One `#[ignore]`d live suite**, in the shape of `alpaca_real.rs`, run
  against a paper gateway by hand.
- **A roll-boundary test that actually crosses a roll** — the prior art's roll
  bug survived because nothing tested the week the contract changed.

## Open questions

1. **What exactly does `SurfaceHealth` hold?** The additive shape is settled
   (optional field, aggregate retained, `schema_version` unchanged); the inner
   type is not. Minimally a `ProviderState` per `Surface` plus the last probe
   time. Cycle 1 designs it.
2. **Which roll cycles ship first?** Quarterly (equity index) covers the most
   likely first consumer; the others are additive but each needs its own
   boundary test.
3. **Does `ibapi` 4.x expose a per-request timeout?** The prior art's
   wait-forever default is library-specific and must be verified, not assumed.
4. **Windows.** IBKR ingestion is most likely to be *operated* on Windows,
   where the daemon has no iceoryx2 node and historical queries answer
   `unsupported_on_windows`. This is not an IBKR problem and should not be
   solved inside this spec, but an IBKR provider whose history surface is
   unreachable through the daemon on its most likely platform is a real gap —
   and it becomes load-bearing the moment a native UI wants to draw a chart
   (see [Consumers](#consumers)).

**Resolved during drafting:** whether IBKR forces a breaking release. It does
not — see [No breaking changes required](#no-breaking-changes-required).

## References

- [App-facing daemon design, appendix](2026-07-05-app-facing-daemon-design.md) — the original IBKR constraint record
- `datamancer-core/src/credentials.rs`, `health.rs`, `traits/provider.rs` — the reserved shapes
- `crates/datamancer/src/providers/alpaca.rs` — the provider implementation template
- The prior art is a private production Python project; its porting guide and incident reports are the inputs to the ingestion rules above and are summarized in this document rather than linked.
