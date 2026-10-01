# Provider toggles, the availability contract, and the composition boundary

**Date:** 2026-09-23 (UI stack decided 2026-09-24)
**Status:** Draft design — establishes the layering model and records where the
implementation does not yet deliver it

## The model

Three layers, with a hard boundary between each:

| Layer | Owns | Answers |
| --- | --- | --- |
| **datamancer** | availability + the data contract | *what data exists, and is it trustworthy?* |
| **UI** (void_ui-derived) | composition + presentation | *how do I want to look at it?* |
| **signal engine** (a private sibling repo) | processing + signal generation | *what does it mean?* |

Datamancer's job stops at "here is the data, categorized, ordered, and honest
about its own gaps." It does not decide what the data means, and it does not
decide how the data is arranged on a screen.

**Providers are toggles.** If Alpaca is on, Alpaca's data is available. If IBKR
is on and its gateway is reachable, IBKR's data is available. Off means gone.
The toggle is per-instance and persistent: once flipped, it stays flipped until
someone flips it back, across restarts, with no config-file editing.

**The UI composes freely over that.** A page showing only Alpaca. A page showing
only IBKR. A page blending both. A page that does heavier derivation and
visualization. None of those are datamancer's business — they are views over a
availability surface that does not change shape depending on who is looking.

**The signal engine pings for categories.** Whatever `(kind, adjustment, surface)`
combinations datamancer publishes, the engine can request. The category space *is*
the contract.

This document exists because the layering is agreed but the **implementation
delivers it on one axis and not the other**, and that gap is invisible until you
try to build the UI.

## Where the implementation already delivers this

More than you would expect. The toggle model is not aspirational at the provider
level — it is how the daemon already works:

- Every compiled-in provider is constructed at boot as a fixed set, and each
  starts **disabled** unless its `[provider.*]` section is present.
- `SettingsSource::Watch(None)` *parks* a provider: disabled, but not torn down.
- `configure-provider` / `remove-provider` flip that at runtime. `provider.` is
  the **only** `Hot`-classified surface in `config_class.rs` — every other
  section is `Cold` (persisted, applied at next boot).
- The daemon is the sole runtime writer of its own config and persists it
  atomically, so a toggle survives restarts without a human editing TOML.
- `ProviderState::Disabled` is a first-class health state, explicitly "not an
  error."
- Credentials hot-apply too: `set-credentials` reaches a running provider.

That is precisely "on until turned off, per instance." Nothing needs redesigning
there.

The other half that already holds is **source-agnostic output**. Once an event
leaves datamancer it is indistinguishable across providers — same `MarketEvent`
shapes, same timestamp triple, same `Control` vocabulary, same fixed-point
`Price`. That property is what makes a blended view *possible at all*; without
it, "blend Alpaca and IBKR on one page" would be a per-provider special-casing
exercise in the UI.

## Where it does not: the symbol axis is cold

**"Provider on ⇒ data available" is true at the provider level and false at the
symbol level.** Toggling a provider on makes its instruments *subscribable*. It
does not make any data flow. Data flows when something holds a subscription, and
there are exactly two ways to hold one:

| Mechanism | Applies live? | Persistent? | Shared? |
| --- | :-: | :-: | :-: |
| `subscribe` (per-client control op) | ✅ | ❌ dies with the client | ❌ client-scoped |
| `[[startup_session]]` with `always_on` | ❌ **restart required** | ✅ | ✅ daemon-wide |

`startup_session.` is classified `Cold`. There is no control op that pins a
symbol persistently at runtime — the op vocabulary covers
`open-client`/`subscribe`/`unsubscribe`/`close-client`, queries, discovery,
credentials, config, and health, and nothing else.

So the honest current statement is: **the provider dimension is a hot,
persistent, per-instance toggle; the symbol dimension is either ephemeral or
requires a restart.** There is no third option.

**And one place where even the provider toggle leaks — issue #64, Bug A.** A
parked provider is supposed to be *gone*. But `instrument_catalog(None)` — the
unfiltered `instruments` op — iterates every *registered* provider rather than
every *enabled* one, and `?`-propagates the first error. So a compiled-in
provider parked without credentials answers "Trading client not initialized"
and takes the entire catalog down with it, including every healthy provider's
rows. That is precisely the failure the toggle model exists to prevent: off
must mean *absent*, never *poisoning*. The library already has the signal it
needs — `Provider::enabled()` reports `false` for a `Watch(None)`-parked
provider — the catalog loop simply does not consult it. The fix belongs in the
library (skip `!enabled()` providers; tolerate a per-provider failure with a
warning instead of failing the whole catalog), not the daemon, so the
in-process embedder path is fixed by the same change.

This is the gap that makes the UI vision not yet buildable as drawn. The
prototype's Config screen labels *Subscribed symbols* as "Applies live" next to
*Endpoint*'s "Needs restart" — that reads as a persistent symbol set applied
hot, which does not exist. A UI page that says "here is Alpaca, and here is data
on it" needs symbols warm and persistent without the operator restarting a
daemon to add one.

### What closing it requires

A `Hot`-classified persistent session set, reachable from the control surface —
conceptually "pin `(instrument, kind, scope, persistence)` for this instance
until unpinned," replacing boot-only `[[startup_session]]` as the mechanism for
a warm symbol set. Three things fall out of that and need deciding:

1. **Reclassifying `startup_session.` from `Cold` to `Hot`**, or introducing a
   separate pinned-session surface and leaving the boot anchors alone. The
   former is fewer concepts; the latter avoids re-litigating boot semantics.
2. **Lifecycle against clients.** An always-on anchor and a client subscription
   already share the authoritative session by refcount, so the machinery exists;
   what is new is a *runtime-mutable* set of refcount holders that the daemon
   owns.
3. **Cost visibility.** Provider toggles are cheap; symbol pins are not. A
   pinned set is open sockets, tap-log writes, and cache growth. The UI that
   makes pinning one click needs to show what the pin costs.

## The category contract

"Whatever raw-or-adjusted data we have categories for" deserves an exact answer,
because it is smaller than it sounds.

The full category space today is the cross-product of:

- **`EventKind`** — `Trade`, `Quote`, and `Bar(interval)` over six intervals
  (`OneSecond`, `OneMinute`, `FiveMinute`, `FifteenMinute`, `OneHour`,
  `OneDay`). **Eight kinds, total.**
- **`Adjustment`** — `Raw`, `Split`, `Dividend`, `SpinOff`, `All`. Applies to
  bars; trades and quotes always store `Raw`.
- **`Surface`** — `Live` and `History`, answered **independently** per provider.
  Neither contains the other.

Two consequences worth stating plainly:

**The category space is closed and growing it is breaking.** `EventKind`,
`BarInterval`, and `Surface` are deliberately *not* `#[non_exhaustive]` —
that is what forces every provider to consciously answer a new category rather
than silently inheriting a wildcard. The cost is that every new category is a
declared breaking change. Things not currently expressible: market depth /
order book, funding rate, open interest, mark and index price, fundamentals,
corporate actions as events, and any bar interval outside the six.

**Availability is per-(provider, instrument, kind, surface), not per-provider.**
`supports()` is answered on that full tuple. So "IBKR is on" does not mean "IBKR
serves everything" — it means IBKR now answers capability questions truthfully.
The `instruments` and `capabilities` control ops exist precisely so a UI or
the engine can ask rather than assume, and an absent capability field means
**unknown**, never "unsupported."

That honesty is the feature. A toggle that made a provider claim blanket
availability would push the failure from a capability query down to a failed
subscription at market open.

## What the UI layer has to decide (and datamancer should not)

Composition raises questions that have no source-agnostic answer, so datamancer
deliberately does not answer them:

**Collision.** `Instrument` is the qualifying tuple `(provider, asset_class,
symbol)`, so Alpaca's AAPL and IBKR's AAPL are different instruments and both
can be live at once. There is no routing layer, no preference order, and no
notion of a canonical AAPL. A blended page must decide: show both, pick one by
operator preference, or prefer-with-fallback. All three are legitimate; none
belongs in datamancer, because a cross-provider canonicalization is explicitly a
non-goal.

**Ordering across sources.** Ordering is per-symbol only — the key is
`(instrument, seq)`, monotonic within an instrument and arrival-order across
them. A blended view showing two providers' trades interleaved is showing an
arrival-order interleave, not a merged market timeline, and must not imply
otherwise. Consumers needing strict cross-source time order buffer and sort
themselves.

**Aggregation.** Any cross-instrument aggregate is the UI's or the signal
engine's, never datamancer's — and this is not a hypothetical conflict. The
signal engine's own architecture document (in its private repo) assigns
aggregate and cross-instrument signals — sector strength, breadth, pairs,
correlation — to datamancer, re-entering the engine as input streams, and it
has deliberately kept its actors per-instrument on that assumption, listing a
cross-actor layer only as a possible later expansion. Datamancer's standing
non-goals say it never will. One of the two documents is wrong about who owns
aggregates, and it has to be settled *before* the engine needs its first
breadth or correlation signal — the options are: datamancer's non-goal changes
(a large scope shift); the engine's document changes and it grows a cross-actor
layer; or a third component between them owns it. This spec's position is that
it is not datamancer, but that is a position, not a decision.

## Provider growth

The prototype shows four providers — Alpaca, IBKR, Polygon, Databento — with an
"+ Add provider" affordance. Two things about that:

Adding a provider is a **Rust implementation plus a release**, not a runtime
action. `compiled_provider_ids()` is a hardcoded list; a provider is a
`Provider` trait implementation compiled into the binary behind a cargo feature.
The UI affordance should therefore read as *enable a provider this build
supports*, not *register an arbitrary new feed* — otherwise it promises
something the architecture does not do.

That said, the trait boundary is genuinely additive: a new provider depends on
`datamancer-core` alone, never the orchestrator, and adding one changes no
consumer code. Polygon and Databento are plausible follow-ons to IBKR on exactly
the same seam — and Databento in particular is the obvious answer for CME
futures if the IBKR gateway dependency proves operationally annoying.

## The UI stack question

### What datamancer has today

The `web-ui` feature (**default on**): ~2,050 lines across nine files in
`crates/datamancerd/src/web/`, built on **axum 0.8 + maud 0.27** — server-rendered
HTML from Rust — with `tower-http`, `arc-swap` for the state swap, and
`tokio-stream` for SSE. Loopback-only, read-only introspection plus a JSON API
(`/api/health`, `/api/config`, `/api/cache`, an SSE `/api/stream`, and a
feature-gated Prometheus `/metrics`), on two cadences: a fast live-state/SSE swap
and a slow cache-catalog swap. `config_api.rs` (454 lines) does carry config
writes, so it is not purely read-only in practice.

It is a real but modest operator dashboard. It is not a rich client, and it is
not what the prototype depicts.

### What void_ui is

Not a style guide — a **mature native widget library**: roughly 70 components
across ~80k lines of Rust, on the Linebender stack (masonry / xilem / Vello)
via the Voidstar fork of xilem. A sibling product (private) already ships a substantial desktop app on it,
including a Vello-based charting crate with sparklines.

The component inventory maps onto this prototype almost one-to-one:

| Prototype need | void_ui component |
| --- | --- |
| Navigation spine, collapsible rail | `sidebar` + `nav_view`, `tabs`, `breadcrumb` |
| Provider on/off | `toggle` |
| Expanding provider rows | `collapsible` |
| Endpoint / credentials key-value rows | `description_list` |
| Paper/Live, parked badges | `badge`, `status_dot` |
| Per-symbol coverage / latency / gap table | `data_grid` (+ `sort`, `filter`, `expand`, `column_strip`) |
| Latency and coverage gauges | `meter` |
| High-rate event stream | `collection/` substrate (`imperative_list`, `overlay_list`, virtualized `window`, `row_click`) |
| Faceted filter bar | `autocomplete`, `dropdown_button` |
| Config editing | `form` (+ validation), `input` (`number`, `masked`, `currency`), `checkbox`, `radio` |
| TOML display | `code_view` |
| Historical range selection | `date_picker` |
| Charts and visualization | the sibling product's charting crate, already built on void_ui |

### The one hard either/or

**Rendering.** void_ui draws through masonry/Vello into a native window; Tauri
draws HTML/CSS in a webview. You cannot host void_ui widgets inside Tauri. So the
prototype *as implemented* and void_ui are genuinely exclusive **implementation**
paths — but the prototype is a design artifact, and its screens, information
architecture, and visual language transfer to either.

Everything below the presentation layer is unaffected. Both paths consume the
same thing: `datamancer-client`'s `app` feature — `AppHandle::ensure` plus
`watch_health()`. That is the seam, and it already exists.

### Why void_ui fits datamancer specifically

Beyond the component match, the **charter** lines up. `DATA_GRID_HOST_CONTRACT.md`
states the grid is presentation-only: the host owns row order, row identity,
sorting, and filtering, and the grid "renders what you hand it, emits intents,
and never reorders or hides data itself." That is precisely datamancer's
boundary — the daemon owns the data and its ordering; the UI renders and emits
intent. The same property that made void_ui work for that product makes it work here.

Two smaller fits worth noting: the grid wants a **stable `u64` row id**, and
datamancer stamps exactly that as per-symbol `seq`; and the host-owns-order rule
means a grid can never quietly re-sort a stream whose ordering contract is
per-symbol-only.

### Costs to weigh honestly

- **void_ui is a read-only team repo.** Datamancer would consume it; a missing
  component is a request to that team, not a local patch. Confirm that policy
  still holds before depending on it.
- **It tracks a fork of a fast-moving upstream.** `masonry`/`xilem` come from
  `VoidstarSolutions/xilem` on `branch = "main"`, unpinned. Datamancer's release
  discipline — release-plz-owned versions, `cargo-semver-checks`, `cargo deny`,
  lockstep pinning — is considerably stricter. A datamancer crate depending on an
  unpinned forked Linebender stack is a real reproducibility tension and needs a
  pinning story.
- **Keep it out of the daemon.** The UI should be its own crate or repo consuming
  `datamancer-client`, never a feature of `datamancerd` — otherwise
  masonry/Vello/winit land in the server's dependency tree and in every CI job.
- **The two UIs are not redundant.** `web-ui` serves the headless case (loopback
  HTTP over an SSH port-forward, no GUI, no display server); a native app serves
  the desktop case. Keeping both is defensible; the question is whether `web-ui`
  stays a first-class surface or becomes a minimal health endpoint.

### Decision (settled 2026-09-24)

**Native void_ui.** The prototype is the visual and information-architecture
reference, not the implementation. The component inventory is already built, the
presentation-only charter matches datamancer's boundary exactly, a sibling product proves
the stack at this scale, and the chart crate exists.

Consequences that follow immediately:

- The Tauri path is closed. The prototype's HTML export stays a reference
  artifact outside the repository (gitignored) and is not a build input.
- `web-ui` **stays**, scoped to the headless case — loopback HTTP over an SSH
  port-forward, no display server. It is not the desktop surface and should not
  grow toward being one.
- The UI is its own crate or repo consuming `datamancer-client`'s `app` feature.
  It is never a `datamancerd` feature: masonry/Vello/winit must not enter the
  server's dependency tree or its CI matrix.

### Fork pinning — resolved by precedent

The concern was that `masonry`/`xilem` come from `VoidstarSolutions/xilem` on
`branch = "main"`, unpinned in the manifest, which sits badly next to
datamancer's release-plz / `cargo-semver-checks` / `cargo deny` discipline.

The sibling product already solves this the ordinary way: the manifest declares
`branch = "main"` while the **committed `Cargo.lock` carries the actual pin** —
currently `xilem`/`masonry`/`xilem_masonry` 0.4.0 at fork rev `fd8a78e9`. (Only
the xilem repo itself is forked; `vello` resolves from crates.io.)

That is sufficient **because that product is an application**. A lockfile pins builds
of a binary and is ignored for library consumers. So the rule for datamancer is:

> The UI is an **application** with a committed lockfile, outside the
> release-managed crate set. It is never published as a library, and
> release-plz / semver-checks do not cover it.

Under that rule the fork tracks `main` with a reproducible lockfile pin, exactly
as it does, and no new machinery is needed. Advancing the pin becomes a
deliberate lockfile bump, reviewable like any other dependency change.

Left open: whether the UI lives in this repo as an excluded workspace member or
in its own repo. Separate-repo matches the sibling product's shape and keeps datamancer's
workspace uniformly release-managed; in-repo keeps the operator surface next to
the daemon it operates. Decide when the first crate is created.

## Open decisions

1. ~~**The UI stack.**~~ **Settled 2026-09-24: native void_ui.** See
   [The UI stack question](#the-ui-stack-question). One sub-question remains —
   whether the UI crate lives in this repo (excluded from release management) or
   in its own repo.
2. **Hot persistent sessions** — reclassify `startup_session.`, or add a
   separate pinned-session surface? (See [above](#what-closing-it-requires).)
3. **The operational event-log store.** The prototype's Event Log screen has no
   backing store. The tap log persists *data* events, not `Control` events, so
   connectivity history, gap history, and subscription changes are not queryable
   after the fact — they are in-band on a live stream and then gone. A
   filterable operational event log is a new store, not a surfacing exercise.
4. **Windows history.** Bounded historical queries answer
   `unsupported_on_windows` — the result plane rides iceoryx2 and Windows has no
   node. Any UI page that draws a chart needs history, so this gates the UI work
   on Windows independently of which stack wins.
5. **How much of `SystemSnapshot` should `HealthView` carry?** A prior gap pass
   found the Status screen is substantially a *surfacing* problem: `seq_position`,
   the `messages` counter, cache volume, and coverage windows already exist in
   `SystemSnapshot` but are dropped by the `HealthView` reduction. Worth
   re-verifying against current code before scoping it as new instrumentation.

## Non-goals

Restating, because the composition layer makes them tempting:

- **No cross-symbol or global ordering.** Permanent.
- **No cross-provider reconciliation or canonicalization** — no merging two
  venues' AAPL into one canonical instrument.
- **No semantic enrichment.** Datamancer does not join a trade to a quote to
  infer side, and does not compute indicators. That is the signal engine's half of the
boundary.
- **No wall-clock-paced replay.** Replay drains as fast as the consumer reads.
- **Datamancer never faces the public.** Same-host, single-operator by design.

## References

- `crates/datamancerd/src/config_class.rs` — the Hot/Cold classification table
- `crates/datamancerd/README.md` — control protocol, config schema, error codes
- `crates/datamancer/README.md` — the library design doc
- [IBKR provider design](2026-09-23-ibkr-provider-design.md) — the second provider on this seam
- [App-facing daemon design](2026-07-05-app-facing-daemon-design.md) — the toggle/hot-reload machinery this builds on
- The UI prototype export — kept outside the repository (gitignored); first-draft visual reference only
