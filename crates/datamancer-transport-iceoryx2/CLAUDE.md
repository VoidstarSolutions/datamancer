# datamancer-transport-iceoryx2

Same-host, zero-copy iceoryx2 transport for datamancer. Two planes:

- **Data plane** — one pub-sub service per client carrying a flat `#[repr(C)]`
  `DataPayload` that holds a compact `SymbolId` instead of the heap-backed
  `Instrument`. A second per-client announcement service publishes
  `SymbolAnnouncement`s (`SymbolId -> Instrument`).
- **Diagnostics plane** — a single byte-slice service publishing the serialized
  Phase-3 `SystemSnapshot` (not the zero-copy hot path).

## Invariants / stance

- **`#![forbid(unsafe_code)]`.** The EXT-1 gate holds: every wire payload uses
  the `#[derive(ZeroCopySend)]` derive (a *safe* generated impl) plus
  fixed-size `iceoryx2-bb-container` types (`StaticString`, `StaticVec`) only —
  **no** hand-written `unsafe impl`. The two core crates keep their forbid
  untouched, and so does this one. If a future payload ever needs a hand-written
  `unsafe impl ZeroCopySend`, relax *only this crate* to `#![deny(unsafe_code)]`
  + one scoped `#[allow(unsafe_code)]` with a `// SAFETY:` proof — never the
  core crates.
- **Pinned iceoryx2 version: `0.9.2`.** All builder/port method names are
  verify-against-this-version. Note 0.9.x renamed `FixedSizeByteString` →
  `StaticString` and uses `BackpressureStrategy` (no `UnableToDeliverStrategy`).
  Ports are created on `ipc_threadsafe::Service` (the default `ipc::Service` is
  single-threaded and its ports are not `Send`/`Sync`, which `EventSink`
  requires).
- **`ZeroCopySend` requires `#[repr(C)]`** (not `repr(transparent)`); the derive
  panics at compile time otherwise.
- **`SymbolId` is sink-local, not a global identity.** It is a per-service
  compaction handle; two clients may map the same id to different instruments.
  `seq` agreement across clients is by-construction (carried verbatim from the
  source-stamped event), not via the id.
- **Wire format may version.** `DataPayload`/`SymbolAnnouncement` are `pub` so
  external consumers can decode, but the layout is transport-internal and may
  change. The subscriber helper (`DataSubscriber`/`HoldBuffer`) is the supported
  decode path; Phase 5's fan-out reuses it.
- **Timestamp triple preserved.** The POD payload carries `source_ts`, `rx_ts`,
  and `seq` end-to-end; `rx_ts` stays observability-only and is **never**
  synthesized by the subscriber. `seq` carries `Seq::SYNTHETIC` verbatim.
- **Control routing.** Connection-scoped controls
  (`ProviderConnected`/`Disconnected`/`ProviderError`) are **suppressed** on the
  data plane (`to_pod` returns `None`) — remote consumers read connectivity from
  the diagnostics `ProviderSnapshot`. Per-symbol `Gap`/`SubscriptionChanged`
  carry their real `SymbolId`; `SessionClosing` routes to `SymbolId::CONNECTION`.
- **Two services, no mutual order.** Data and announcement services have no
  delivery-order guarantee; a data sample can outrun its `SymbolId`
  announcement. `HoldBuffer` holds unresolved samples and replays them on
  resolution — never drop/error.

## Tests

Unit tests (normal CI) protect the wire format: POD round-trips per variant,
symbol-table round-trip, capacity rejection, control routing, diagnostics codec
+ cap. iceoryx2 runtime integration tests live in `tests/iceoryx2_runtime.rs`
and are `#[ignore]`d (run with `--ignored`); they need a live iceoryx2 runtime.
