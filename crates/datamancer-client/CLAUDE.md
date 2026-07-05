# datamancer-client

Consumer-side surface for datamancerd: the shared control vocabulary
(`spec`, `codes`, `protocol::{uds,ws}`) and, behind features `ws` /
`iceoryx2`, the two `Client` trait implementations.

## Invariants / stance

- **`#![forbid(unsafe_code)]`**, `[lints] workspace = true`.
- **Depends on `datamancer-core` + the transport crates only — never the
  `datamancer` orchestrator.** The orchestrator re-exports this crate
  (features `client-ws`/`client-iceoryx2`), not the reverse.
- **The vocabulary is the operator contract.** JSON shapes and stable code
  strings moved here verbatim from `datamancerd`; changing either is a
  breaking change guarded by the moved regression tests. `datamancerd`
  re-imports them — one definition.
- **The trait is generic (assoc types), not dyn.** Transport is a
  compile-time choice. Runtime selection is a consumer-side enum, deferred.
- **`connect` returns a split `(handle, events)` pair** so control calls and
  stream draining never contend.
- **Two-layer errors.** Daemon rejections → `ClientError::Control` with a
  stable code (identical across transports); only transport failures are the
  per-impl `Error` type.
- **Loss contract is documented, not normalized.** iceoryx2: in-band
  `Control::Gap`. WS: stream end. Graceful close is marked by a terminal
  `SessionClosing`. The client never synthesizes events (`rx_ts` included).
- **Pinned versions in lockstep:** tokio-tungstenite 0.29.0 and iceoryx2
  0.9.2 must match the transport crates and `datamancerd`.

## `app` feature (find-or-spawn facade)

- **`app` implies `iceoryx2` and gains no WS lifecycle powers.** `AppHandle`
  is same-host only, built on `Iceoryx2Client`; it is not a third transport.
- **The facade adds no protocol semantics.** Every `AppHandle` method maps to
  an existing control-surface op (`ping`, `open-client`/`connect`,
  `subscribe`, `unsubscribe`, `snapshot`, `close`) — it composes, it does not
  extend, the vocabulary this crate already owns.
- **Platform seams are internal traits, not a public abstraction.**
  `ControlEndpoint` and `DaemonSpawner` (`app::lifecycle`) isolate the
  find-or-spawn state machine from the unix-specific `TokioEndpoint` /
  `ProcessSpawner` (`app::platform`). A Windows port is a new `platform`
  module (named pipe + `CreateProcess`) implementing the same seams — never a
  widened state machine in `lifecycle.rs`.
- **`EnsureError` variants and the `ping` reply shape are app-facing
  contract.** `NoSocketPath`, `SpawnFailed`, `ReadyTimeout` (with
  `ReadyDiagnosis`), `VersionSkew`, `Connect` are matched by consuming apps;
  treat additions/removals as breaking. The `ping` reply
  (`{"ok":true,"version":"…"}`) is the daemon's control protocol, documented
  in `datamancerd/README.md` — this crate only consumes it.
