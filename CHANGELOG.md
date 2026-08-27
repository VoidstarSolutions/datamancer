# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.9.0](https://github.com/VoidstarSolutions/datamancer/compare/v0.8.0...v0.9.0) - 2026-08-27

### Added

- *(winsec)* Windows token/handle identity & integrity readers
- *(winsec)* new crate with pure integrity-level classifier
- *(windows)* boot datamancerd WS-only (skip the iceoryx2 node)
- *(windows)* hybrid AppHandle — WS-loopback data + health plane
- *(windows)* [**breaking**] health-push over WS (watch-health)
- *(windows)* standalone PipeControlClient for the hybrid admin plane
- *(windows)* add integrity_rejected control code
- *(windows)* named-pipe control transport with owner-DACL same-user auth
- *(windows)* daemon rejects non-Medium control clients in-band
- *(windows)* daemon refuses to start at non-Medium integrity
- *(windows)* [server].allow_any_integrity override flag

### Fixed

- *(winsec)* capture the Win32 error before CloseHandle in token/integrity reads
- *(winsec)* satisfy pinned clippy on Windows — # Errors docs + ptr-deref allow
- *(winsec)* guard 0-count integrity SID; gate winsec dep on iceoryx2
- *(windows)* align winsec dep requirement to workspace 0.8.0
- *(windows)* align winsec dependency version to workspace 0.7.0 after rebase
- *(windows)* address PR #37 review — owner SID stamp, resilient accept loop
- *(datamancerd)* reject zero ws channel_depth/max_connections; checked timestamp math
- *(windows)* boot the CI smoke daemon at any integrity
- *(windows)* assert same-process client integrity equals own, not Medium
- *(windows)* read allow_any_integrity before build_runtime consumes config
- *(windows)* clear error for a non-pipe control-socket name (review #1)
- *(windows)* daemon integrity message covers the lowered case too

### Other

- *(windows)* document Medium-integrity enforcement; winsec crate; bump 0.6.0
- *(windows)* review-readiness — reject zero diag interval; doc hygiene
- *(windows)* robustify integrity assertions + add winsec/win_pipe CI coverage
- *(windows)* client win_pipe on datamancer-winsec; restore forbid(unsafe_code)
- *(windows)* record the Phase 3 EXT-1 unsafe exception in baseline docs
- Merge branch 'main' into docs/ws-consumer-example
- *(datamancerd)* cover the unprivileged control-gate deny arm; run the pipe round-trip test in CI
- *(windows)* config-service admin-plane e2e over the named pipe (Phase 5/B3)
- *(windows)* daemon win_control sources identity from datamancer-winsec

## [0.8.0](https://github.com/VoidstarSolutions/datamancer/compare/v0.7.0...v0.8.0) - 2026-07-19

### Added

- [**breaking**] split Provider::supports into live and history surfaces

### Fixed

- *(alpaca)* wire historical quotes through fetch_history
- *(datamancerd)* update client_transport_e2e for the kinds split
- *(examples)* declare only the surfaces each example provider implements

## [0.7.0](https://github.com/VoidstarSolutions/datamancer/compare/v0.6.0...v0.7.0) - 2026-07-19

### Added

- *(core)* add InstrumentCapabilities, OrderType, TimeInForce
- *(core)* InstrumentEntry + optional capabilities on InstrumentInfo
- *(core)* list_instruments returns InstrumentEntry; add Provider::capabilities
- *(datamancer)* fold inline capabilities into catalog; add instrument_capabilities
- *(alpaca)* populate InstrumentCapabilities from /v2/assets
- *(client)* capabilities op wire types (uds + ws)
- capabilities control op (client trait, transports, daemon dispatch)

### Fixed

- surface failing symbol on capabilities op; docs + ws reply test (review follow-ups)
- provider stamps authoritative asset class on capabilities; correct crypto policy
- gate fractional caps on fractionable; eligibility-filter capability lookups

## [0.6.0](https://github.com/VoidstarSolutions/datamancer/compare/v0.5.0...v0.6.0) - 2026-07-18

### Added

- *(windows)* Phase 1 cleanups + open-sourcing spec consolidation
- *(windows)* client app compiles on Windows (named-pipe control + detached spawn)
- *(windows)* daemon compiles on Windows (real lock+signals, fail-closed control stub)
- *(core)* add provided Provider::latest() for live seed
- seed pure-live subscriptions with provider latest value
- *(alpaca)* implement Provider::latest via stock snapshot
- *(alpaca-crypto)* implement Provider::latest via crypto snapshot

### Fixed

- pin documented Windows control-socket path in test
- *(windows)* address review — defer admin-socket fallback, fence lang
- *(windows)* audit cleanup -- fail-closed pipe, byte-exact logs, native CI guard
- *(windows)* address CodeRabbit review on #35
- *(alpaca)* seed stock latest() from the configured stream feed

### Other

- add native Windows support design spec
- record iceoryx2 Windows spike result and transport decision
- *(windows)* complete the credential-backend name contract
- bump oxidized_alpaca 0.0.9 -> 0.0.10 for PDT changes
- design for live latest-value seed on pure-live subscriptions
- implementation plan for live latest-value seed
- soften seed-vs-connect-control ordering claim (final review)
- address live-latest-seed review findings

## [0.5.0] - 2026-07-07

Baseline release: the workspace version unification that introduced release
automation. Everything before it predates this changelog.
