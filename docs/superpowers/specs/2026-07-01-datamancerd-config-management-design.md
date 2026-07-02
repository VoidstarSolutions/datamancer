# datamancerd config management — default location + UI editing

**Date:** 2026-07-01
**Status:** Approved design, pre-implementation

## Goal

Move `datamancerd` from a required `--config <path>` argument to a platform-native
default config location, scaffold a working default config on first run, and let
the existing web UI edit the config file through a structured settings form.
Changes take effect on restart only — no hot-reload.

## Non-goals

- Hot-reload / live reconfiguration of a running daemon.
- Raw-TOML editing in the UI (structured form only; comments in a hand-edited
  file are lost on the first UI save — accepted trade-off).
- Storing provider credentials in the config file. Credentials remain
  environment variables, unchanged.
- System-wide (`/etc`) config or multi-file layering.

## 1. Config resolution & first run

### Path resolution

- New dependency: `directories` (in `datamancerd` only).
- Default path: `ProjectDirs::from("", "Voidstar", "datamancerd")` →
  `config_dir().join("config.toml")`.
  - macOS: `~/Library/Application Support/datamancerd/config.toml`
  - Linux: `~/.config/datamancerd/config.toml` (respects `$XDG_CONFIG_HOME`)
- `--config <path>` becomes **optional**:
  - Present → that path is used verbatim. A missing file at an explicit path is
    an **error** (no scaffolding). This keeps the daemon e2e tests and dev
    workflows working unchanged.
  - Absent → the default path is used, with first-run scaffolding below.

### First-run scaffolding (default path only)

If no file exists at the default path:

1. Create the config directory (`create_dir_all`).
2. Write a commented default config (atomic write: temp file in the same
   directory + rename).
3. Log the path at `info` level and continue running with it.

### Default config content

- `[provider.alpaca]` with `account_type = "paper"` (validation requires at
  least one provider).
- `[web_ui] enabled = true` with existing bind/port defaults
  (`127.0.0.1:8080`) — the UI is the editing surface, so it must be reachable
  out of the box.
- `[server] admin_socket` pointing at a user-writable location (the config
  directory, e.g. `<config_dir>/admin.sock`) instead of the current
  `/run/datamancerd/admin.sock` default, which requires root and does not
  exist on macOS. The scaffolded file sets this explicitly; the compiled-in
  default in `config.rs` is unchanged.
- No `[[startup_session]]` entries, no `[cache]`/`[tap_log]` — first run
  connects to nothing and persists nothing.
- Generous comments so the file remains a useful hand-editing reference until
  the first UI save.

## 2. Config write path (shared plumbing)

- `Config` and all section structs gain `#[derive(Serialize)]` alongside the
  existing `Deserialize`. One schema drives file parsing, JSON for the UI, and
  TOML for writes — the form never re-implements validation.
- New `Config::save(&self, path)` (or free function): validate → serialize to
  TOML (`toml::to_string_pretty`) → atomic write (temp + rename, same
  directory).
- The daemon records at boot the exact bytes (or a hash) of the config it
  loaded. `restart_required` is true whenever the on-disk file no longer
  matches the loaded config.

## 3. Web UI editing

### Routes (existing axum server, `web-ui` feature)

- `GET /config` — server-rendered settings page (maud, consistent with the
  existing operator page): a structured form with a fieldset per section —
  provider(s), cache, tap_log, session, server, diagnostics, iceoryx2, web_ui,
  and a repeatable startup-sessions list. Submits via `fetch` as JSON.
- `GET /api/config` — `{ "config": <Config as JSON>, "restart_required": bool,
  "path": "<config file path>" }`. Reads and parses the on-disk file (not the
  in-memory boot config) so external edits show up.
- `PUT /api/config` — body is the full `Config` as JSON. Deserialize →
  `Config::validate()` → serialize → atomic write. Success returns the same
  shape as `GET /api/config` (with `restart_required` now true, unless the
  write restored the boot config). Validation/parse failures return the
  existing stable error codes (`config` for validation failures,
  `bad_request` for malformed bodies) with a human-readable message; nothing
  is written on failure.

### Restart-required banner

The operator page and the settings page both show a persistent banner when
`restart_required` is true: "Configuration changed on disk — restart
datamancerd to apply." The flag also rides the existing SSE live-state stream
so the banner appears without a page reload.

### Apply model

Edit-on-disk, apply-on-restart. The daemon's runtime configuration is
immutable after boot. No per-field hot-apply.

## 4. Security posture change

The web UI's documented contract changes from "read-only, GET-only" to
"read-only **plus one mutating route**":

- `PUT /api/config` is the only non-GET route. Loopback-only bind enforcement
  is unchanged.
- CSRF defense for the localhost port:
  - Require `Content-Type: application/json` (cross-origin JSON forces a CORS
    preflight, which fails because the server emits no CORS headers; simple
    cross-site form posts can't send this content type).
  - Reject requests whose `Origin` (when present) or `Host` does not match the
    bound address.
- Existing CSP / `X-Content-Type-Options` headers unchanged.
- The config file never contains credentials, so the write path cannot be used
  to exfiltrate or plant secrets beyond what the config already controls.
- `crates/datamancerd/README.md` security section updated to document the new
  contract (the README is the operator contract of record).

**Rejected alternative:** routing writes through the UDS control socket
(`op: "write-config"`) to keep HTTP GET-only. The web server is in-process and
can validate/write directly; a second hop adds surface without a security
gain. The UDS socket remains the programmatic control plane, unchanged.

## 5. Testing

- **Path resolution (unit):** default-path construction with injected
  overrides (no real home directories in tests), explicit `--config` beats
  default, missing explicit path errors, missing default path scaffolds.
- **Scaffolding (unit):** generated default parses and passes
  `Config::validate()`; atomic write leaves no temp droppings; second boot
  does not overwrite an existing file.
- **Round-trip (unit):** `Config` → TOML → `Config` equality across all
  sections, including `[[startup_session]]` lists.
- **Web handlers:** `GET /api/config` shape; `PUT` happy path writes and flips
  `restart_required`; `PUT` with invalid config writes nothing and returns
  code `config`; missing/wrong `Content-Type` and mismatched `Origin`/`Host`
  are rejected; `PUT` restoring the boot config clears `restart_required`.
- **E2e:** existing `#[ignore]`d daemon e2e tests pass unchanged (they use
  explicit `--config`).

## Open items deliberately deferred

- Hot-apply of "safe" fields (cadences) — possible later layer on the same
  write path.
- A raw-TOML "advanced" editor tab.
- Config versioning/migrations (schema is young; serde defaults cover
  additive fields).
