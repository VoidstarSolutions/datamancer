//! The `/config` settings page: a server-rendered shell whose inline JS
//! fetches `GET /api/config`, renders a structured form (typed inputs per
//! section, repeatable startup-session rows), and submits the assembled
//! config back via `PUT /api/config`. Validation is entirely server-side —
//! the form never re-implements the `Config` schema rules; it renders what
//! the API returns and displays what the API rejects.
//!
//! Apply-on-restart: a successful save flips the restart banner; the daemon's
//! running config is unchanged until restart.

use maud::{DOCTYPE, Markup, html};

/// `GET /config` — the settings shell.
pub(crate) async fn page() -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { "datamancerd — settings" }
                style { (CSS) }
            }
            body {
                header {
                    h1 { a href="/" { "datamancerd" } " / settings" }
                    p.sub { "edits the config file; changes apply on restart" }
                    div #banner hidden { "Configuration changed on disk — restart datamancerd to apply." }
                    div #error hidden {}
                }
                main {
                    div #settings { "loading…" }
                    p {
                        button #save disabled { "Save config" }
                        span #path.note {}
                    }
                }
                script { (maud::PreEscaped(JS)) }
            }
        }
    }
}

const CSS: &str = r"
:root { color-scheme: light dark; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
body { margin: 0; padding: 1rem 1.25rem; line-height: 1.5; max-width: 60rem; }
header h1 { margin: 0; font-size: 1.4rem; } header h1 a { color: inherit; }
.sub { margin: .15rem 0 1rem; opacity: .7; }
.note { font-size: .8rem; opacity: .6; margin-left: 1rem; }
#banner { background: #b45309; color: #fff; padding: .4rem .6rem; border-radius: 4px; margin: .4rem 0; }
#error { background: #b91c1c; color: #fff; padding: .4rem .6rem; border-radius: 4px; margin: .4rem 0; white-space: pre-wrap; }
fieldset { border: 1px solid rgba(127,127,127,.4); border-radius: 4px; margin: 0 0 1rem; }
legend { font-weight: 600; padding: 0 .4rem; }
label { display: inline-block; margin: .2rem 1rem .2rem 0; }
input[type=text], input[type=number] { font: inherit; width: 14rem; }
input[type=number] { width: 8rem; }
select { font: inherit; }
button { font: inherit; padding: .3rem .8rem; }
.session-row { border-bottom: 1px dashed rgba(127,127,127,.4); padding: .4rem 0; }
";

const JS: &str = r#"
const $ = (id) => document.getElementById(id);
let current = null;

const esc = (v) => String(v ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

const SEL = (name, options, value) =>
  `<select name="${esc(name)}">` + options.map(o => `<option value="${esc(o)}"${o===value?' selected':''}>${esc(o)}</option>`).join('') + `</select>`;
const TXT = (name, value, ph) => `<input type="text" name="${esc(name)}" value="${esc(value)}" placeholder="${esc(ph)}">`;
const NUM = (name, value) => `<input type="number" name="${esc(name)}" value="${esc(value)}">`;
const CHK = (name, value) => `<input type="checkbox" name="${name}"${value?' checked':''}>`;
const L = (text, control) => `<label>${text} ${control}</label>`;

const KINDS = ['trade','quote','bar1s','bar1m','bar5m','bar15m','bar1h','bar1d'];
const PERSIST = ['none','cached','cached_with_tap','read_only','refresh','tap_only'];

function sessionRow(s, i) {
  s = s || {provider:'alpaca-crypto', asset_class:'crypto', symbol:'', kind:'trade',
            scope:'live', persistence:'none', always_on:false};
  return `<div class="session-row" data-i="${esc(i)}">`
    + L('provider', TXT(`ss-provider-${i}`, s.provider))
    + L('asset_class', SEL(`ss-asset-${i}`, ['equity','crypto'], s.asset_class))
    + L('symbol', TXT(`ss-symbol-${i}`, s.symbol))
    + L('kind', SEL(`ss-kind-${i}`, KINDS, s.kind))
    + L('scope', SEL(`ss-scope-${i}`, ['live','live_backfill'], s.scope))
    + L('backfill_from', TXT(`ss-backfill-${i}`, s.backfill_from, 'RFC3339, for live_backfill'))
    + L('persistence', SEL(`ss-persist-${i}`, PERSIST, s.persistence))
    + L('always_on', CHK(`ss-always-${i}`, s.always_on))
    + `<button type="button" data-remove="${i}">remove</button></div>`;
}

function storageFields(key, cfg) {
  const on = cfg != null;
  return `<fieldset><legend>${key}</legend>`
    + L('enabled', CHK(`${key}-on`, on))
    + L('backend', SEL(`${key}-backend`, ['embedded','memory'], on ? cfg.backend : 'embedded'))
    + L('path', TXT(`${key}-path`, on ? cfg.path : ''))
    + `</fieldset>`;
}

function render(cfg) {
  const p = cfg.provider || {};
  const w = cfg.web_ui || {enabled:false, bind:'127.0.0.1', port:8080,
                           live_state_cadence_ms:1000, cache_catalog_cadence_ms:30000};
  $('settings').innerHTML =
    `<fieldset><legend>provider.alpaca (equities)</legend>`
      + L('enabled', CHK('alpaca-on', !!p.alpaca))
      + L('account_type', SEL('alpaca-account', ['paper','live'], p.alpaca?.account_type ?? 'paper'))
      + L('history_feed', SEL('alpaca-history', ['auto','iex','sip'], p.alpaca?.history_feed ?? 'auto'))
    + `</fieldset>`
    + `<fieldset><legend>provider.alpaca_crypto</legend>`
      + L('enabled', CHK('crypto-on', !!p.alpaca_crypto))
      + L('account_type', SEL('crypto-account', ['paper','live'], p.alpaca_crypto?.account_type ?? 'paper'))
      + L('venue', SEL('crypto-venue', ['us','us_kraken','eu_kraken'], p.alpaca_crypto?.venue ?? 'us'))
    + `</fieldset>`
    + storageFields('cache', cfg.cache)
    + storageFields('tap_log', cfg.tap_log)
    + `<fieldset><legend>session</legend>`
      + L('resume_buffer_events', NUM('sess-buffer', cfg.session.resume_buffer_events))
      + L('adjustment', SEL('sess-adjust', ['raw','split','dividend','spin_off','all'], cfg.session.adjustment))
      + L('query_persistence', SEL('sess-query', ['none','cached','cached_with_tap','read_only','refresh','tap_only'], cfg.session.query_persistence))
      + L('max_backfill_days', NUM('sess-backfill', cfg.session.max_backfill_days))
    + `</fieldset>`
    + `<fieldset><legend>server</legend>`
      + L('admin_socket', TXT('srv-socket', cfg.server.admin_socket))
      + L('service_prefix', TXT('srv-prefix', cfg.server.service_prefix))
      + L('shutdown_timeout_secs', NUM('srv-timeout', cfg.server.shutdown_timeout_secs))
      + L('max_concurrent_backfills', NUM('srv-backfills', cfg.server.max_concurrent_backfills))
    + `</fieldset>`
    + `<fieldset><legend>diagnostics</legend>`
      + L('publish_interval_ms', NUM('diag-live', cfg.diagnostics.publish_interval_ms))
      + L('cache_catalog_interval_ms', NUM('diag-catalog', cfg.diagnostics.cache_catalog_interval_ms))
    + `</fieldset>`
    + `<fieldset><legend>iceoryx2</legend>`
      + L('max_clients', NUM('iox-clients', cfg.iceoryx2.max_clients))
    + `</fieldset>`
    + `<fieldset><legend>web_ui</legend>`
      + L('enabled', CHK('web-on', w.enabled))
      + L('bind', TXT('web-bind', w.bind))
      + L('port', NUM('web-port', w.port))
      + L('assets_dir', TXT('web-assets', w.assets_dir, 'optional'))
      + L('live_state_cadence_ms', NUM('web-live', w.live_state_cadence_ms))
      + L('cache_catalog_cadence_ms', NUM('web-catalog', w.cache_catalog_cadence_ms))
    + `</fieldset>`
    + `<fieldset><legend>startup sessions</legend><div id="sessions">`
      + (cfg.startup_session || []).map(sessionRow).join('')
    + `</div><button type="button" id="add-session">add session</button></fieldset>`;
  $('save').disabled = false;
}

const val = (n) => document.getElementsByName(n)[0].value;
const num = (n) => Number(val(n));
const chk = (n) => document.getElementsByName(n)[0].checked;
const opt = (v) => (v === '' ? undefined : v);

// Starts from the loaded config, so a section or key the form has no field
// for is saved as loaded.
function collect() {
  const cfg = structuredClone(current);
  cfg.provider = cfg.provider || {};
  delete cfg.provider.alpaca;
  if (chk('alpaca-on')) {
    cfg.provider.alpaca = { ...current.provider?.alpaca, account_type: val('alpaca-account') };
    // 'auto' = omit the key: the daemon then follows the stream feed.
    const historyFeed = val('alpaca-history');
    if (historyFeed === 'auto') delete cfg.provider.alpaca.history_feed;
    else cfg.provider.alpaca.history_feed = historyFeed;
  }
  delete cfg.provider.alpaca_crypto;
  if (chk('crypto-on')) cfg.provider.alpaca_crypto = { ...current.provider?.alpaca_crypto,
    account_type: val('crypto-account'), venue: val('crypto-venue') };
  for (const key of ['cache','tap_log']) {
    delete cfg[key];
    if (chk(`${key}-on`)) cfg[key] = { ...current[key], backend: val(`${key}-backend`), path: opt(val(`${key}-path`)) };
  }
  cfg.session = { ...cfg.session, resume_buffer_events: num('sess-buffer'), adjustment: val('sess-adjust'),
    query_persistence: val('sess-query'), max_backfill_days: num('sess-backfill') };
  cfg.server = { ...cfg.server, admin_socket: val('srv-socket'), service_prefix: val('srv-prefix'), shutdown_timeout_secs: num('srv-timeout'),
    max_concurrent_backfills: num('srv-backfills') };
  cfg.diagnostics = { ...cfg.diagnostics, publish_interval_ms: num('diag-live'), cache_catalog_interval_ms: num('diag-catalog') };
  cfg.iceoryx2 = { ...cfg.iceoryx2, max_clients: num('iox-clients') };
  cfg.web_ui = { ...cfg.web_ui, enabled: chk('web-on'), bind: val('web-bind'), port: num('web-port'),
                 assets_dir: opt(val('web-assets')),
                 live_state_cadence_ms: num('web-live'), cache_catalog_cadence_ms: num('web-catalog') };
  cfg.startup_session = [...document.querySelectorAll('.session-row')].map(row => {
    const i = row.dataset.i;
    return { provider: val(`ss-provider-${i}`), asset_class: val(`ss-asset-${i}`),
             symbol: val(`ss-symbol-${i}`), kind: val(`ss-kind-${i}`),
             scope: val(`ss-scope-${i}`), backfill_from: opt(val(`ss-backfill-${i}`)),
             persistence: val(`ss-persist-${i}`), always_on: chk(`ss-always-${i}`) };
  });
  return cfg;
}

function show(data) {
  current = data.config;
  $('banner').hidden = !data.restart_required;
  $('path').textContent = data.path;
  render(current);
}

function fail(msg) { const e = $('error'); e.hidden = false; e.textContent = msg; }

let nextI = 0;
document.addEventListener('click', async (ev) => {
  if (ev.target.id === 'add-session') {
    const div = document.createElement('div');
    div.innerHTML = sessionRow(null, `n${nextI++}`);
    $('sessions').appendChild(div.firstChild);
  } else if (ev.target.dataset.remove !== undefined) {
    ev.target.closest('.session-row').remove();
  } else if (ev.target.id === 'save') {
    $('error').hidden = true;
    const resp = await fetch('/api/config', {
      method: 'PUT',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(collect()),
    });
    const data = await resp.json().catch(() => null);
    if (resp.ok && data) { show(data); }
    else { fail(data ? `${data.code}: ${data.message}` : `save failed (${resp.status})`); }
  }
});

fetch('/api/config').then(r => r.json()).then(show).catch(e => fail(String(e)));
"#;

#[cfg(test)]
mod tests {
    use super::JS;

    /// Run the page script under node with a stub DOM whose inputs are all
    /// empty and unchecked, load `config` as the page's current config, and
    /// return what the form's save would send. `None` when node is missing.
    fn collect_under_node(config: &serde_json::Value) -> Option<serde_json::Value> {
        const HARNESS: &str = r"
const vm = require('node:vm');
const fs = require('node:fs');
const [script, config] = [fs.readFileSync(process.argv[2], 'utf8'), fs.readFileSync(process.argv[3], 'utf8')];
const input = () => ({ value: '', checked: false });
const document = {
  getElementById: () => ({}),
  getElementsByName: () => [input()],
  querySelectorAll: () => [],
  addEventListener: () => {},
};
const context = vm.createContext({ document, fetch: () => new Promise(() => {}), structuredClone, console });
vm.runInContext(script, context);
process.stdout.write(vm.runInContext(`current = ${config}; JSON.stringify(collect())`, context));
";
        let dir = tempfile::tempdir().expect("tempdir");
        let harness = dir.path().join("harness.js");
        let script = dir.path().join("settings.js");
        let config_path = dir.path().join("config.json");
        std::fs::write(&harness, HARNESS).expect("write harness");
        std::fs::write(&script, JS).expect("write script");
        std::fs::write(&config_path, config.to_string()).expect("write config");
        let output = match std::process::Command::new("node")
            .arg(&harness)
            .arg(&script)
            .arg(&config_path)
            .output()
        {
            Ok(output) => output,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return None,
            Err(e) => panic!("run node: {e}"),
        };
        assert!(
            output.status.success(),
            "node failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Some(serde_json::from_slice(&output.stdout).expect("collect() output is JSON"))
    }

    /// Saving the form keeps a section it has no field for and still applies
    /// the fields it has: here the unchecked Alpaca box drops `alpaca`.
    #[test]
    fn saving_the_web_form_keeps_sections_it_has_no_field_for() {
        let config = crate::config::Config::parse(
            "[provider.alpaca]\naccount_type = \"paper\"\n\n\
             [log]\nlevel = \"debug\"\nformat = \"json\"\n",
        )
        .expect("parse");
        let loaded = serde_json::to_value(&config).expect("config as JSON");
        let Some(saved) = collect_under_node(&loaded) else {
            assert!(
                std::env::var_os("CI").is_none(),
                "node is required for this test in CI"
            );
            eprintln!("node not found; skipping");
            return;
        };
        assert_eq!(saved["log"], loaded["log"], "{saved}");
        assert!(saved["provider"].get("alpaca").is_none(), "{saved}");
    }
}
