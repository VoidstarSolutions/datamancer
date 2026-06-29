//! The server-rendered operator page (`maud`).
//!
//! Read-only and **button-less**: the page renders the shell and a small inline
//! bootstrap that subscribes to `GET /api/stream` (SSE) and repaints per-symbol
//! panels from each live-state sample, plus a client-side circular buffer
//! driving a tiny latency sparkline per authoritative unit. No external CDN, no
//! JS build step; operator interactivity (the Phase-5 control surface) is a
//! later additive layer (`hx-post`), not present here.
//!
//! **Determinism in presentation:** every ordered quantity is shown
//! per-`(instrument, kind)`. There is no global cross-symbol event count,
//! stream position, or merged ordering; `seq` is labelled per-symbol and
//! `latency_ns` is marked observability-only.

use maud::{DOCTYPE, Markup, html};

/// `GET /` — the operator shell.
pub(crate) async fn index() -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { "datamancerd — introspection" }
                style { (PAGE_CSS) }
            }
            body {
                header {
                    h1 { "datamancerd" }
                    p.sub { "read-only introspection · same-host · per-symbol" }
                    p.status { "stream: " span #conn { "connecting…" } }
                }
                main {
                    section {
                        h2 { "Providers" }
                        div #providers.cards { "—" }
                    }
                    section {
                        h2 { "Authoritative sessions " span.note { "(per (instrument, kind); seq is per-symbol)" } }
                        div #authoritative { "—" }
                    }
                    section {
                        h2 { "Client sessions" }
                        div #clients { "—" }
                    }
                    section {
                        h2 { "Cache catalog" }
                        div #cache { "—" }
                    }
                }
                script { (maud::PreEscaped(PAGE_JS)) }
            }
        }
    }
}

const PAGE_CSS: &str = r"
:root { color-scheme: light dark; font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
body { margin: 0; padding: 1rem 1.25rem; line-height: 1.4; }
header h1 { margin: 0; font-size: 1.4rem; }
.sub { margin: .15rem 0; opacity: .7; }
.status { margin: .15rem 0 1rem; }
#conn { font-weight: 600; }
section { margin-bottom: 1.5rem; }
h2 { font-size: 1.05rem; border-bottom: 1px solid currentColor; padding-bottom: .2rem; }
.note { font-weight: 400; font-size: .8rem; opacity: .6; }
table { border-collapse: collapse; width: 100%; font-size: .85rem; }
th, td { text-align: left; padding: .25rem .5rem; border-bottom: 1px solid rgba(127,127,127,.3); }
th { opacity: .7; font-weight: 600; }
canvas { vertical-align: middle; }
";

// The live bootstrap. EventSource over /api/stream; each message is a full
// SystemSnapshot. Panels are repainted per-symbol; a per-unit circular buffer
// (capped) feeds a small latency sparkline (client-side history only — no
// server-side history per the snapshot model).
const PAGE_JS: &str = r"
const conn = document.getElementById('conn');
const CAP = 300; // ~5 min @ 1/s
const hist = new Map(); // key -> [latency_ns,...]
function key(s){ return s.instrument.provider + ':' + s.instrument.symbol + ':' + JSON.stringify(s.kind); }
function esc(v){ return String(v).replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])); }
function spark(arr){
  const c = document.createElement('canvas'); c.width=120; c.height=24;
  const g = c.getContext('2d'); if(!arr.length) return c;
  const max = Math.max(...arr, 1), min = Math.min(...arr, 0);
  g.beginPath();
  arr.forEach((v,i)=>{ const x=i/(CAP-1)*c.width; const y=c.height-(v-min)/(max-min||1)*c.height; i?g.lineTo(x,y):g.moveTo(x,y); });
  g.strokeStyle='currentColor'; g.stroke(); return c;
}
function table(headers, rows){
  const t = document.createElement('table');
  const thead = t.createTHead().insertRow();
  headers.forEach(h=>{ const th=document.createElement('th'); th.textContent=h; thead.appendChild(th); });
  const tb = t.createTBody();
  rows.forEach(r=>{ const tr=tb.insertRow(); r.forEach(cell=>{ const td=tr.insertCell(); if(cell instanceof Node) td.appendChild(cell); else td.innerHTML=esc(cell); }); });
  return t;
}
function paint(snap){
  const prov = document.getElementById('providers'); prov.replaceChildren(
    table(['provider','connection','history_fetches','live_starts','messages','gaps','last_error'],
      (snap.providers||[]).map(p=>[p.provider, p.connection_state, p.history_fetches, p.live_starts, p.messages, p.gaps_emitted, p.last_error??''])));
  const auth = document.getElementById('authoritative');
  const arows = (snap.authoritative_sessions||[]).map(s=>{
    const k = key(s); const h = hist.get(k) || []; if(s.latency_ns!=null){ h.push(s.latency_ns); if(h.length>CAP) h.shift(); hist.set(k,h); }
    return [s.instrument.symbol, JSON.stringify(s.kind), s.subscriber_refcount, (s.seq_position?.[0] ?? s.seq_position ?? '—'), (s.latency_ns ?? '—'), s.gap_count, spark(h)];
  });
  auth.replaceChildren(table(['symbol','kind','refcount','seq (per-symbol)','latency_ns (obs)','gaps','latency'], arows));
  const cl = document.getElementById('clients');
  cl.replaceChildren(table(['id','subscriptions','buffer occ/cap','dropped'],
    (snap.client_sessions||[]).map(c=>[c.id?.[0]??c.id, (c.subscriptions||[]).map(x=>x.instrument.symbol).join(', '), (c.resume_buffer.occupancy+'/'+c.resume_buffer.capacity), c.resume_buffer.dropped_events])));
  const cache = document.getElementById('cache');
  cache.replaceChildren(table(['provider','symbol','kind','adjustment','events','est_bytes','gaps'],
    ((snap.cache&&snap.cache.entries)||[]).map(e=>[e.provider, e.symbol, JSON.stringify(e.kind), JSON.stringify(e.adjustment), e.event_count, (e.est_bytes??'—'), (e.gaps||[]).length])));
}
const es = new EventSource('/api/stream');
es.onopen = () => { conn.textContent = 'live'; };
es.onerror = () => { conn.textContent = 'reconnecting…'; };
es.onmessage = (ev) => { try { paint(JSON.parse(ev.data)); } catch (e) {} };
// The live-state stream already carries the (slow-cadence) cache catalog; the
// dedicated /api/cache endpoint exists for machine consumers.
";
