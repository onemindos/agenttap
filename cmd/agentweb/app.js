'use strict';
// agentweb — "The Stream" engine.
// The page is a pure function of (event log + playhead + play-state): events stream
// in over SSE; a playhead walks event-time (real time x speed); render() projects the
// prefix of events at/under the playhead into the roster, conversation threads, and wire.
// DOM is updated incrementally (keyed) so CSS animations are never reset.

const $ = (s) => document.querySelector(s);
const el = (tag, cls, html) => { const e = document.createElement(tag); if (cls) e.className = cls; if (html != null) e.innerHTML = html; return e; };
const escapeHtml = (s) => String(s).replace(/[&<>]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;' }[c]));
const trunc = (s, n = 16) => { s = String(s || ''); return s.length > n ? s.slice(0, Math.ceil(n / 2)) + '…' + s.slice(-Math.floor(n / 2)) : s; };
// Routing identity shared by an instance's prompt and heartbeat subjects: tokens 3-5 of
// agents.{verb}.{agent}.{owner}.{name} (core-protocol §2). This is the only reliable link
// between a conversation (keyed by the prompt subject) and a roster node (keyed by the
// heartbeat subject) — the 3rd token is a subject abbreviation, not the canonical agent
// name, and the 5th (instance name) lives only in the subject (§8.1, §8.3).
const routeKey = (subject) => {
  const t = String(subject || '').split('.');
  return (t.length >= 5 && t[0] === 'agents') ? `${t[2]}/${t[3]}/${t.slice(4).join('.')}` : null;
};

function parseTs(ts) { // RFC3339Nano -> ms; trim sub-millisecond digits so Date.parse is reliable
  const m = String(ts).replace(/(\.\d{3})\d+(Z|[+\-]\d\d:\d\d)$/, '$1$2');
  const v = Date.parse(m);
  return isNaN(v) ? 0 : v;
}
function clockOf(ms) {
  const d = new Date(ms), p = (n, w = 2) => String(n).padStart(w, '0');
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${p(d.getMilliseconds(), 3)}`;
}
function durOf(ms) {
  ms = Math.max(0, ms); const s = ms / 1000, m = Math.floor(s / 60);
  return `${String(m).padStart(2, '0')}:${(s - m * 60).toFixed(1).padStart(4, '0')}`;
}
// Classify an agent-SDK reply frame carried in a chunk's `data`.
function classifyFrame(data) {
  let f = null; try { f = JSON.parse(data); } catch (e) { /* not framed */ }
  if (f && typeof f === 'object' && typeof f.type === 'string') {
    if (f.type === 'response') {
      // §6.3: data is a string, or an object { text, attachments }
      const text = typeof f.data === 'string' ? f.data : (f.data && typeof f.data.text === 'string' ? f.data.text : '');
      return { kind: 'prose', text, ft: 'response' };
    }
    if (f.type === 'query') return { kind: 'query', ft: 'query', q: (f.data && typeof f.data === 'object') ? f.data : {} }; // §7
    const c = typeof f.data === 'string' ? f.data : JSON.stringify(f.data);
    return { kind: 'control', ft: f.type, ctrl: c };
  }
  return { kind: 'prose', text: data, ft: 'raw' };
}
// Accent for an event: the type's color, mirrored across the focus card, the focused
// wire row, and the source agent node in the roster. cls -> focus-card theme class;
// rgb -> the matching --fc-rgb / --sel-rgb triplet.
function accentOf(o) {
  if (o.type === 'prompt') return { cls: 't-prompt', rgb: '60,231,255' };
  if (o.type === 'heartbeat' || o.type === 'online') return { cls: 't-hb', rgb: '255,87,203' };
  if (o.type === 'chunk') return classifyFrame(o.data).kind === 'query'
    ? { cls: 't-query', rgb: '255,216,77' } : { cls: 't-reply', rgb: '255,159,49' };
  if (o.type === 'query_reply') return { cls: 't-query', rgb: '255,216,77' };
  if (o.type === 'warn') return { cls: 't-warn', rgb: '255,72,96' };
  if (o.type === 'done' || o.type === 'sentinel') return { cls: 't-done', rgb: '159,182,201' };
  return { cls: 't-reply', rgb: '255,159,49' };
}
// The agent an event belongs to, as a routeKey (heartbeat: its own hb subject; prompt: the
// addressed prompt subject; reply/done/etc.: the flow's prompt subject). null when none.
function agentKeyOf(o) {
  if (o.type === 'heartbeat' || o.type === 'online') return routeKey(o.subject);
  if (o.type === 'prompt') return routeKey(o.subject);
  if (o.seq != null) { const f = S.flows.get(o.seq); if (f) return routeKey(f.subject); }
  return null;
}

const S = {
  events: [], mode: 'live', eof: false, started: false,
  playing: false, speed: 1, playhead: 0, t0: null, tEnd: null, follow: true,
  focus: null, lastFocus: -2, focusCard: null,
  roster: new Map(), threads: new Map(), wireCount: 0, wireEls: [], tickKey: '',
  flows: new Map(), convoOrder: '',
};

// --- ingest -----------------------------------------------------------------
const es = new EventSource('/events');
es.addEventListener('control', (e) => {
  let c = {}; try { c = JSON.parse(e.data); } catch (_) {}
  if (c.mode) { S.mode = c.mode; S.follow = (c.mode === 'live'); $('#src').textContent = c.mode; }
  if (c.eof) S.eof = true;
  updateStatePill();
});
es.onmessage = (e) => ingest(e.data);
es.onerror = () => { const s = $('#src'); if (s && !S.events.length) s.textContent = 'disconnected — retrying'; };

function ingest(line) {
  let o; try { o = JSON.parse(line); } catch (e) { return; }
  if (!o || typeof o !== 'object' || !o.type) return;
  o._ts = parseTs(o.ts); o._i = S.events.length;
  S.events.push(o);
  if (o.type === 'meta' && o.schema != null) $('#schema').textContent = o.schema;
  if (S.t0 === null) { S.t0 = o._ts; S.playhead = o._ts; }
  S.tEnd = o._ts;
  if (!S.started) { S.started = true; S.playing = true; updateStatePill(); } // auto-play on first data
}

// --- player loop ------------------------------------------------------------
let last = performance.now();
function frame(now) {
  const dt = now - last; last = now;
  if (S.playing && S.t0 !== null) {
    if (S.follow) {
      S.playhead = S.tEnd;
    } else {
      S.playhead += dt * S.speed;
      if (S.playhead >= S.tEnd) {
        S.playhead = S.tEnd;
        if (S.mode === 'live') S.follow = true;
        else if (S.eof) { S.playing = false; updateStatePill(); }
      }
    }
  }
  if (S.t0 !== null) render();
  requestAnimationFrame(frame);
}
requestAnimationFrame(frame);

function visibleCount() {
  const ph = S.playhead; let n = 0;
  while (n < S.events.length && S.events[n]._ts <= ph) n++;
  return n;
}

// --- render -----------------------------------------------------------------
function render() {
  const vis = visibleCount();
  buildFlows(vis);
  $('#clock').textContent = clockOf(S.playhead);
  renderWire(vis);
  renderConvos(vis);
  renderRoster(vis);
  renderTransport(vis);
}

function buildFlows(vis) {
  const flows = new Map();
  for (let i = 0; i < vis; i++) {
    const o = S.events[i];
    if (o.type === 'prompt') {
      flows.set(o.seq, { seq: o.seq, prompt: o, chunks: [], ctrls: [], queries: [], qreplies: {}, done: null, sentinel: null, last: o._ts,
        agent: o.agent, owner: o.owner, session: o.session, subject: o.subject, reply: o.reply });
    } else if (o.type === 'chunk') {
      const f = flows.get(o.seq); if (!f) continue;
      const c = classifyFrame(o.data);
      if (c.kind === 'prose') f.chunks.push({ o, c });
      else if (c.kind === 'query') f.queries.push({ o, c });
      else f.ctrls.push({ o, c });
      f.last = o._ts;
    } else if (o.type === 'query_reply') { const f = flows.get(o.seq); if (f) { f.qreplies[o.qid] = o; f.last = o._ts; } }
    else if (o.type === 'sentinel') { const f = flows.get(o.seq); if (f) { f.sentinel = o; f.last = o._ts; } }
    else if (o.type === 'done') { const f = flows.get(o.seq); if (f) { f.done = o; f.last = o._ts; } }
  }
  S.flows = flows;
}

const SVG_HEX = '<svg viewBox="0 0 40 40"><polygon class="hexfill" points="20,3 35,11.5 35,28.5 20,37 5,28.5 5,11.5"/></svg>';

function renderRoster(vis) {
  const inst = new Map();
  for (let i = 0; i < vis; i++) {
    const o = S.events[i];
    if ((o.type === 'heartbeat' || o.type === 'online') && o.instance_id)
      inst.set(o.instance_id, { agent: o.agent || '?', owner: o.owner || '?', session: o.session || '', subject: o.subject || '', last: o._ts, interval: o.interval_s || 0 });
  }
  // base highlight: agents with an IN-FLIGHT reply at the playhead (prompted, not yet
  // done/sentinel). Reconstructed from the visible prefix, so scrubbing replays it as if
  // live. Keyed by the prompt's routing identity (agent/owner/session).
  const inflightKeys = new Set([...S.flows.values()].filter((f) => !f.done && !f.sentinel).map((f) => routeKey(f.subject)).filter(Boolean));
  // selection override: a focused event lights its own agent, in the item's accent, on top
  // of the base. Heartbeats match their node exactly by instance_id; everything else falls
  // back to the routed identity (which can't always resolve — see agentKeyOf).
  let selKey = null, selInst = null, selRgb = '255,159,49';
  if (S.focus != null && S.events[S.focus]) {
    const fo = S.events[S.focus];
    selRgb = accentOf(fo).rgb;
    if (fo.type === 'heartbeat' || fo.type === 'online') selInst = fo.instance_id || null;
    else selKey = agentKeyOf(fo);
  }
  const body = $('#roster-body');
  if (inst.size && body.firstElementChild && body.firstElementChild.classList.contains('empty')) body.innerHTML = '';
  const seen = new Set();
  for (const [id, d] of inst) {
    seen.add(id);
    let n = S.roster.get(id);
    if (!n) {
      n = el('div', 'node');
      n.innerHTML = `<div class="av"><span class="heart">&#9829;</span>${SVG_HEX}</div>
        <div><div class="who"></div><div class="meta"><span class="sess"></span><span class="iid"></span><span class="ivl"></span></div><div class="beatbar"></div></div>`;
      S.roster.set(id, n); body.appendChild(n);
    }
    const k = routeKey(d.subject);
    const isSel = selInst ? id === selInst : (!!selKey && !!k && k === selKey); // inspected agent (accent)
    const inflight = !!k && inflightKeys.has(k);                               // currently streaming (orange)
    const beatMs = (d.interval > 0 ? d.interval : 30) * 1000; // §8.2: offline after 3× interval_s
    const stale = (S.playhead - d.last) > 3 * beatMs;
    n.classList.toggle('active', inflight);
    n.classList.toggle('selected', isSel);
    n.classList.toggle('stale', stale && !inflight && !isSel);
    n.style.setProperty('--sel-rgb', isSel ? selRgb : '255,159,49');
    n.querySelector('.who').innerHTML = `${escapeHtml(d.agent)}/${escapeHtml(d.owner)}` + (inflight ? ' <span class="tag-live">replying</span>' : '');
    n.querySelector('.sess').textContent = d.session || '—';
    n.querySelector('.iid').textContent = trunc(id, 14);
    n.querySelector('.ivl').textContent = d.interval ? d.interval + 's' : '';
  }
  for (const [id, n] of [...S.roster]) if (!seen.has(id)) { n.remove(); S.roster.delete(id); }
  $('#roster-count').textContent = inst.size ? `${inst.size} instance${inst.size > 1 ? 's' : ''}` : '';
}

function renderConvos(vis) {
  const body = $('#convo-body');
  const all = [...S.flows.values()];
  if (all.length && body.firstElementChild && body.firstElementChild.classList.contains('empty')) body.innerHTML = '';
  // up to 2 threads: prefer still-open flows, then most-recent; keep stable display order by prompt time
  const open = all.filter((f) => !f.done).sort((a, b) => b.last - a.last);
  const closed = all.filter((f) => f.done).sort((a, b) => b.last - a.last);
  const show = [...open, ...closed].slice(0, 2).sort((a, b) => a.prompt._ts - b.prompt._ts);
  const seen = new Set();
  for (const f of show) {
    seen.add(f.seq);
    let t = S.threads.get(f.seq);
    if (!t) { t = buildThread(f); S.threads.set(f.seq, t); body.appendChild(t); }
    updateThread(t, f);
  }
  for (const [seq, t] of [...S.threads]) if (!seen.has(seq)) { t.remove(); S.threads.delete(seq); }
  // Reorder the DOM only when the shown set/order actually changes. Re-appending
  // every frame would restart each thread's entrance animation and pin the bubbles
  // at opacity 0 forever (threads arrive in time order, so creation order is right).
  const orderKey = show.map((f) => f.seq).join(',');
  if (orderKey !== S.convoOrder) {
    S.convoOrder = orderKey;
    show.forEach((f) => body.appendChild(S.threads.get(f.seq)));
  }
  $('#convo-count').textContent = all.length > 2 ? `2 of ${all.length}` : (all.length ? `${all.length}` : '');
}

function buildThread() {
  const t = el('div', 'thread');
  t.innerHTML = `<div class="thread-head"></div>
    <div class="bubble prompt"><div class="bmeta"></div><span class="ptext"></span></div>
    <div class="acks"></div>
    <div class="queries"></div>
    <div class="bubble reply"><div class="bmeta"></div><span class="rtext"></span></div>`;
  return t;
}
function updateThread(t, f) {
  t.classList.toggle('done', !!f.done);
  t.querySelector('.thread-head').innerHTML =
    `<span class="rt">${escapeHtml(f.agent || '?')}/${escapeHtml(f.owner || '?')}</span> <span class="arr">&#8592;</span> prompt #${f.seq} <span class="arr">·</span> ${escapeHtml(f.session || '')}`;
  t.querySelector('.bubble.prompt .bmeta').innerHTML =
    `<span class="dot cy"></span> prompt &#183; #${f.seq} &#183; ${escapeHtml(f.subject || '')} <span class="btime">${clockOf(f.prompt._ts)}</span>`;
  t.querySelector('.ptext').textContent = f.prompt.text || '';
  // control frames (ack, tool_call) as chips
  const acks = t.querySelector('.acks'); acks.innerHTML = '';
  for (const x of f.ctrls) {
    const chip = el('div', 'ack-chip');
    chip.innerHTML = `<span class="dot"></span> ${escapeHtml(x.c.ft)} &#183; ${escapeHtml(x.c.ctrl || '')}`;
    acks.appendChild(chip);
  }
  // assembled reply prose
  const prose = f.chunks.map((x) => x.c.text).join('');
  const reply = t.querySelector('.bubble.reply');
  const lastTs = f.done ? f.done._ts : (f.last || f.prompt._ts);
  reply.querySelector('.bmeta').innerHTML =
    `<span class="dot or"></span> ${escapeHtml(f.agent || '?')} &#183; ${escapeHtml(f.session || '')} &#183; ` +
    (f.done ? 'done' : '<span style="color:var(--orange-soft)">streaming</span>') +
    ` &#183; ${f.chunks.length} frame${f.chunks.length === 1 ? '' : 's'} <span class="btime">${clockOf(lastTs)}</span>`;
  reply.querySelector('.rtext').innerHTML = escapeHtml(prose) + (f.done ? '' : '<span class="caret"></span>');
  let foot = reply.querySelector('.reply-foot');
  if (f.done) {
    if (!foot) { foot = el('div', 'reply-foot'); reply.appendChild(foot); }
    foot.textContent = `done · chunks=${f.done.chunks} · ${f.done.reason}`;
  } else if (foot) foot.remove();
  // mid-stream queries (§7): agent's question + the caller's answer, as sub-bubbles
  // enclosed between the prompt and the final reply.
  const qbox = t.querySelector('.queries'); qbox.innerHTML = '';
  for (const { c } of f.queries) {
    const d = c.q || {};
    const ans = f.qreplies[d.id];
    const ask = el('div', 'query-ask');
    ask.innerHTML = `<div class="qhead">&#10068; mid-stream query &#183; §7</div>` +
      `<div class="qb qq"><span class="ql">agent asks</span>${escapeHtml(d.prompt || '')}</div>` +
      (ans
        ? `<div class="qb qa"><span class="ql">caller</span>${escapeHtml(ans.answer || '')}</div>`
        : `<div class="qpending">awaiting reply on <span class="qinbox">${escapeHtml(trunc(d.reply_subject || '?', 22))}</span>…</div>`);
    qbox.appendChild(ask);
  }
  if (!prose && !f.chunks.length && !f.ctrls.length && !f.queries.length) reply.style.opacity = '.5'; else reply.style.opacity = '1';
}

// --- the wire (append-only + focus inspector) -------------------------------
function tagFor(o) {
  if (o.type === 'heartbeat' || o.type === 'online') return ['tag-hb', '&#9829; ' + (o.type === 'online' ? 'ONLINE' : 'HB')];
  if (o.type === 'prompt') return ['tag-prompt', '&#9656; PROMPT #' + o.seq];
  if (o.type === 'chunk') {
    if (classifyFrame(o.data).kind === 'query') return ['tag-query', '&#10068; QUERY #' + o.seq];
    return ['tag-reply', '&#9666; #' + o.seq + '&#183;' + o.n];
  }
  if (o.type === 'query_reply') return ['tag-query', '&#8617; ANSWER #' + o.seq];
  if (o.type === 'done') return ['tag-done', '&#9633; DONE #' + o.seq];
  if (o.type === 'sentinel') return ['tag-done', '&#9679; END #' + o.seq];
  if (o.type === 'warn') return ['tag-warn', '! WARN'];
  return ['tag-done', (o.type || '?').toUpperCase()];
}
function wireMain(o) {
  if (o.type === 'heartbeat' || o.type === 'online') return `${escapeHtml(o.agent || '?')}/${escapeHtml(o.owner || '?')} <span class="faint">·</span> ${escapeHtml(o.session || '—')}`;
  if (o.type === 'prompt') return escapeHtml(o.subject || '');
  if (o.type === 'chunk') {
    const c = classifyFrame(o.data);
    if (c.kind === 'prose') return `<span class="frame"><span class="k">${escapeHtml(c.ft)}</span> <span class="s">${escapeHtml(JSON.stringify(c.text))}</span></span>`;
    if (c.kind === 'query') return `<span class="frame"><span class="qk">asks</span> <span class="s">${escapeHtml(JSON.stringify((c.q && c.q.prompt) || ''))}</span></span>`;
    return `<span class="frame"><span class="k">${escapeHtml(c.ft)}</span> <span class="s">${escapeHtml(c.ctrl || '')}</span> <span class="p">&#183; control</span></span>`;
  }
  if (o.type === 'query_reply') return `<span class="frame"><span class="qk">answers</span> <span class="s">${escapeHtml(JSON.stringify(o.answer || ''))}</span></span>`;
  if (o.type === 'done') return `<span class="faint">chunks=${o.chunks} · ${escapeHtml(o.reason || '')}</span>`;
  if (o.type === 'warn') return escapeHtml(o.msg || '');
  return '';
}
function wireMeta(o) {
  if (o.type === 'heartbeat' || o.type === 'online') return trunc(o.instance_id, 14);
  if (o.type === 'prompt') return '&#8617; ' + trunc(o.reply || '(none)', 16);
  if (o.type === 'chunk') return (o.subject ? '&#8617; ' + trunc(o.subject, 16) + ' · ' : '') + (o.bytes != null ? o.bytes + ' B' : '');
  if (o.type === 'query_reply') return o.subject ? '&#8617; ' + trunc(o.subject, 16) : (o.qid ? trunc(o.qid, 12) : '');
  return '';
}
function buildWireRow(o, i) {
  const [tag, label] = tagFor(o);
  const row = el('div', 'ev' + (o.type === 'chunk' && classifyFrame(o.data).kind === 'control' ? ' ctrl' : ''));
  row.innerHTML = `<span class="ev-t">${clockOf(o._ts)}</span><span class="ev-tag ${tag}">${label}</span>` +
    `<span class="ev-main">${wireMain(o)}</span><span class="ev-meta">${wireMeta(o)}</span>`;
  row.addEventListener('click', () => setFocus(S.focus === i ? null : i));
  return row;
}
function renderWire(vis) {
  const body = $('#wire-body');
  while (S.wireCount < vis) { const i = S.wireCount; const r = buildWireRow(S.events[i], i); S.wireEls[i] = r; body.appendChild(r); S.wireCount++; }
  while (S.wireCount > vis) { S.wireCount--; const r = S.wireEls[S.wireCount]; if (r) r.remove(); S.wireEls[S.wireCount] = null; if (S.focus === S.wireCount) S.focus = null; }

  const focusVisible = S.focus != null && S.focus < vis;
  body.classList.toggle('has-focus', focusVisible);
  if (focusVisible) body.style.setProperty('--fc-rgb', accentOf(S.events[S.focus]).rgb); // tint the focused row to match its card
  else body.style.removeProperty('--fc-rgb');
  for (let i = 0; i < vis; i++) {
    const r = S.wireEls[i]; if (!r) continue;
    r.classList.toggle('focused', i === S.focus);
    const o = S.events[i];
    const edge = i === vis - 1 && o.type === 'chunk' && S.flows.get(o.seq) && !S.flows.get(o.seq).done;
    r.classList.toggle('liveedge', !!edge && !focusVisible);
  }
  // inject / refresh the inspector card only when the focus selection changes
  if (S.focus !== S.lastFocus) {
    if (S.focusCard) { S.focusCard.remove(); S.focusCard = null; }
    if (focusVisible) { S.focusCard = buildInspector(S.events[S.focus]); S.wireEls[S.focus].after(S.focusCard); }
    S.lastFocus = S.focus;
  }
  if (S.follow && S.playing) body.scrollTop = body.scrollHeight;
}

function rawFrameHTML(data) {
  let f; try { f = JSON.parse(data); } catch (e) { return `<span class="s">${escapeHtml(JSON.stringify(data))}</span>`; }
  const parts = Object.entries(f).map(([k, v]) => `<span class="k">${escapeHtml(JSON.stringify(k))}</span><span class="b">:</span> <span class="s">${escapeHtml(JSON.stringify(v))}</span>`);
  return `<span class="b">{</span> ${parts.join('<span class="b">,</span> ')} <span class="b">}</span>`;
}
function field(k, v, cls) { return `<span class="fk">${k}</span><span class="fv ${cls || ''}">${escapeHtml(v)}</span>`; }
function buildInspector(o) {
  const card = el('div', 'focus-card ' + accentOf(o).cls);
  const [tag, label] = tagFor(o);
  let stats = '', fields = '', secs = '';
  fields += field('type', o.type) + field('ts', o.ts || '');
  if (o.label) fields += field('label', o.label);
  if (o.subject) fields += field('subject', o.subject, 'cy');
  if (o.reply != null) fields += field('reply', o.reply || '(none — fire-and-forget)', 'cy');
  if (o.instance_id) fields += field('instance', o.instance_id);

  if (o.type === 'chunk') {
    const c = classifyFrame(o.data);
    stats = `<span class="stat">seq <b>#${o.seq}</b></span><span class="stat">n <b>${o.n}</b></span><span class="stat"><b>${o.bytes}</b> B</span><span class="stat">frame <b>${escapeHtml(c.ft)}</b></span>`;
    secs += `<div class="fc-sec"><div class="sec-lbl">Raw frame · data · ${o.bytes} B</div><div class="raw">${rawFrameHTML(o.data)}</div></div>`;
    if (c.kind === 'prose') {
      secs += `<div class="fc-sec"><div class="sec-lbl">Decoded</div><div class="decoded">frame.type = <b style="color:var(--fc-soft)">${escapeHtml(c.ft)}</b> <span class="arrow">&#8594;</span> prose token <span class="tok">${escapeHtml(JSON.stringify(c.text))}</span></div></div>`;
      secs += `<div class="fc-sec"><div class="sec-lbl">Correlation</div><div class="corr">arrived on reply inbox <span class="hl">${escapeHtml(o.subject || '?')}</span><br>(the reply-to of <span class="hl">PROMPT #${o.seq}</span>) <span class="feeds">&#8594; feeds</span> <span class="ho">${escapeHtml(JSON.stringify(c.text))}</span> into the <span class="ho">conversation</span> bubble &#8592;</div></div>`;
    } else if (c.kind === 'query') {
      const d = c.q || {};
      secs += `<div class="fc-sec"><div class="sec-lbl">Mid-stream query · §7</div><div class="corr">the agent paused the stream to ask the caller:<br><span class="ho">${escapeHtml(d.prompt || '')}</span></div></div>`;
      secs += `<div class="fc-fields" style="padding-top:0">${field('query id', d.id || '')}${field('reply_subject', d.reply_subject || '', 'cy')}</div>`;
      secs += `<div class="fc-sec"><div class="sec-lbl">Note</div><div class="decoded">the caller answers on <b>reply_subject</b> — a fresh inbox agenttap does not currently follow, so the answer isn't in this stream.</div></div>`;
    } else {
      secs += `<div class="fc-sec"><div class="sec-lbl">Decoded</div><div class="decoded">control frame <b style="color:var(--ink-dim)">${escapeHtml(c.ft)}</b> &#183; not shown as prose</div></div>`;
    }
  } else if (o.type === 'prompt') {
    stats = `<span class="stat">seq <b>#${o.seq}</b></span>`;
    secs += `<div class="fc-sec"><div class="sec-lbl">Prompt text</div><div class="decoded" style="white-space:pre-wrap">${escapeHtml(o.text || '')}</div></div>`;
    if (o.attachments && o.attachments.length) {
      const a = o.attachments.map((x) => `${escapeHtml(x.filename || '(unnamed)')} [${x.bytes} B]`).join(', ');
      secs += `<div class="fc-sec"><div class="sec-lbl">Attachments · ${o.attachments.length}</div><div class="decoded">${a}</div></div>`;
    }
  } else if (o.type === 'done') {
    stats = `<span class="stat">seq <b>#${o.seq}</b></span><span class="stat">chunks <b>${o.chunks}</b></span><span class="stat">${escapeHtml(o.reason || '')}</span>`;
  } else if (o.type === 'sentinel') {
    stats = `<span class="stat">seq <b>#${o.seq}</b></span><span class="stat">end of stream</span>`;
    secs += `<div class="fc-sec"><div class="sec-lbl">Raw frame · 0 B</div><div class="raw"><span class="b">(empty body · no headers)</span></div></div>`;
    secs += `<div class="fc-sec"><div class="sec-lbl">Decoded · §6.5</div><div class="decoded">the uniform <b style="color:var(--fc-soft)">end-of-stream</b> terminator — a zero-byte reply published to the inbox <span class="hl">${escapeHtml(o.subject || '?')}</span>. Agents MUST NOT publish further on the reply subject after it.</div></div>`;
  } else if (o.type === 'warn') {
    secs += `<div class="fc-sec"><div class="sec-lbl">Message</div><div class="decoded">${escapeHtml(o.msg || '')}</div></div>`;
  } else if (o.type === 'heartbeat' || o.type === 'online') {
    fields += field('agent', o.agent || '') + field('owner', o.owner || '');
    if (o.session) fields += field('session', o.session);
    if (o.hb_ts) fields += field('agent ts', o.hb_ts);
    if (o.interval_s != null) fields += field('interval', o.interval_s + 's');
    const iv = o.interval_s || 0;
    stats = `<span class="stat">${o.type === 'online' ? 'first seen' : 'heartbeat'}</span>` + (iv ? `<span class="stat">every <b>${iv}</b>s</span>` : '');
    if (iv) secs += `<div class="fc-sec"><div class="sec-lbl">Liveness · §8.2</div><div class="decoded">offline after <b style="color:var(--fc-soft)">3 × ${iv}s = ${3 * iv}s</b> without a beat</div></div>`;
  } else if (o.type === 'query_reply') {
    stats = `<span class="stat">seq <b>#${o.seq}</b></span><span class="stat">§7 answer</span>`;
    secs += `<div class="fc-sec"><div class="sec-lbl">Mid-stream query · §7</div><div class="corr">agent asked:<br><span class="ho">${escapeHtml(o.prompt || '')}</span><br>caller answered <span class="feeds">&#8594;</span> <span class="tok">${escapeHtml(JSON.stringify(o.answer || ''))}</span><br>published to reply inbox <span class="hl">${escapeHtml(o.subject || '?')}</span></div></div>`;
    secs += `<div class="fc-fields" style="padding-top:0">${field('qid', o.qid || '')}</div>`;
  }
  // raw frame for any event that carries a payload — heartbeats, prompts, etc. (chunks
  // render their own raw frame above). The "Wireshark" view: see exactly what was on the wire.
  if (o.type !== 'chunk' && typeof o.data === 'string' && o.data.length) {
    secs += `<div class="fc-sec"><div class="sec-lbl">Raw frame${o.bytes != null ? ' · ' + o.bytes + ' B' : ''}</div><div class="raw">${rawFrameHTML(o.data)}</div></div>`;
  }
  if (o.headers) {
    const h = Object.entries(o.headers).map(([k, v]) => `${escapeHtml(k)} = ${escapeHtml([].concat(v).join(', '))}`).join('<br>');
    secs += `<div class="fc-sec"><div class="sec-lbl">Headers</div><div class="decoded">${h}</div></div>`;
  }
  card.innerHTML = `<div class="fc-top"><span class="ev-tag ${tag}">${label}</span><span class="fc-title">${o.type}</span>` +
    `<span class="fc-focus"><span class="ring"></span> Focused <span class="fc-close" title="close">&#10005;</span></span></div>` +
    (stats ? `<div class="fc-stats">${stats}</div>` : '') + `<div class="fc-fields">${fields}</div>` + secs;
  card.querySelector('.fc-close').addEventListener('click', (e) => { e.stopPropagation(); setFocus(null); });
  return card;
}

// --- transport --------------------------------------------------------------
function renderTransport(vis) {
  const span = (S.tEnd - S.t0) || 1;
  const frac = Math.min(1, Math.max(0, (S.playhead - S.t0) / span));
  $('#fill').style.width = (frac * 100) + '%';
  $('#playhead').style.left = (frac * 100) + '%';
  $('#elapsed').textContent = durOf(S.playhead - S.t0);
  const total = durOf(S.tEnd - S.t0);
  $('#total').textContent = total; $('#total2').textContent = total;

  const key = S.events.length + ':' + S.tEnd;
  if (key !== S.tickKey) {
    S.tickKey = key;
    const cls = { prompt: 'cy', chunk: 'or', heartbeat: 'mg', online: 'mg', done: 'dn', sentinel: 'dn', warn: 'wn', query_reply: 'qy', meta: '' };
    $('#ticks').innerHTML = S.events.map((o) => {
      const l = ((o._ts - S.t0) / span) * 100;
      return `<span class="tick ${cls[o.type] || ''}" style="left:${l}%"></span>`;
    }).join('');
  }
  const fl = $('#focuslbl');
  if (S.focus != null && S.events[S.focus]) { const o = S.events[S.focus]; fl.innerHTML = `&#9650; focused: ${tagFor(o)[1]}`; } else fl.textContent = '';
}

function updateStatePill() {
  const pill = $('#statepill'), txt = $('#statetext'), st = $('#tstate'), icon = $('#playicon');
  const live = S.playing && S.follow && S.mode === 'live';
  pill.classList.toggle('live', live);
  txt.textContent = live ? 'Live' : (S.playing ? 'Playing' : 'Paused');
  st.textContent = S.playing ? 'playing' : 'paused';
  icon.innerHTML = S.playing ? '<path d="M6 5h4v14H6zM14 5h4v14h-4z"/>' : '<path d="M8 5v14l11-7L8 5z"/>';
}
function setFocus(i) {
  S.focus = i;
  if (i != null) { S.playing = false; S.follow = false; updateStatePill(); }
}
function togglePlay() {
  S.playing = !S.playing;
  if (S.playing && S.playhead >= S.tEnd) { if (S.mode === 'live') S.follow = true; else S.playhead = S.t0; }
  updateStatePill();
}
function step(dir) {
  S.playing = false; S.follow = false;
  const ph = S.playhead;
  if (dir > 0) { for (const o of S.events) if (o._ts > ph + 0.001) { S.playhead = o._ts; break; } }
  else { for (let i = S.events.length - 1; i >= 0; i--) if (S.events[i]._ts < ph - 0.001) { S.playhead = S.events[i]._ts; break; } }
  updateStatePill();
}
function scrubTo(clientX) {
  const t = $('#track'), r = t.getBoundingClientRect();
  const frac = Math.min(1, Math.max(0, (clientX - r.left) / r.width));
  S.playhead = S.t0 + frac * ((S.tEnd - S.t0) || 0);
  S.follow = false; updateStatePill();
}

$('#play').addEventListener('click', togglePlay);
$('#stepback').addEventListener('click', () => step(-1));
$('#stepfwd').addEventListener('click', () => step(1));
$('#speed').addEventListener('click', (e) => {
  const sp = e.target.closest('span[data-s]'); if (!sp) return;
  S.speed = parseFloat(sp.dataset.s);
  [...$('#speed').children].forEach((c) => c.classList.toggle('on', c === sp));
});
let dragging = false;
$('#track').addEventListener('pointerdown', (e) => { dragging = true; $('#track').setPointerCapture(e.pointerId); scrubTo(e.clientX); });
$('#track').addEventListener('pointermove', (e) => { if (dragging) scrubTo(e.clientX); });
$('#track').addEventListener('pointerup', () => { dragging = false; });
document.addEventListener('keydown', (e) => {
  if (e.code === 'Space') { e.preventDefault(); togglePlay(); }
  else if (e.code === 'ArrowRight') step(1);
  else if (e.code === 'ArrowLeft') step(-1);
  else if (e.code === 'Escape') setFocus(null);
});
