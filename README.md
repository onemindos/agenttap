# agenttap

[![CI](https://github.com/M64GitHub/agenttap/actions/workflows/ci.yml/badge.svg)](https://github.com/M64GitHub/agenttap/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/M64GitHub/agenttap.svg)](https://pkg.go.dev/github.com/M64GitHub/agenttap)
[![Go Report Card](https://goreportcard.com/badge/github.com/M64GitHub/agenttap)](https://goreportcard.com/report/github.com/M64GitHub/agenttap)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**A live, read-only protocol visualizer for [Synadia](https://www.synadia.com/) agent-SDK traffic on [NATS](https://nats.io/) — in your terminal, or animated in your browser.**

`agenttap` connects to a NATS account and passively watches agents talk — every
prompt, every streamed reply chunk, every heartbeat as it crosses the bus. Think
**“Wireshark for AI agents.”** Read it as a compact log, a scrolling visualizer
that reassembles each exchange into a chat-style bubble, or a full-screen
**dashboard** — a live agent roster (with heartbeat-fading hearts) plus a packet
list you can scrub and inspect frame by frame. All in ASCII with truecolor.

It also emits a machine-readable **`json`** event stream that powers
**`agentweb`** — a self-contained, animated **browser** visualizer (“The
Stream”): conversation bubbles on one side, every raw wire frame on the other,
with a transport bar to play, pause, scrub, and inspect — live or from a
recording. So this repo ships **two binaries**:

- **`agenttap`** — the tap and its terminal views (`log`, `viz`, `dash`, `json`).
- **`agentweb`** — the browser visualizer; it consumes `agenttap`'s `json`
  stream and serves a single page over Server-Sent Events. It speaks no NATS.

Both are **pure observers**: they never publish, never answer a prompt, and
never modify traffic. `agenttap` writes nothing to disk and runs no JetStream —
just a single connection and your terminal; `agentweb` adds only a local web
server.

> Tip: `agenttap -demo` plays a scripted exchange in the terminal with no
> connection — and `make web-demo` does the same in the browser.

---

## What it looks like

The `viz` view renders each message on the bus as an ASCII packet card and
assembles the streamed reply into a bubble when it completes (shown here with
`-no-color`; on a real terminal the chrome is colorized):

```
======================================================================
           A G E N T T A P   ::   live agent-protocol trace
======================================================================
[PROMPT] client>>agent   [CHUNK] agent>>client   [END] <empty>   [ONLINE]   hb=heartbeat

18:38:55.676  [ONLINE]  hermes/rs (miami-demo)  inst=P6jQnJ56tG..
18:38:55.676  hb   hermes/rs (miami-demo)  inst=P6jQnJ56tG..
+- [PROMPT] client >> agent ------------------- #0001  18:38:56.518 -+
| subject  agents.prompt.hermes.rs.miami-demo                        |
| reply    _INBOX.agents.FYFA2ZE57WAKEQRTQGNFGI                      |
| headers  Nats-Msg-Id=01J8Z3K9QWERTYUIOP; X-Agent-Protocol=0.3      |
| attach   telemetry.json [2.4 KiB]                                  |
+--------------------------------------------------------------------+
| What's the weather in Miami for the F1 race this weekend, and is   |
| rain likely during the session?                                    |
+--------------------------------------------------------------------+

+- [CHUNK 1] agent >> client ------------ #0001  18:38:56.939  30 B -+
| {"type":"status","data":"ack"}                                     |
+--------------------------------------------------------------------+

+- [CHUNK 2] agent >> client ------------ #0001  18:38:57.369  64 B -+
| {"type":"response","data":"The Miami GP forecast looks great: "}   |
+--------------------------------------------------------------------+

+- [END] agent >> client ---------------------- #0001  18:38:59.053 -+
| subject  _INBOX.agents.FYFA2ZE57WAKEQRTQGNFGI                      |
| data     <empty>                                                   |
| note     stream terminator -- 0 bytes, no headers                  |
+--------------------------------------------------------------------+

-------------- exchange #0001 : hermes/rs (miami-demo) ---------------
.-----------------------------------------------.
| [USER] rs (miami-demo) -> hermes              |
+-----------------------------------------------+
| What's the weather in Miami for the F1 race   |
| this weekend, and is rain likely during the   |
| session?                                      |
| attach: telemetry.json [2.4 KiB]              |
'-----------------------------------------------'
                     .-----------------------------------------------.
                     | [AGENT] hermes (miami-demo)                   |
                     +-----------------------------------------------+
                     | The Miami GP forecast looks great: mostly     |
                     | sunny skies with highs around 30°C (86°F).    |
                     | Winds are light from the east at 10-15 km/h.  |
                     | Rain probability during the race window is    |
                     | low, under 10%.                               |
                     '-----------------------------------------------'
                     5 frames (4 text, 1 ctrl) | 336 B | 2.54s | sentinel
```

Note how the **packet cards** show the raw wire frames
(`{"type":"response","data":"…"}`), while the **agent bubble** reconstructs the
human-readable message from the text frames and reports the breakdown
(`5 frames (4 text, 1 ctrl)`).

*Prefer pixels to ASCII?* The same exchange drives **`agentweb`** in the
browser — animated conversation bubbles beside a live wire view of every
frame. See the **`agentweb`** section below.

---

## Features

- **Four output modes** — a compact line `log`, a scrolling `viz` protocol
  visualizer, a full-screen `dash` analyzer (a live agent roster plus a
  one-line-per-event traffic tail), and a machine-readable **`json`** (NDJSON)
  event stream.
- **Browser visualizer (`agentweb`)** — pipe the `json` stream into a
  self-contained, animated web dashboard: conversation bubbles beside a live
  “wire” view of every frame, a transport bar to **play / pause / scrub**, and
  click-to-inspect on any event. Runs live or replays a recording.
- **Live packet cards** for prompts, every reply chunk, and the end-of-stream
  sentinel (rendered as `<empty>`), showing subject, reply inbox, headers,
  byte sizes, and payload.
- **Assembled reply bubbles** — streamed `{"type":"response","data":"…"}`
  deltas are concatenated back into the actual message; control frames
  (`status`, `ack`, …) are counted but kept out of the prose.
- **Every heartbeat of every agent**, one line per beat.
- **Attachments** surfaced with name and decoded size.
- **Truecolor, ASCII-forward chrome** that degrades to clean plain text — pipe
  it to a file and it stays readable (`-no-color`).
- **Auto-sized** to your terminal width.
- **Safe**: read-only, writes nothing, and sanitizes payloads so a rogue byte
  sequence can't corrupt your terminal or inject escape codes.
- **Lean**: `agenttap` has one dependency
  ([`nats.go`](https://github.com/nats-io/nats.go)); `agentweb` is pure standard
  library (no NATS, no web framework). No JetStream, no embedded broker.

---

## Install

With Go 1.25+:

```sh
go install github.com/M64GitHub/agenttap@latest                 # agenttap
go install github.com/M64GitHub/agenttap/cmd/agentweb@latest    # agentweb
```

Or build both from source:

```sh
git clone https://github.com/M64GitHub/agenttap
cd agenttap
make            # builds ./agenttap and ./agentweb
```

---

## Quick start

`agenttap` connects using a named [`nats context`](https://github.com/nats-io/natscli)
(the same contexts the `nats` CLI manages under `~/.config/nats`). Pass the
context name:

```sh
agenttap -view viz my-context
```

No agents handy? Rehearse with the built-in demo (no connection required):

```sh
agenttap -demo -view viz
```

Record a trace to a file while still watching connection status on the
terminal (event output goes to stdout, diagnostics to stderr):

```sh
agenttap -view viz -no-color my-context > trace.log
```

### …or in the browser

Pipe the `json` stream into `agentweb` and open the URL it prints:

```sh
agenttap -view json my-context | agentweb -open      # live → http://127.0.0.1:8088
```

No agents handy? `make web-demo` builds both binaries and serves the scripted
demo. Full details — record/replay, remote access — in the **`agentweb`**
section below.

---

## Views

### `viz` — protocol visualizer (scrolling)

Packet cards for every message, plus the assembled bubble shown above. Best
for demos, debugging a single exchange, and watching the protocol flow.

### `log` — compact line log (default)

One tagged, timestamped line per event with indented detail — ideal for
greppable logs and high-volume streams.

```
18:39:06.453  PROMPT  hermes/rs (miami-demo)  #0001
        subject: agents.prompt.hermes.rs.miami-demo
        reply: _INBOX.agents.FYFA2ZE57WAKEQRTQGNFGI
        headers: Nats-Msg-Id=01J8Z3K9QWERTYUIOP; X-Agent-Protocol=0.3
        attach: 1 — telemetry.json [2.4 KiB]
        text:
          What's the weather in Miami for the F1 race this weekend, ...
18:39:06.874  REPLY   hermes/rs (miami-demo)  #0001  chunk 1 (30 B)
        data:
          {"type":"status","data":"ack"}
```

In the `log` view, heartbeats are summarized as a single `ONLINE` line the
first time each agent is seen; pass `-hb` to print every beat. The `viz` view
always shows every beat.

### `dash` — full-screen analyzer (redraw-in-place)

A live, full-screen dashboard for watching *all* traffic at once — the closest
thing here to a Wireshark packet list. The screen splits into three panels: a
**roster** of every discovered agent (left), a **detail** panel for the
selected agent (top right), and a dense **one-line-per-event traffic tail** of
the whole bus (bottom right).

Each agent carries a **heart** (`♥`) whose color tracks heartbeat liveness: it
pulses bright on every beat, then fades green → yellow → gray as beats go stale,
and the agent drops off the roster once it falls fully silent. The beat interval
is estimated per agent (≈5s until learned). The traffic tail shows every event —
heartbeats, prompts, each response and control frame (`ack`, `tool_call`, …),
the end sentinel, and the done summary — as `time · type · from -> to · detail`,
newest at the bottom.

`dash` is **interactive** (so it needs a real terminal — it can't be piped).
`←` / `→` move focus between the **roster** and the **traffic list**, and the
DETAIL pane follows whichever is focused — like Wireshark's packet list and
packet detail:

- **Roster focus** — `↑`/`↓` select an agent; DETAIL shows that agent's
  conversation (prompt + assembled reply + stats); the traffic list live-tails.
- **Feed focus** — `↑`/`↓` move a **packet cursor** through the traffic list and
  DETAIL shows that packet's full decode: type, endpoints, full subject and
  reply inbox, headers, sizes, and payload (only the payload is shortened).
  Scrolling up **freezes** the live tail (the header shows `PAUSED +N` for the
  events that arrived below); returning to the newest line (or `G`) resumes it.

| Key                    | Action                                              |
| ---------------------- | --------------------------------------------------- |
| `←` / `→`              | Move focus between the roster and the traffic list. |
| `↑` / `↓` (or `k`/`j`) | Select an agent / scrub the packet cursor (by focus). |
| `g` / `G`              | Jump to first / last (last resumes live tailing).   |
| `space`                | Mute the focused agent (hide it from the tail).     |
| `s`                    | Solo: mute every agent except the focused one.      |
| `a`                    | Unmute all.                                         |
| `h`                    | Toggle heartbeat lines in the tail on/off.          |
| `q` / `Ctrl-C`         | Quit (and restore the terminal).                    |

Watch it run without a connection — two demo agents stream replies
concurrently; here `hermes` is selected, so its still-growing reply fills the
detail panel while both streams interleave in the tail (colorized on a real
terminal):

```sh
agenttap -demo -view dash
```

```
+= agenttap ======================================================== 21:12:57   4 agents | 2 live =+
|AGENTS                   |DETAIL  hermes/rs (miami)  ♥ 1.90s ~3.09s                               |
|  [x] ♥ atlas/rs (ops)   |inst P6jQnJ56tG..   5 beats                                             |
|  [x] ♥ echo/m64 (m64)   |reply _IN..UHTHPJ..PBK9                                                 |
|> [x] ♥ hermes/rs (miami)|client> How many requests did we serve in the last hour, broken down by |
|  [x] ♥ nyx/rs (research)|        region?                                                         |
|                         |agent > Sure -- here's a concise answer streamed as several deltas, so  |
|                         |        you can watch the reply grow token by  (streaming)              |
|                         |20 fr (19 txt/1 ctrl)  682 B  3.40s                                     |
|                         |                                                                        |
|                         +------------------------------------------------------------------------+
|                         |LIVE TRAFFIC   hb:on                                                    |
|                         |21:12:55.702 resp 11 hermes -> _IN..UHTHPJ..PBK9  deltas,               |
|                         |21:12:55.786 resp 32 echo -> _IN..Y2SCZ0..88C8    frame                 |
|                         |21:12:55.873 resp 12 hermes -> _IN..UHTHPJ..PBK9  so                    |
|                         |21:12:55.957 resp 33 echo -> _IN..Y2SCZ0..88C8    on                    |
|                         |21:12:56.043 resp 13 hermes -> _IN..UHTHPJ..PBK9  you                   |
|                         |21:12:56.128 resp 34 echo -> _IN..Y2SCZ0..88C8    the                   |
|                         |21:12:56.214 resp 14 hermes -> _IN..UHTHPJ..PBK9  can                   |
|                         |21:12:56.299 resp 35 echo -> _IN..Y2SCZ0..88C8    wire.                 |
|                         |21:12:56.384 resp 15 hermes -> _IN..UHTHPJ..PBK9  watch                 |
|                         |21:12:56.470 END     echo -> _IN..Y2SCZ0..88C8    0 B stream terminator |
|                         |21:12:56.470 DONE    echo (m64)                   35 fr (34 txt/1 ctrl) |
|                         |21:12:56.555 resp 16 hermes -> _IN..UHTHPJ..PBK9  the                   |
|                         |21:12:56.726 resp 17 hermes -> _IN..UHTHPJ..PBK9  reply                 |
|                         |21:12:56.896 resp 18 hermes -> _IN..UHTHPJ..PBK9  grow                  |
|                         |21:12:57.067 resp 19 hermes -> _IN..UHTHPJ..PBK9  token                 |
|                         |21:12:57.238 resp 20 hermes -> _IN..UHTHPJ..PBK9  by                    |
+==================================================================================================+
  up/down select   space mute   s solo   a all   h heartbeats(on)   q quit
```

(`2 live` because the demo beats agents at staggered phases, so only some are
within a fresh interval at any instant; `nyx` goes quiet partway through to show
the heart fade and drop.)

Press `→` to focus the traffic list, then `↑` to scrub back through events — the
tail **freezes** and the DETAIL pane decodes the selected packet in full: the
agent's full identity and **instance id**, the originating prompt **subject**,
and the complete reply **inbox** (vs. the shortened forms in the list), with
`PAUSED +N` showing how many events have streamed in below:

```
+= agenttap ======================================================== 13:52:50   4 agents | 4 live =+
|AGENTS                   |PACKET #13  resp 3   13:52:47.126                                       |
|  [x] ♥ atlas/rs (ops)   |type      response (frame 3)                                            |
|  [x] ♥ echo/m64 (m64)   |agent     echo/m64 (m64)                                                |
|> [x] ♥ hermes/rs (miami)|instance  Ab12Cd34Ef56Gh78                                              |
|  [x] ♥ nyx/rs (research)|subject   agents.prompt.echo.m64.m64                                    |
|                         |reply     _INBOX.agents.0AZYHE6089P5KOHFC6DGRY                          |
|                         |bytes     32 B                                                          |
|                         |t+        421ms                                                         |
|                         |data      --                                                            |
|                         +------------------------------------------------------------------------+
|                         |LIVE TRAFFIC   PAUSED +21   hb:on                                       |
|                         |> 13:52:47.126 resp 3  echo -> _IN..0AZYHE..DGRY    --                  |
|                         |  13:52:47.296 resp 4  echo -> _IN..0AZYHE..DGRY    here's              |
|                         |  13:52:47.466 resp 5  echo -> _IN..0AZYHE..DGRY    a                   |
|                         |  13:52:47.637 resp 6  echo -> _IN..0AZYHE..DGRY    concise             |
|                         |  13:52:47.808 resp 7  echo -> _IN..0AZYHE..DGRY    answer              |
|                         |  13:52:47.979 resp 8  echo -> _IN..0AZYHE..DGRY    streamed            |
+==================================================================================================+
  left/right pane   up/down scrub packets   space mute   s solo   a all   h hb(on)   q quit
```

### `json` — machine-readable event stream (NDJSON)

Emits one self-describing JSON object per line — every prompt, reply chunk,
end-of-stream sentinel, done summary, online, heartbeat, and mid-stream query
reply — each stamped with an observation timestamp. It filters nothing and truncates nothing: a complete,
replayable record of the bus. It's the stream **`agentweb`** consumes, and it's
useful on its own:

```sh
agenttap -view json my-context | jq                 # inspect live
agenttap -view json my-context > session.ndjson     # record a session
```

Because every line carries a `ts`, a recording replays later at its original
pace (see `agentweb -replay`).

---

## agentweb — the browser visualizer

`agentweb` turns the `json` stream into **“The Stream”**: a single, self-contained
web page that animates the protocol as it flows. It reads NDJSON on stdin (or from
a file with `-replay`), serves one page, and pushes events to the browser over
**Server-Sent Events**. It imports no NATS and writes nothing to disk — a pure
presenter, standard library only.

```sh
# live: tap a context and stream it to the browser
agenttap -view json my-context | agentweb -open

# record once, then replay deterministically (ideal for demos and talks)
agenttap -view json my-context > session.ndjson
agentweb -replay session.ndjson -open

# no agents at all — build both binaries and serve the scripted demo
make web-demo                                        # → http://127.0.0.1:8088
```

### The layout

Three zones and a transport bar:

- **Roster** — every agent discovered from heartbeats, as a glowing node with a
  pulsing heart; the agent currently replying lights up.
- **Conversation** — the human-readable exchange as chat bubbles (the prompt, and
  the reply assembling token by token), stacked **up to two threads** so a
  delegation — one agent prompting another — is visible side by side. Mid-stream
  queries (§7) appear as sub-bubbles between the prompt and reply — the agent's
  question and the caller's answer.
- **The Wire** — every raw event in time order; **click any row** to expand it
  into a full inspector (raw frame, decoded type, the correlation back to its
  prompt) and spotlight it.
- **Transport** — **play / pause / step / scrub / speed**, a live clock, and a
  timeline with color-coded event ticks. Keyboard: `space`, `←` / `→`, `Esc`.

The page is a pure function of *(event log + playhead)*: **playing** is the
flowing visualizer; **pausing** turns it into a scrub-and-inspect analyzer — same
data, two modes. Live mode follows the head as events arrive; replay walks the
recorded timeline at the original pace (honoring each event's `ts`), scaled by
the speed control.

### `agentweb` options

```
usage: agentweb [flags]      # reads NDJSON on stdin unless -replay is given
```

| Flag      | Default          | Description                                              |
| --------- | ---------------- | -------------------------------------------------------- |
| `-addr`   | `127.0.0.1:8088` | Listen address. Use `0.0.0.0:8088` to reach it remotely. |
| `-replay` | *(stdin)*        | Read the NDJSON stream from this file instead of stdin.  |
| `-open`   | `false`          | Open the page in a browser on start.                     |

A backlog buffer means a page reload — or a second tab — receives the full
history before the live tail, so you never miss what already streamed.

> **Heads-up:** binding `0.0.0.0` exposes the page — and the observed prompt and
> reply content — to anyone who can reach that host:port. Fine on a trusted
> network; keep the default `127.0.0.1` otherwise.

---

## `agenttap` command-line options

```
usage: agenttap [flags] <nats-context>
```

| Flag         | Default | Description                                                        |
| ------------ | ------- | ------------------------------------------------------------------ |
| `-view`      | `log`   | Output view: `log`, `viz`, `dash`, or `json`.                              |
| `-hb`        | `false` | `log` view: print every heartbeat (`viz` always does).             |
| `-idle`      | `5m`    | Give up on a reply stream after this much silence.                 |
| `-truncate`  | `0`     | Truncate prompt/reply text to N bytes (`0` = no limit).            |
| `-width`     | `0`     | `viz` layout width in columns (`0` = auto-detect from terminal).   |
| `-color`     | `auto`  | Colorize output: `auto`, `always`, or `never`.                     |
| `-no-color`  | `false` | Disable all color (handy for piping); overrides `-color`.          |
| `-demo`      | `false` | Render a scripted demo exchange and exit (no NATS connection).     |

`auto` color is enabled only when stdout is a terminal, and is disabled if the
[`NO_COLOR`](https://no-color.org/) environment variable is set.

The `dash` view is full-screen and interactive: it requires a terminal on
stdout (it refuses to run when piped) and auto-sizes to the whole window, so
`-width` and `-truncate` don't apply to it.

---

## How it works

`agenttap` subscribes to two wildcard subjects on the agents account:

- `agents.prompt.>` — prompts. The subject encodes the target as
  `agents.prompt.{agent}.{owner}.{session}`, and each prompt message carries a
  **reply inbox** in its NATS reply-to field.
- `agents.hb.>` — agent heartbeats (identity + liveness).

When a prompt is observed, `agenttap` reactively subscribes to that prompt's
reply inbox and prints each chunk as it streams in. The agent SDK terminates a
reply stream with an empty, header-less sentinel message, which `agenttap`
renders as the `[END]` card and uses to finalize the exchange. A stream that
goes silent for `-idle` is closed out as `idle-timeout`.

### Reply-frame assembly

Agents stream replies as a sequence of JSON frames, e.g.
`{"type":"response","data":"<delta>"}`, often preceded by a control frame like
`{"type":"status","data":"ack"}`. The packet cards show these raw (wire truth),
while the closing bubble concatenates the `data` of the **text** frames into
the real message. Frame types such as `status`, `error`, `ack`, `ping`,
`start`, `done`, `usage`, `metadata`, `tool_call`, and `tool_result` are
treated as control frames and excluded from the prose (but counted). A chunk
that isn't a typed frame is treated as raw reply text, so plain-text agents
work too.

### Mid-stream queries (§7)

An agent may pause its reply to ask the caller a question — a permission prompt or
a clarification — by emitting a `query` frame carrying a fresh `reply_subject`.
When `agenttap` sees one it follows that subject and captures the caller's one-shot
answer, so both halves surface: the question (a `query` chunk) and the answer (a
`query_reply`, tagged with the `reply_subject` it was published to). In `agentweb`
the pair renders as sub-bubbles between the prompt and the final reply; the
terminal views capture them as events.

### A note on the reply-capture race

`agenttap` is a side-observer, not the agent. It learns a reply inbox only when
it sees the prompt, and then subscribes to it. The real agent received the same
prompt at the same moment and may begin streaming immediately. Because core
NATS has no replay for a subject with no interest yet, **a very eager agent can
emit its first chunk(s) before `agenttap`'s subscription registers on the
server** — those would be missed. `agenttap` minimizes the window by
subscribing before doing any rendering work, but the race is inherent to
passively observing point-to-point request/reply traffic. For a guaranteed
complete transcript you would need the agent to mirror replies to a stable
subject, or a JetStream-backed recorder in the actual reply path.

### Safety

All displayed payload text is sanitized: control bytes are replaced and ESC is
dropped, so a payload can neither corrupt the box layout nor inject its own
ANSI escape sequences into your terminal.

---

## Requirements

- Go 1.25 or newer (to build).
- A reachable NATS account and a named `nats context`. Create one with the
  [`nats` CLI](https://github.com/nats-io/natscli):

  ```sh
  nats context add my-context --server nats://localhost:4222
  # or, for Synadia Cloud (NGS):
  nats context add ngs --server tls://connect.ngs.global --creds /path/to/user.creds
  ```

  `agenttap` reads the context file directly and honors `url`, `creds`,
  `nkey`, `user`/`password`, `token`, `cert`/`key`, `ca`, `inbox_prefix`,
  inline `user_jwt`/`user_seed`, and `tls_first`.
- **`agentweb`** needs only a web browser — it consumes `agenttap`'s `json`
  output and never connects to NATS itself.

---

## Contributing

Contributions are welcome — see [CONTRIBUTING.md](CONTRIBUTING.md). In short:
`go test ./...`, `go vet ./...`, and `gofmt` should all be clean before you
open a PR.

## License

[MIT](LICENSE) © m64
