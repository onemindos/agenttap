# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- `dash` view: a full-screen, redraw-in-place protocol analyzer (alt-screen). A
  left roster of every agent with a heartbeat-driven **heart** that pulses on
  each beat and fades green → yellow → gray as it goes stale (per-agent interval
  estimate), a detail panel for the selected agent (prompt + assembled reply +
  frame/byte stats), and a dense one-line-per-event live traffic tail of the
  whole bus. Interactive: select agents (`↑`/`↓` or `k`/`j`), mute (`space`),
  solo (`s`), unmute all (`a`), toggle heartbeat lines (`h`), quit (`q`/`Ctrl-C`).
  TTY-guarded and restores the terminal cleanly on exit; `-demo -view dash`
  showcases it with a small live traffic generator.
- `dash`: Wireshark-style packet inspection. `←`/`→` switch focus between the
  agent roster and the traffic list; in feed focus `↑`/`↓` move a packet cursor
  (freezing the live tail, with a `PAUSED +N` indicator) and the DETAIL pane
  shows the selected packet's full decode. Every exchange packet (prompt, reply
  frame, END, DONE) carries a consistent identity block — the agent's full
  identity and instance id, the originating prompt subject, and the reply inbox
  (so a reply is no longer just "pi") — plus headers, sizes, and payload (only
  the payload is shortened); heartbeat packets show their `ts`. In roster focus
  the DETAIL pane keeps the agent-conversation view.

## [0.1.0] - 2026-05-26

Initial release.

### Added
- Read-only tap on Synadia agent-SDK NATS traffic (`agents.prompt.>`,
  `agents.hb.>`, and per-prompt reply inboxes).
- `viz` view: scrolling protocol visualizer with ASCII packet cards for
  prompts, reply chunks, and the end-of-stream sentinel, plus a chat-style
  bubble that assembles each completed reply.
- `log` view: compact, line-oriented colored log (default).
- Reply-frame assembly: concatenates `{"type":"response","data":"…"}` deltas
  into the message and excludes control frames (`status`, `ack`, …).
- One line per heartbeat for every agent.
- Truecolor output that degrades to clean plain text (`-color`, `-no-color`),
  auto terminal-width detection (`-width`), and payload truncation
  (`-truncate`).
- Payload sanitization to prevent terminal corruption / ANSI injection.
- `-demo` mode that plays a scripted exchange with no NATS connection.
- Self-contained `nats context` loader (no `jsm.go` / JetStream dependency).

[Unreleased]: https://github.com/M64GitHub/agenttap/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/M64GitHub/agenttap/releases/tag/v0.1.0
