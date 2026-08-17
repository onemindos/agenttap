# agenttap — read-only NATS agent-protocol visualizer (pure observer)

Go 1.25 · module github.com/M64GitHub/agenttap · one dependency: nats.go

## Build / test
- `make ci` — MUST be green before calling anything done (gofmt-check · vet · test -race · build)
- `make demo VIEW=log|viz|dash|json` — scripted demo, no NATS needed
- `go run . -view dash <nats-context>` — against real traffic

## Hard rules (don't break these)
- **Pure observer**: never publish, answer, or modify traffic; write nothing to disk.
- **One dependency** (`nats.go`). Never reintroduce `jsm.go`/JetStream/nats-server — it drags
  in tens of MB. Verify: `go list -deps . | grep -iE 'jsm|nats-server'` → empty.
- **Sanitize** every payload string before display (`sanitize()`), or a payload can corrupt
  the layout / inject ANSI.
- **ASCII-forward**; `♥` (U+2665) is the only sanctioned non-ASCII glyph. Color does the work.
- **dash concurrency**: view methods only mutate state under `mu` and return fast — they NEVER
  write to the screen. The render goroutine is the sole writer.

## Map
- `main.go` — flags, NATS subscribe + dispatch, the `view` interface (the seam), `log` view, helpers
- `viz.go` — `viz` view + rendering primitives (box/color/width/wrap/sanitize) + demo
- `dash.go` — `dash` full-screen TUI: roster + packet list/detail, focus model, render loop
- `json.go` — `json` view: NDJSON event stream (one event/line, recordable; feeds cmd/agentweb)
- `natscontext.go` — self-contained `nats context` loader (replaces jsm.go)

## Notes
- Four views via `-view`: `log` (default), `viz`, `dash`, `json` (NDJSON for tooling/replay). User-facing docs live in `README.md`.
- The reply-capture race is inherent to side-observing request/reply — don't "fix" it by
  buffering; just subscribe before rendering.
- Deep design history/provenance: `tmp/handoff.md` (gitignored, long — skim only if needed).
