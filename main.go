// Command agenttap is a live, read-only protocol visualizer for Synadia
// agent-SDK traffic on NATS.
//
// It connects a single NATS context and passively observes prompt and
// heartbeat traffic on the agents account, reactively follows each prompt's
// reply inbox, and renders everything in the terminal. It is a pure observer:
// it never publishes, never answers prompts, and never modifies traffic.
//
// Four views are available (-view):
//
//	log   a compact, line-oriented colored log (default)
//	viz   a scrolling "protocol visualizer": ASCII packet cards for every
//	      message on the bus, plus a chat-style bubble assembling each reply
//	dash  a full-screen, redraw-in-place analyzer: a live agent roster with
//	      heartbeat-fading hearts, a detail panel for the selected agent, and a
//	      one-line-per-event live traffic tail of the whole bus
//	json  newline-delimited JSON (NDJSON), one self-describing event per line
//	      with an observation timestamp; a machine-readable stream for tooling
//	      (e.g. cmd/agentweb) and for recording and replaying a session
//
// Event lines go to stdout; status/diagnostics go to stderr, so you can pipe
// the trace to a file while still watching connection state on the terminal.
package main

import (
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

const (
	appName = "agenttap"

	subjHeartbeats = "agents.hb.>"
	subjPrompts    = "agents.prompt.>"
	subjPromptRoot = "agents.prompt."
)

// Reasons recorded on the terminal "done" line for each prompt.
const (
	doneSentinel = "sentinel"
	doneIdle     = "idle-timeout"
	doneShutdown = "shutdown"
	doneNoReply  = "no-reply"
	doneSubErr   = "subscribe-error"
)

func main() {
	cfg, err := parseFlags()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		flag.Usage()
		os.Exit(2)
	}

	// dash is a full-screen TUI: it needs a real terminal on stdout and cannot
	// be piped. Guard early with a clear message (log/viz remain pipeable).
	if cfg.view == viewDash && !stdoutIsTerminal() {
		fmt.Fprintf(os.Stderr,
			"%s: -view dash is a full-screen UI and needs a terminal on stdout (it cannot be piped).\n"+
				"Use -view log or -view viz to pipe or redirect output.\n", appName)
		os.Exit(2)
	}

	out := newView(cfg)
	defer out.stop() // restores the terminal for dash; no-op (and idempotent) otherwise

	// Demo mode renders without a NATS connection.
	if cfg.demo {
		out.banner()
		if d, ok := out.(*dashView); ok {
			go runDashDemo(d)
			<-d.quit() // q / Ctrl-C / SIGTERM
		} else {
			runDemo(out)
		}
		return
	}

	nc, err := connectContext(cfg.ctxName)
	if err != nil {
		log.Fatal(err)
	}

	a := &app{
		nc:   nc,
		out:  out,
		tr:   newTracker(cfg.idle),
		seen: newSeenSet(),
		idle: cfg.idle,
	}

	// Surface async NATS problems through the view. dash owns the screen, so
	// these must never be written straight to the terminal.
	nc.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, e error) { out.warnf("nats: %v", e) })
	nc.SetDisconnectErrHandler(func(_ *nats.Conn, e error) {
		if e != nil {
			out.warnf("nats disconnected: %v", e)
		}
	})
	nc.SetReconnectHandler(func(c *nats.Conn) { out.warnf("nats reconnected: %s", c.ConnectedUrl()) })

	// Subscribe before banner() so dash enters the alt-screen last: a subscribe
	// error then still prints cleanly to the normal screen.
	hbSub, err := nc.Subscribe(subjHeartbeats, a.onHeartbeat)
	if err != nil {
		log.Fatalf("subscribe heartbeats: %v", err)
	}
	promptSub, err := nc.Subscribe(subjPrompts, a.onPrompt)
	if err != nil {
		log.Fatalf("subscribe prompts: %v", err)
	}

	if cfg.view != viewDash {
		log.Printf("connected context=%q url=%s", cfg.ctxName, nc.ConnectedUrl())
		log.Printf("watching %s and %s — Ctrl-C to stop", subjPrompts, subjHeartbeats)
		if cfg.view == viewLog && !cfg.showHB {
			log.Printf("(heartbeats shown as ONLINE on first sight; pass -hb to print every beat)")
		}
	}
	out.banner()

	// Wait for shutdown: Ctrl-C / SIGTERM for log/viz; dash routes those (and
	// the q key) through its own quit channel.
	if d, ok := out.(*dashView); ok {
		<-d.quit()
	} else {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		<-sigs
	}

	out.stop() // restore the terminal before any further logging (dash)
	log.Printf("shutting down")
	_ = hbSub.Unsubscribe()
	_ = promptSub.Unsubscribe()
	a.closeAll()
	_ = nc.Drain()
}

// stdoutIsTerminal reports whether stdout is a character device (a terminal).
func stdoutIsTerminal() bool {
	fi, err := os.Stdout.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// --- config -----------------------------------------------------------------

const (
	viewLog  = "log"
	viewViz  = "viz"
	viewDash = "dash"
	viewJSON = "json"
)

type config struct {
	ctxName  string
	view     string
	showHB   bool
	idle     time.Duration
	truncate int
	width    int
	color    bool
	demo     bool
}

func parseFlags() (config, error) {
	var (
		viewMode  = flag.String("view", viewLog, "output view: log|viz|dash|json")
		showHB    = flag.Bool("hb", false, "log view: print every heartbeat (viz always shows every beat)")
		idleStr   = flag.String("idle", "5m", "give up on a reply stream after this much silence")
		truncate  = flag.Int("truncate", 0, "truncate prompt/reply text to this many bytes (0 = no limit)")
		width     = flag.Int("width", 0, "viz view: layout width in columns (0 = auto-detect)")
		colorMode = flag.String("color", "auto", "colorize output: auto|always|never")
		noColor   = flag.Bool("no-color", false, "disable all color (handy for piping/redirecting); overrides -color")
		demo      = flag.Bool("demo", false, "render a scripted demo exchange and exit (no NATS connection)")
	)
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [flags] <nats-context>\n\nflags:\n", appName)
		flag.PrintDefaults()
	}
	flag.Parse()

	if *viewMode != viewLog && *viewMode != viewViz && *viewMode != viewDash && *viewMode != viewJSON {
		return config{}, fmt.Errorf("invalid -view %q (want log|viz|dash|json)", *viewMode)
	}
	idle, err := time.ParseDuration(*idleStr)
	if err != nil || idle <= 0 {
		return config{}, fmt.Errorf("invalid -idle %q: want a positive duration (e.g. 90s, 5m)", *idleStr)
	}
	color, err := resolveColor(*colorMode)
	if err != nil {
		return config{}, err
	}
	if *noColor {
		color = false
	}

	ctxName := ""
	if *demo {
		if flag.NArg() > 1 {
			return config{}, fmt.Errorf("-demo takes no context argument")
		}
	} else {
		if flag.NArg() != 1 {
			return config{}, fmt.Errorf("expected exactly one context name, got %d", flag.NArg())
		}
		ctxName = flag.Arg(0)
	}

	return config{
		ctxName:  ctxName,
		view:     *viewMode,
		showHB:   *showHB,
		idle:     idle,
		truncate: *truncate,
		width:    *width,
		color:    color,
		demo:     *demo,
	}, nil
}

func resolveColor(mode string) (bool, error) {
	switch mode {
	case "always":
		return true, nil
	case "never":
		return false, nil
	case "auto":
		if os.Getenv("NO_COLOR") != "" {
			return false, nil
		}
		fi, err := os.Stdout.Stat()
		return err == nil && fi.Mode()&os.ModeCharDevice != 0, nil
	default:
		return false, fmt.Errorf("invalid -color %q (want auto|always|never)", mode)
	}
}

// --- wire types (subset of the agent SDK protocol) --------------------------

type heartbeatPayload struct {
	Agent      string  `json:"agent"`
	Owner      string  `json:"owner"`
	InstanceID string  `json:"instance_id"`
	Session    string  `json:"session,omitempty"`
	TS         string  `json:"ts,omitempty"`
	IntervalS  float64 `json:"interval_s,omitempty"` // cadence in seconds (§8.3); liveness uses 3×

	// Transport metadata, filled by the handler (not decoded from the body), so the
	// JSON view can emit the whole frame — "emit everything". The subject in particular
	// carries the routing identity agents.hb.{agent}.{owner}.{name}, the only link
	// between a heartbeat and the agent's prompt/reply traffic (§2, §8.1).
	Subject string      `json:"-"`
	Raw     []byte      `json:"-"`
	Headers nats.Header `json:"-"`
}

type attachment struct {
	Filename string `json:"filename"`
	Content  string `json:"content"` // base64 per agent SDK protocol
}

// agentPromptBody mirrors the agent SDK request envelope. The body can also be
// plain text, in which case JSON decode fails and we fall back to raw bytes.
type agentPromptBody struct {
	Prompt      string       `json:"prompt"`
	Attachments []attachment `json:"attachments,omitempty"`
}

func extractPromptBody(data []byte) (string, []attachment) {
	var body agentPromptBody
	if err := json.Unmarshal(data, &body); err == nil && body.Prompt != "" {
		return body.Prompt, body.Attachments
	}
	return string(data), nil
}

// queryFrame is the mid-stream query chunk (§7): the agent pauses its reply to ask
// the caller a question, supplying a fresh reply_subject for the one-shot answer.
type queryFrame struct {
	Type string `json:"type"`
	Data struct {
		ID           string `json:"id"`
		ReplySubject string `json:"reply_subject"`
		Prompt       string `json:"prompt"`
	} `json:"data"`
}

// parseQueryFrame reports whether data is a well-formed query chunk and extracts it.
func parseQueryFrame(data []byte) (queryFrame, bool) {
	var qf queryFrame
	if err := json.Unmarshal(data, &qf); err != nil {
		return qf, false
	}
	return qf, qf.Type == "query" && qf.Data.ReplySubject != ""
}

// parsePromptSubject extracts (agent, owner, session) from
// "agents.prompt.{agent}.{owner}.{session}".
func parsePromptSubject(s string) (agent, owner, session string, ok bool) {
	if !strings.HasPrefix(s, subjPromptRoot) {
		return "", "", "", false
	}
	parts := strings.SplitN(s[len(subjPromptRoot):], ".", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", "", "", false
	}
	return parts[0], parts[1], parts[2], true
}

// --- view abstraction -------------------------------------------------------

// promptMeta carries the parsed prompt-subject tokens to views that want them
// (e.g. for avatars). parsed is false when the subject wasn't the 3-token form.
type promptMeta struct {
	agent, owner, session string
	parsed                bool
}

// view is the rendering surface. Both the line-oriented log printer and the
// scrolling protocol visualizer implement it. Methods are called from multiple
// goroutines (prompt sub, heartbeat sub, per-reply subs), so implementations
// must serialize their own output.
type view interface {
	banner()
	online(hb heartbeatPayload)
	heartbeat(hb heartbeatPayload)
	prompt(seq uint64, label string, pm promptMeta, m *nats.Msg, text string, atts []attachment)
	chunk(seq uint64, label string, n int, m *nats.Msg)
	sentinel(seq uint64, label string, m *nats.Msg)
	done(seq uint64, label string, chunks int, reason string)
	// queryReply surfaces the caller's answer to a mid-stream query (§7), captured by
	// following the query's fresh reply_subject. qid pairs it with the query chunk;
	// subject is the reply_subject the answer was published to.
	queryReply(seq uint64, label, qid, subject, prompt, answer string)
	warnf(format string, args ...any)
	// stop tears the view down. Line-oriented views own no terminal state and
	// no-op; the full-screen dash view restores the terminal and stops its loop.
	stop()
}

func newView(cfg config) view {
	switch cfg.view {
	case viewViz:
		return newVizPrinter(cfg.color, cfg.truncate, cfg.width)
	case viewDash:
		return newDashView(cfg.color)
	case viewJSON:
		return newJSONView()
	default:
		return &printer{color: cfg.color, truncate: cfg.truncate, showHB: cfg.showHB}
	}
}

// --- app + flow tracking ----------------------------------------------------

type app struct {
	nc   *nats.Conn
	out  view
	tr   *tracker
	seen *seenSet
	idle time.Duration
	seq  atomic.Uint64
}

// flow holds the live state of one observed prompt and its reply stream.
type flow struct {
	seq    uint64
	label  string
	chunks int
	sub    *nats.Subscription
	qsubs  []*nats.Subscription // mid-stream query reply subs (§7); torn down with the flow
	idle   *time.Timer
}

// tracker maps a reply inbox to its in-flight flow. Its mutex guards both the
// map and the flow fields, since reply callbacks, the idle timer, and shutdown
// all touch them concurrently.
type tracker struct {
	mu    sync.Mutex
	idle  time.Duration
	flows map[string]*flow
}

func newTracker(idle time.Duration) *tracker {
	return &tracker{idle: idle, flows: map[string]*flow{}}
}

func (t *tracker) add(inbox string, f *flow) {
	t.mu.Lock()
	t.flows[inbox] = f
	t.mu.Unlock()
}

func (t *tracker) setSub(inbox string, sub *nats.Subscription) {
	t.mu.Lock()
	if f, ok := t.flows[inbox]; ok {
		f.sub = sub
	}
	t.mu.Unlock()
}

// addQuerySub attaches a mid-stream query reply subscription (§7) to a flow so it
// is torn down when the flow finalizes. If the flow is already gone, the sub is
// unsubscribed immediately rather than leaked.
func (t *tracker) addQuerySub(inbox string, sub *nats.Subscription) {
	t.mu.Lock()
	if f, ok := t.flows[inbox]; ok {
		f.qsubs = append(f.qsubs, sub)
		t.mu.Unlock()
		return
	}
	t.mu.Unlock()
	_ = sub.Unsubscribe()
}

// peek returns a flow's seq/label without removing it.
func (t *tracker) peek(inbox string) (uint64, string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if f, ok := t.flows[inbox]; ok {
		return f.seq, f.label, true
	}
	return 0, "", false
}

// chunk records one reply chunk and resets the idle timer. Returns the flow's
// seq/label, the 1-based chunk number, and false if the flow is already gone.
func (t *tracker) chunk(inbox string) (uint64, string, int, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	f, ok := t.flows[inbox]
	if !ok {
		return 0, "", 0, false
	}
	f.chunks++
	if f.idle != nil {
		f.idle.Reset(t.idle)
	}
	return f.seq, f.label, f.chunks, true
}

// finalize claims a flow exactly once, stopping its timer and unsubscribing.
// Returns the flow and true the first time; (nil, false) on subsequent calls,
// which is what guarantees a single "done" line per prompt.
func (t *tracker) finalize(inbox string) (*flow, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	f, ok := t.flows[inbox]
	if !ok {
		return nil, false
	}
	delete(t.flows, inbox)
	if f.idle != nil {
		f.idle.Stop()
	}
	if f.sub != nil {
		_ = f.sub.Unsubscribe()
	}
	for _, qs := range f.qsubs {
		_ = qs.Unsubscribe()
	}
	return f, true
}

func (t *tracker) inboxes() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, 0, len(t.flows))
	for inbox := range t.flows {
		out = append(out, inbox)
	}
	return out
}

func (a *app) finish(inbox, reason string) {
	if f, ok := a.tr.finalize(inbox); ok {
		a.out.done(f.seq, f.label, f.chunks, reason)
	}
}

func (a *app) closeAll() {
	for _, inbox := range a.tr.inboxes() {
		a.finish(inbox, doneShutdown)
	}
}

// --- handlers ---------------------------------------------------------------

func (a *app) onHeartbeat(m *nats.Msg) {
	var hb heartbeatPayload
	if err := json.Unmarshal(m.Data, &hb); err != nil {
		a.out.warnf("heartbeat decode (%s): %v", m.Subject, err)
		return
	}
	if hb.InstanceID == "" {
		return
	}
	hb.Subject, hb.Raw, hb.Headers = m.Subject, m.Data, m.Header
	if a.seen.markSeen(hb.InstanceID) {
		a.out.online(hb)
	}
	a.out.heartbeat(hb)
}

func (a *app) onPrompt(m *nats.Msg) {
	seq := a.seq.Add(1)
	agent, owner, session, parsed := parsePromptSubject(m.Subject)
	label := subjectLabel(agent, owner, session, parsed, m.Subject)
	pm := promptMeta{agent: agent, owner: owner, session: session, parsed: parsed}

	// Follow the reply inbox before printing, to shrink the window in which an
	// eager agent streams chunks we never see. We still can't win the race
	// outright (subscription interest must propagate to the server), but there's
	// no synchronous work in front of the SUB.
	var subErr error
	if m.Reply != "" {
		f := &flow{seq: seq, label: label}
		f.idle = time.AfterFunc(a.idle, func() { a.finish(m.Reply, doneIdle) })
		a.tr.add(m.Reply, f)

		sub, err := a.nc.Subscribe(m.Reply, func(rm *nats.Msg) {
			a.onReply(m.Reply, rm)
		})
		if err != nil {
			subErr = err
		} else {
			a.tr.setSub(m.Reply, sub)
		}
	}

	prompt, atts := extractPromptBody(m.Data)
	a.out.prompt(seq, label, pm, m, prompt, atts)

	switch {
	case m.Reply == "":
		a.out.done(seq, label, 0, doneNoReply)
	case subErr != nil:
		a.out.warnf("prompt #%d: subscribe reply %s: %v", seq, m.Reply, subErr)
		a.finish(m.Reply, doneSubErr)
	}
}

func (a *app) onReply(inbox string, m *nats.Msg) {
	// Agent SDK end-of-stream terminator: empty body and no headers.
	if len(m.Data) == 0 && len(m.Header) == 0 {
		if seq, label, ok := a.tr.peek(inbox); ok {
			a.out.sentinel(seq, label, m)
		}
		a.finish(inbox, doneSentinel)
		return
	}
	seq, label, n, ok := a.tr.chunk(inbox)
	if !ok {
		return // already finalized (idle/shutdown); straggler chunk
	}
	a.out.chunk(seq, label, n, m)
	if qf, ok := parseQueryFrame(m.Data); ok {
		a.followQuery(inbox, seq, label, qf)
	}
}

// followQuery captures the caller's answer to a mid-stream query (§7). The agent
// supplied a fresh reply_subject in the query frame and the caller publishes one
// reply there; we subscribe (auto-unsubscribing after that single reply) and
// surface it. The sub is also torn down when the parent flow finalizes. The same
// subscribe-after-observe race as the main reply stream applies, but the answer
// follows caller deliberation, so it is far less likely to be missed.
func (a *app) followQuery(inbox string, seq uint64, label string, qf queryFrame) {
	rsub, qid, prompt := qf.Data.ReplySubject, qf.Data.ID, qf.Data.Prompt
	sub, err := a.nc.Subscribe(rsub, func(rm *nats.Msg) {
		answer, _ := extractPromptBody(rm.Data)
		a.out.queryReply(seq, label, qid, rsub, prompt, answer)
	})
	if err != nil {
		a.out.warnf("prompt #%d: subscribe query reply %s: %v", seq, rsub, err)
		return
	}
	_ = sub.AutoUnsubscribe(1) // the caller replies exactly once (§7.2)
	a.tr.addQuerySub(inbox, sub)
}

// --- seen set (for first-sight ONLINE lines) --------------------------------

type seenSet struct {
	mu sync.Mutex
	m  map[string]struct{}
}

func newSeenSet() *seenSet { return &seenSet{m: map[string]struct{}{}} }

// markSeen records id and reports whether this was the first time it was seen.
func (s *seenSet) markSeen(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.m[id]; ok {
		return false
	}
	s.m[id] = struct{}{}
	return true
}

// --- log view (line-oriented) -----------------------------------------------

const (
	cReset  = "\033[0m"
	cBold   = "\033[1m"
	cRed    = "\033[31m"
	cGreen  = "\033[32m"
	cYellow = "\033[33m"
	cCyan   = "\033[36m"
	cGray   = "\033[90m"
)

// printer renders events to stdout as compact colored lines. Its mutex
// serializes whole events so lines from concurrent handlers don't interleave.
type printer struct {
	mu       sync.Mutex
	color    bool
	truncate int
	showHB   bool
}

func (p *printer) banner() {}

func (p *printer) c(code, s string) string {
	if !p.color {
		return s
	}
	return code + s + cReset
}

func (p *printer) field(name, val string) string {
	return p.c(cGray, name+": ") + val
}

// emit writes one event: a timestamped header line plus indented detail lines.
func (p *printer) emit(tagColor, tag, head string, lines ...string) {
	ts := time.Now().Format("15:04:05.000")
	p.mu.Lock()
	defer p.mu.Unlock()
	fmt.Printf("%s  %s  %s\n", p.c(cGray, ts), p.c(tagColor, fmt.Sprintf("%-6s", tag)), head)
	for _, ln := range lines {
		fmt.Printf("        %s\n", ln)
	}
}

// textBlock renders a possibly multi-line body under a label, honoring -truncate.
func (p *printer) textBlock(label, s string) []string {
	s = sanitize(s)
	shown, extra := s, 0
	if p.truncate > 0 && len(s) > p.truncate {
		shown, extra = s[:p.truncate], len(s)-p.truncate
	}
	lines := strings.Split(strings.TrimRight(shown, "\n"), "\n")
	out := make([]string, 0, len(lines)+2)
	out = append(out, p.c(cGray, label+":"))
	for _, ln := range lines {
		out = append(out, "  "+ln)
	}
	if extra > 0 {
		out = append(out, p.c(cGray, fmt.Sprintf("  … +%d bytes", extra)))
	}
	return out
}

func (p *printer) prompt(seq uint64, label string, _ promptMeta, m *nats.Msg, text string, atts []attachment) {
	head := fmt.Sprintf("%s  %s", p.c(cBold, label), p.c(cGray, idTag(seq)))
	det := []string{p.field("subject", m.Subject)}
	if m.Reply != "" {
		det = append(det, p.field("reply", m.Reply))
	} else {
		det = append(det, p.field("reply", p.c(cYellow, "(none — fire-and-forget)")))
	}
	if h := headerString(m.Header); h != "" {
		det = append(det, p.field("headers", h))
	}
	if s := attachmentsString(atts); s != "" {
		det = append(det, p.field("attach", s))
	}
	det = append(det, p.textBlock("text", text)...)
	p.emit(cCyan, "PROMPT", head, det...)
}

func (p *printer) chunk(seq uint64, label string, n int, m *nats.Msg) {
	head := fmt.Sprintf("%s  %s  %s %s",
		p.c(cBold, label), p.c(cGray, idTag(seq)),
		p.c(cGreen, fmt.Sprintf("chunk %d", n)),
		p.c(cGray, "("+humanBytes(len(m.Data))+")"))
	var det []string
	if h := headerString(m.Header); h != "" {
		det = append(det, p.field("headers", h))
	}
	det = append(det, p.textBlock("data", string(m.Data))...)
	p.emit(cGreen, "REPLY", head, det...)
}

// sentinel is a no-op in the log view; the done line already carries reason=sentinel.
func (p *printer) sentinel(uint64, string, *nats.Msg) {}

func (p *printer) done(seq uint64, label string, chunks int, reason string) {
	head := fmt.Sprintf("%s  %s  %s",
		p.c(cBold, label), p.c(cGray, idTag(seq)),
		p.c(cGray, fmt.Sprintf("chunks=%d reason=%s", chunks, reason)))
	p.emit(cGray, "DONE", head)
}

func (p *printer) queryReply(seq uint64, label, qid, subject, prompt, answer string) {
	head := fmt.Sprintf("%s  %s", p.c(cBold, label), p.c(cGray, idTag(seq)))
	det := []string{p.field("reply", subject)}
	det = append(det, p.textBlock("ask", prompt)...)
	det = append(det, p.textBlock("answer", answer)...)
	p.emit(cYellow, "QUERY", head, det...)
}

func (p *printer) online(hb heartbeatPayload) {
	p.emit(cYellow, "ONLINE", fmt.Sprintf("%s  %s",
		p.c(cBold, agentLabel(hb)), p.c(cGray, "instance="+hb.InstanceID)))
}

func (p *printer) heartbeat(hb heartbeatPayload) {
	if !p.showHB {
		return
	}
	p.emit(cGray, "HB", fmt.Sprintf("%s  %s",
		agentLabel(hb), p.c(cGray, "instance="+hb.InstanceID)))
}

func (p *printer) warnf(format string, args ...any) {
	p.emit(cRed, "WARN", fmt.Sprintf(format, args...))
}

// stop is a no-op: the log view scrolls normally and owns no screen state.
func (p *printer) stop() {}

// --- shared formatting helpers ----------------------------------------------

func idTag(seq uint64) string { return fmt.Sprintf("#%04d", seq) }

func subjectLabel(agent, owner, session string, parsed bool, subject string) string {
	if !parsed {
		return subject
	}
	return fmt.Sprintf("%s/%s (%s)", agent, owner, session)
}

func agentLabel(hb heartbeatPayload) string {
	if hb.Session != "" {
		return fmt.Sprintf("%s/%s (%s)", hb.Agent, hb.Owner, hb.Session)
	}
	return fmt.Sprintf("%s/%s", hb.Agent, hb.Owner)
}

func headerString(h nats.Header) string {
	if len(h) == 0 {
		return ""
	}
	keys := make([]string, 0, len(h))
	for k := range h {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+strings.Join(h[k], ","))
	}
	return strings.Join(parts, "; ")
}

func attachmentsString(atts []attachment) string {
	if len(atts) == 0 {
		return ""
	}
	parts := make([]string, 0, len(atts))
	for _, a := range atts {
		parts = append(parts, attOne(a))
	}
	return fmt.Sprintf("%d — %s", len(atts), strings.Join(parts, ", "))
}

// attSize returns an attachment's real (decoded) size in bytes, falling back to
// the raw content length when the content isn't valid base64.
func attSize(a attachment) int {
	if raw, err := base64.StdEncoding.DecodeString(a.Content); err == nil {
		return len(raw)
	}
	return len(a.Content)
}

func attOne(a attachment) string {
	name := a.Filename
	if name == "" {
		name = "(unnamed)"
	}
	return fmt.Sprintf("%s [%s]", name, humanBytes(attSize(a)))
}

func humanBytes(n int) string {
	if n < 1024 {
		return fmt.Sprintf("%d B", n)
	}
	const units = "KMGT"
	f, i := float64(n), -1
	for f >= 1024 && i < len(units)-1 {
		f /= 1024
		i++
	}
	return fmt.Sprintf("%.1f %ciB", f, units[i])
}
