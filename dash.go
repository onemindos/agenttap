package main

// dash.go implements the "dash" view: a full-screen, redraw-in-place protocol
// analyzer. Unlike the scrolling log/viz views, dash owns the whole terminal
// (alt-screen) and repaints at ~10 Hz. The screen is three panels:
//
//	AGENTS   a left roster of every discovered agent, each with a heart whose
//	         color fades with heartbeat staleness (and pulses on each beat), a
//	         mute checkbox, and a selection cursor.
//	DETAIL   the top-right panel: a richer view of the *selected* agent — its
//	         liveness, instance, reply inbox, and current/last exchange (prompt
//	         plus assembled-so-far reply and frame/byte counts).
//	TRAFFIC  the bottom-right panel: the star of the view — a dense, one-line-
//	         per-event live tail of ALL wire traffic (heartbeats, prompts,
//	         response/control frames, the end sentinel, and the done summary).
//
// Keys: up/down (or k/j) select an agent, space mutes it (hides its traffic),
// s solos the selection, a unmutes all, h toggles heartbeat lines, q/Ctrl-C
// quits. Everything is ASCII-forward except the heart glyph; color does the
// work and the whole thing degrades to plain glyphs when color is disabled.
//
// Concurrency contract: events arrive from several goroutines (the prompt sub,
// the heartbeat sub, per-reply subs, the demo). Every view method takes d.mu,
// mutates state, and returns fast — it NEVER writes to the screen. The render
// goroutine is the only writer. This is what keeps the redraw coherent.

import (
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/nats-io/nats.go"
)

// --- palette + glyphs -------------------------------------------------------

var (
	colHeartLive  = rgb(120, 215, 140) // fresh: beating within ~1.2x interval
	colHeartPulse = rgb(175, 255, 200) // the ~300ms flash right after a beat
	colHeartWarn  = rgb(228, 196, 92)  // going stale: ~1.2x .. 2.5x interval
	colHeartStale = rgb(120, 126, 142) // stale: > 2.5x interval (or no hb yet)
	colHeartDead  = rgb(80, 84, 98)    // offline: > 4x interval (dropped)
	colSel        = rgb(245, 247, 255) // selected roster row label
	colMute       = rgb(96, 102, 120)  // muted roster row label
)

// heartGlyph is the only sanctioned non-ASCII glyph in the project: a single
// width-1 BMP rune, widely supported. Color carries its state.
const heartGlyph = "♥" // ♥

// Terminal control sequences for the alt-screen TUI.
const (
	altEnter = "\033[?1049h" // enter alternate screen buffer
	altLeave = "\033[?1049l" // leave it, restoring the user's scrollback
	curHide  = "\033[?25l"
	curShow  = "\033[?25h"
	clearAll = "\033[2J"
	clearEOL = "\033[K" // clear from cursor to end of line
)

// --- heart staleness --------------------------------------------------------

type heartStageT int

const (
	stageDead  heartStageT = iota // > 4x interval — drop from roster (offline)
	stageStale                    // > 2.5x interval
	stageWarn                     // 1.2x .. 2.5x interval
	stageLive                     // < 1.2x interval
)

// heartStage maps an agent's age-since-last-beat (relative to its estimated
// beat interval) to a liveness stage. Pure, so it can be unit-tested.
func heartStage(age, interval time.Duration) heartStageT {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	switch r := float64(age) / float64(interval); {
	case r > 4.0:
		return stageDead
	case r > 2.5:
		return stageStale
	case r > 1.2:
		return stageWarn
	default:
		return stageLive
	}
}

func heartColor(st heartStageT, pulse bool) string {
	switch st {
	case stageLive:
		if pulse {
			return colHeartPulse
		}
		return colHeartLive
	case stageWarn:
		return colHeartWarn
	case stageStale:
		return colHeartStale
	default:
		return colHeartDead
	}
}

// --- state model ------------------------------------------------------------

// agentStat is one row in the roster, keyed by agent identity (see agentKeyOf).
type agentStat struct {
	key      string
	agent    string
	owner    string
	session  string
	instance string              // most recent instance_id seen via heartbeat
	instSet  map[string]struct{} // distinct instance_ids (for "+N" in detail)
	lastSeen time.Time           // time of the last heartbeat
	interval time.Duration       // EWMA estimate of the beat interval (default 5s)
	beats    int
	seenHB   bool // have we seen a heartbeat (vs. discovered via a prompt)?
}

// exchange wraps a convo (reused from viz.go for its reply-frame accumulation)
// with the few extra fields the dash detail panel needs.
type exchange struct {
	c             *convo
	key           string
	promptSubject string // the originating prompt subject (agents.prompt.…)
	replyInbox    string
	lastAt        time.Time
	done          bool
	reason        string
}

// feedKind classifies a feed line. Only fkHB is special (the h toggle hides
// it); the rest are informational and drive the type-tag color.
type feedKind uint8

const (
	fkOther feedKind = iota
	fkOnline
	fkHB
	fkPrompt
	fkChunk
	fkCtrl
	fkEnd
	fkDone
	fkWarn
)

// maxPayloadStore caps how many runes of each event's payload we retain for the
// packet-detail view. The live list only ever shows a width-clipped snippet, so
// this bounds memory while still letting the detail pane show "all" of a packet.
const maxPayloadStore = 2048

// feedLine is one captured event. The first block feeds the one-line traffic
// tail; the second block is the full packet detail shown when the feed is
// focused (Wireshark-style "click a packet"). Text is sanitized at capture.
type feedLine struct {
	id     uint64 // monotonic event id, stable across ring wrap
	t      time.Time
	kind   feedKind
	key    string // agent identity for mute/solo filtering ("" = always shown)
	typ    string // short type tag, e.g. "PROMPT", "resp 7", "ack", "hb"
	typCol string // color of the type tag
	flow   string // "from -> to" (list line)
	detail string // payload snippet / stats (list line)

	// Full packet detail (DETAIL pane, feed focus). Metadata is kept in full;
	// only the payload is capped (to maxPayloadStore).
	seq         uint64        // owning prompt seq (0 if none)
	subject     string        // originating prompt subject (agents.prompt.…)
	reply       string        // full reply inbox
	headers     string        // formatted header string
	atts        string        // attachments summary (prompts)
	instance    string        // full instance_id (the "agent id")
	ts          string        // heartbeat timestamp (hb / online)
	frameType   string        // reply frame type (response/status/tool_call/…)
	chunkN      int           // reply frame index
	bytesN      int           // wire byte size of this message
	payload     string        // sanitized payload, capped to maxPayloadStore runes
	payloadFull bool          // payload holds the entire (sanitized) text
	sinceStart  time.Duration // reply: time since the exchange started
}

// feedRing is a fixed-capacity ring buffer of events (live tail; oldest entries
// are overwritten). Each entry gets a monotonic id so a selection can survive
// the ring wrapping. Fixed memory, no per-push reallocation.
type feedRing struct {
	buf    []feedLine
	head   int // index of the oldest entry
	count  int
	pushed uint64 // total ever pushed; also the next id
}

func newFeedRing(capacity int) *feedRing { return &feedRing{buf: make([]feedLine, capacity)} }

// push appends l (assigning it the next monotonic id) and returns that id.
func (r *feedRing) push(l feedLine) uint64 {
	l.id = r.pushed
	if r.count < len(r.buf) {
		r.buf[(r.head+r.count)%len(r.buf)] = l
		r.count++
	} else {
		r.buf[r.head] = l
		r.head = (r.head + 1) % len(r.buf)
	}
	r.pushed++
	return l.id
}

// at returns the i-th oldest entry (0 .. count-1).
func (r *feedRing) at(i int) feedLine { return r.buf[(r.head+i)%len(r.buf)] }
func (r *feedRing) len() int          { return r.count }

// oldestID / newestID bound the ids currently retained (valid when len() > 0).
func (r *feedRing) oldestID() uint64 { return r.pushed - uint64(r.count) }
func (r *feedRing) newestID() uint64 { return r.pushed - 1 }

// byID returns the entry with the given id, or false if it has been evicted.
func (r *feedRing) byID(id uint64) (feedLine, bool) {
	if r.count == 0 || id < r.oldestID() || id > r.newestID() {
		return feedLine{}, false
	}
	return r.at(int(id - r.oldestID())), true
}

// focusT is which pane keyboard navigation drives. left/right switch between
// them; the DETAIL pane is a readout that follows whichever is focused.
type focusT int

const (
	focusRoster focusT = iota // up/down pick an agent; DETAIL shows its conversation; feed live-tails
	focusFeed                 // up/down move the packet cursor; DETAIL shows that packet
)

// dashView is the full-screen analyzer. See the file header for the contract.
type dashView struct {
	pt paint

	mu        sync.Mutex
	width     int
	height    int
	agents    map[string]*agentStat
	bySeq     map[uint64]string // prompt seq -> agent key (attribute reply events)
	exch      map[string]*exchange
	feed      *feedRing
	muted     map[string]bool
	selected  string // selected agent key (DETAIL panel follows it in roster focus)
	showHB    bool   // include heartbeat lines in the traffic tail
	clearNext bool   // force a full clear on the next frame (after a resize)

	// Focus + the feed's packet cursor (Wireshark-style packet list/detail).
	focus      focusT // which pane up/down drives
	followLive bool   // feed cursor tails the newest event (auto-follow)
	cursorID   uint64 // selected event id when frozen (followLive == false)

	// lifecycle (touched only by the main goroutine via banner()/stop(), plus
	// the signal/render/key goroutines as noted)
	out        *os.File
	in         *os.File
	oldTermios *syscall.Termios
	rawActive  bool
	altActive  bool
	sigCh      chan os.Signal
	stopCh     chan struct{}
	quitCh     chan struct{}
	renderDone chan struct{}
	stopOnce   sync.Once
	quitOnce   sync.Once
}

func newDashView(color bool) *dashView {
	rows, cols := terminalSize()
	if cols <= 0 {
		cols = 100
	}
	if rows <= 0 {
		rows = 30
	}
	return &dashView{
		pt:         paint{on: color},
		width:      cols,
		height:     rows,
		agents:     map[string]*agentStat{},
		bySeq:      map[uint64]string{},
		exch:       map[string]*exchange{},
		feed:       newFeedRing(4096),
		muted:      map[string]bool{},
		showHB:     true, // analyzer: show heartbeats by default; toggle with 'h'
		followLive: true, // feed tails the newest event until you scroll up
		focus:      focusRoster,
		out:        os.Stdout,
		in:         os.Stdin,
		stopCh:     make(chan struct{}),
		quitCh:     make(chan struct{}),
		renderDone: make(chan struct{}),
	}
}

// agentKeyOf is the roster identity for an agent. It MUST match agentLabel so
// that heartbeats and prompts for the same agent land on the same row.
func agentKeyOf(agent, owner, session string) string {
	if session != "" {
		return fmt.Sprintf("%s/%s (%s)", agent, owner, session)
	}
	return fmt.Sprintf("%s/%s", agent, owner)
}

func (d *dashView) agentLocked(agent, owner, session string) *agentStat {
	return d.agentByKeyLocked(agentKeyOf(agent, owner, session), agent, owner, session)
}

func (d *dashView) agentByKeyLocked(key, agent, owner, session string) *agentStat {
	a := d.agents[key]
	if a == nil {
		now := time.Now()
		a = &agentStat{key: key, agent: agent, owner: owner, session: session,
			lastSeen: now, interval: 5 * time.Second}
		d.agents[key] = a
	}
	return a
}

// --- lifecycle (terminal control, loops) ------------------------------------

// banner doubles as the dash start hook: enter alt-screen, raw mode, install
// signal handling, and launch the render and keyboard goroutines.
func (d *dashView) banner() { d.start() }

func (d *dashView) start() {
	if rows, cols := terminalSize(); rows > 0 && cols > 0 {
		d.mu.Lock()
		d.height, d.width = rows, cols
		d.mu.Unlock()
	}
	// Raw mode is best-effort: dash is TTY-guarded in main, but if termios
	// twiddling fails we still render — only key input would be degraded.
	_ = d.enterRaw()
	fmt.Fprint(d.out, altEnter+curHide+clearAll)
	d.altActive = true

	d.sigCh = make(chan os.Signal, 8)
	signal.Notify(d.sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGWINCH)
	go d.signalLoop()
	go d.renderLoop()
	go d.keyLoop()
}

// stop tears the TUI down: halt the render loop, restore the terminal, and
// stop signal delivery. Idempotent and safe to call from defer.
func (d *dashView) stop() {
	d.stopOnce.Do(func() {
		close(d.stopCh)
		// Let the render goroutine finish its current frame so the restore
		// sequence can't land mid-draw. Bounded so we never hang on exit.
		if d.altActive {
			select {
			case <-d.renderDone:
			case <-time.After(250 * time.Millisecond):
			}
		}
		if d.sigCh != nil {
			signal.Stop(d.sigCh)
			close(d.sigCh)
		}
		if d.altActive {
			fmt.Fprint(d.out, curShow+altLeave)
			d.altActive = false
		}
		d.restoreTermios()
	})
}

func (d *dashView) triggerQuit()          { d.quitOnce.Do(func() { close(d.quitCh) }) }
func (d *dashView) quit() <-chan struct{} { return d.quitCh }

func (d *dashView) signalLoop() {
	for sig := range d.sigCh {
		switch sig {
		case syscall.SIGWINCH:
			rows, cols := terminalSize()
			d.mu.Lock()
			if rows > 0 && cols > 0 {
				d.height, d.width = rows, cols
			}
			d.clearNext = true
			d.mu.Unlock()
		case syscall.SIGINT, syscall.SIGTERM:
			d.triggerQuit()
			return
		}
	}
}

func (d *dashView) renderLoop() {
	defer close(d.renderDone)
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-d.stopCh:
			return
		case <-t.C:
			d.render()
		}
	}
}

func (d *dashView) keyLoop() {
	buf := make([]byte, 16)
	for {
		n, err := d.in.Read(buf)
		if err != nil || n == 0 {
			d.triggerQuit() // stdin closed -> treat as quit
			return
		}
		if d.handleKeys(buf[:n]) {
			d.triggerQuit()
			return
		}
	}
}

// handleKeys decodes a read buffer (which may hold several bytes, e.g. a 3-byte
// arrow sequence) and applies actions. Returns true if the user asked to quit.
//
// left/right switch the focused pane; up/down then either pick an agent (roster
// focus) or scrub the packet cursor (feed focus). space/s act on the focused
// thing; a and h are global.
func (d *dashView) handleKeys(b []byte) (quit bool) {
	for i := 0; i < len(b); i++ {
		switch c := b[i]; c {
		case 'q', 'Q', 0x03, 0x04: // q, Ctrl-C, Ctrl-D
			return true
		case 'k':
			d.move(-1)
		case 'j':
			d.move(1)
		case ' ':
			d.muteFocused()
		case 's', 'S':
			d.soloFocused()
		case 'a', 'A':
			d.unmuteAll()
		case 'h', 'H':
			d.toggleHB()
		case 'g':
			d.moveEdge(true)
		case 'G':
			d.moveEdge(false)
		case 0x1b: // ESC: decode an arrow sequence (ESC [ A/B/C/D); ignore lone ESC
			if i+2 < len(b) && b[i+1] == '[' {
				switch b[i+2] {
				case 'A':
					d.move(-1)
				case 'B':
					d.move(1)
				case 'C':
					d.setFocus(focusFeed)
				case 'D':
					d.setFocus(focusRoster)
				}
				i += 2
			}
		}
	}
	return false
}

// --- termios (raw / cbreak mode) --------------------------------------------
//
// We keep raw I/O minimal and consistent with viz.go's terminalSize: a direct
// ioctl, no extra dependency. We deliberately KEEP ISIG enabled so Ctrl-C still
// raises SIGINT (handled in signalLoop) — the terminal is always restored.

func getTermios(fd uintptr) (*syscall.Termios, error) {
	var t syscall.Termios
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, uintptr(ioctlGetTermios), uintptr(unsafe.Pointer(&t)))
	if errno != 0 {
		return nil, errno
	}
	return &t, nil
}

func setTermios(fd uintptr, t *syscall.Termios) error {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, fd, uintptr(ioctlSetTermios), uintptr(unsafe.Pointer(t)))
	if errno != 0 {
		return errno
	}
	return nil
}

func (d *dashView) enterRaw() error {
	fd := d.in.Fd()
	old, err := getTermios(fd)
	if err != nil {
		return err
	}
	d.oldTermios = old
	raw := *old
	// cbreak: byte-at-a-time, no echo, no SW flow control / CR translation —
	// but keep ISIG so Ctrl-C still signals.
	raw.Lflag &^= syscall.ICANON | syscall.ECHO
	raw.Iflag &^= syscall.IXON | syscall.ICRNL
	raw.Cc[syscall.VMIN] = 1
	raw.Cc[syscall.VTIME] = 0
	if err := setTermios(fd, &raw); err != nil {
		return err
	}
	d.rawActive = true
	return nil
}

func (d *dashView) restoreTermios() {
	if d.rawActive && d.oldTermios != nil {
		_ = setTermios(d.in.Fd(), d.oldTermios)
		d.rawActive = false
	}
}

// --- interaction (state mutations) ------------------------------------------

// setFocus switches the focused pane. Entering feed focus starts on the newest
// packet, tailing live.
func (d *dashView) setFocus(f focusT) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.focus == f {
		return
	}
	d.focus = f
	if f == focusFeed {
		d.followLive = true
		if ids := d.visibleIDsLocked(); len(ids) > 0 {
			d.cursorID = ids[len(ids)-1]
		}
	}
}

// move is up/down: it scrubs the packet cursor in feed focus, else moves the
// roster selection.
func (d *dashView) move(delta int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.focus == focusFeed {
		d.feedMoveLocked(delta)
	} else {
		d.moveSelLocked(delta)
	}
}

// moveEdge jumps to the first/last item of the focused pane. In feed focus,
// "last" resumes live tailing.
func (d *dashView) moveEdge(top bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.focus == focusFeed {
		ids := d.visibleIDsLocked()
		if len(ids) == 0 {
			return
		}
		if top {
			d.cursorID, d.followLive = ids[0], false
		} else {
			d.cursorID, d.followLive = ids[len(ids)-1], true
		}
		return
	}
	keys := d.rosterKeysLocked()
	if len(keys) == 0 {
		return
	}
	if top {
		d.selected = keys[0]
	} else {
		d.selected = keys[len(keys)-1]
	}
}

func (d *dashView) moveSelLocked(delta int) {
	keys := d.rosterKeysLocked()
	if len(keys) == 0 {
		return
	}
	idx := indexOf(keys, d.selected)
	if idx < 0 {
		idx = 0
	} else {
		idx += delta
	}
	d.selected = keys[clamp(idx, 0, len(keys)-1)]
}

// feedMoveLocked moves the packet cursor over the VISIBLE events. Moving up off
// the newest freezes the live tail; returning to the newest resumes it.
func (d *dashView) feedMoveLocked(delta int) {
	ids := d.visibleIDsLocked()
	if len(ids) == 0 {
		return
	}
	cur := d.cursorID
	if d.followLive {
		cur = ids[len(ids)-1]
	}
	idx := indexOfU64(ids, cur)
	if idx < 0 {
		idx = len(ids) - 1 // selection hidden/evicted -> snap to newest visible
	}
	idx = clamp(idx+delta, 0, len(ids)-1)
	d.cursorID = ids[idx]
	d.followLive = idx == len(ids)-1
}

// effectiveCursorLocked is the id of the packet the feed cursor points at,
// resolving live-follow and a hidden/evicted selection to the newest visible.
func (d *dashView) effectiveCursorLocked() (uint64, bool) {
	ids := d.visibleIDsLocked()
	if len(ids) == 0 {
		return 0, false
	}
	if d.followLive || indexOfU64(ids, d.cursorID) < 0 {
		return ids[len(ids)-1], true
	}
	return d.cursorID, true
}

// focusedKeyLocked is the agent key the current focus is "on": the selected
// roster agent, or (feed focus) the agent owning the selected packet.
func (d *dashView) focusedKeyLocked() string {
	if d.focus == focusFeed {
		if id, ok := d.effectiveCursorLocked(); ok {
			if fl, ok2 := d.feed.byID(id); ok2 {
				return fl.key
			}
		}
		return ""
	}
	return d.selected
}

func (d *dashView) muteFocused() {
	d.mu.Lock()
	defer d.mu.Unlock()
	k := d.focusedKeyLocked()
	if k == "" {
		return
	}
	if d.muted[k] {
		delete(d.muted, k)
	} else {
		d.muted[k] = true
	}
}

func (d *dashView) soloFocused() {
	d.mu.Lock()
	defer d.mu.Unlock()
	k := d.focusedKeyLocked()
	if k == "" {
		return
	}
	for m := range d.muted {
		delete(d.muted, m)
	}
	for _, rk := range d.rosterKeysLocked() {
		if rk != k {
			d.muted[rk] = true
		}
	}
}

// visibleLocked reports whether an event passes the current filters (heartbeat
// toggle + per-agent mute).
func (d *dashView) visibleLocked(fl feedLine) bool {
	if fl.kind == fkHB && !d.showHB {
		return false
	}
	if fl.key != "" && d.muted[fl.key] {
		return false
	}
	return true
}

// visibleIDsLocked returns the ids of all currently-visible events, oldest first.
func (d *dashView) visibleIDsLocked() []uint64 {
	ids := make([]uint64, 0, d.feed.len())
	for i := 0; i < d.feed.len(); i++ {
		if fl := d.feed.at(i); d.visibleLocked(fl) {
			ids = append(ids, fl.id)
		}
	}
	return ids
}

func (d *dashView) unmuteAll() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for k := range d.muted {
		delete(d.muted, k)
	}
}

func (d *dashView) toggleHB() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.showHB = !d.showHB
}

// rosterKeysLocked returns the visible roster keys, sorted. Heartbeat-known
// agents that have gone fully stale (> 4x interval) are dropped as offline;
// prompt-only agents are always shown.
func (d *dashView) rosterKeysLocked() []string {
	now := time.Now()
	keys := make([]string, 0, len(d.agents))
	for k, a := range d.agents {
		if a.seenHB && heartStage(now.Sub(a.lastSeen), a.interval) == stageDead {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// --- view methods (state-only; never print) ---------------------------------

func (d *dashView) online(hb heartbeatPayload) {
	d.mu.Lock()
	defer d.mu.Unlock()
	a := d.agentLocked(hb.Agent, hb.Owner, hb.Session)
	d.markInstance(a, hb.InstanceID)
	a.seenHB = true
	d.pushFeedLocked(feedLine{
		t: time.Now(), kind: fkOnline, key: a.key,
		typ: "ONLINE", typCol: colOnline,
		flow: agentLabel(hb), detail: "inst=" + shortID(hb.InstanceID),
		instance: hb.InstanceID, ts: hb.TS,
	})
}

func (d *dashView) heartbeat(hb heartbeatPayload) {
	now := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	a := d.agentLocked(hb.Agent, hb.Owner, hb.Session)
	d.markInstance(a, hb.InstanceID)
	// EWMA interval estimate from the inter-beat delta, ignoring the first beat
	// and out-of-range gaps (our own startup, or a long stall).
	if a.seenHB {
		if delta := now.Sub(a.lastSeen); delta >= 250*time.Millisecond && delta <= 60*time.Second {
			a.interval = (a.interval*7 + delta*3) / 10
		}
	}
	a.seenHB = true
	a.lastSeen = now
	a.beats++
	if d.selected == "" {
		d.selected = a.key
	}
	d.pushFeedLocked(feedLine{
		t: now, kind: fkHB, key: a.key,
		typ: "hb", typCol: colHb,
		flow: agentLabel(hb), detail: "inst=" + shortID(hb.InstanceID),
		instance: hb.InstanceID, ts: hb.TS,
	})
}

func (d *dashView) markInstance(a *agentStat, inst string) {
	if inst == "" {
		return
	}
	a.instance = inst
	if a.instSet == nil {
		a.instSet = map[string]struct{}{}
	}
	a.instSet[inst] = struct{}{}
}

func (d *dashView) prompt(seq uint64, label string, pm promptMeta, m *nats.Msg, text string, atts []attachment) {
	now := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()

	var key string
	if pm.parsed {
		key = d.agentLocked(pm.agent, pm.owner, pm.session).key
	} else {
		key = label // unparsed subject: keep it as its own roster row
		d.agentByKeyLocked(key, "", "", "")
	}
	d.bySeq[seq] = key
	if d.selected == "" {
		d.selected = key
	}

	d.exch[key] = &exchange{
		c: &convo{
			seq: seq, label: label,
			agent: pm.agent, owner: pm.owner, session: pm.session, parsed: pm.parsed,
			promptText: text, atts: atts, started: now,
		},
		key: key, promptSubject: m.Subject, replyInbox: m.Reply, lastAt: now,
	}

	flow := label
	if pm.parsed {
		flow = promptFlow(pm)
	}
	detail := snippet(text)
	if m.Reply == "" {
		detail = "(fire-and-forget) " + detail
	}
	payload, full := capPayload(text)
	d.pushFeedLocked(feedLine{
		t: now, kind: fkPrompt, key: key,
		typ: "PROMPT", typCol: colPrompt, flow: flow, detail: detail,
		seq: seq, subject: m.Subject, reply: m.Reply, instance: d.instanceLocked(key),
		headers: headerString(m.Header), atts: attachmentsString(atts),
		bytesN: len(text), payload: payload, payloadFull: full,
	})
}

func (d *dashView) chunk(seq uint64, label string, n int, m *nats.Msg) {
	now := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	key := d.bySeq[seq]
	var since time.Duration
	if ex := d.exch[key]; ex != nil {
		ex.c.chunks = n
		ex.c.bytesN += len(m.Data)
		ex.c.accumulate(m.Data)
		ex.lastAt = now
		since = now.Sub(ex.c.started)
	}
	typ, typCol, kind, detail := classifyChunk(m.Data, n)
	frameType, payloadText := framePayload(m.Data)
	payload, full := capPayload(payloadText)
	d.pushFeedLocked(feedLine{
		t: now, kind: kind, key: key,
		typ: typ, typCol: typCol,
		flow: d.flowToInboxLocked(key, m.Subject), detail: detail,
		seq: seq, subject: d.promptSubjectLocked(key), reply: m.Subject,
		instance: d.instanceLocked(key), headers: headerString(m.Header),
		frameType: frameType, chunkN: n, bytesN: len(m.Data),
		payload: payload, payloadFull: full, sinceStart: since,
	})
}

func (d *dashView) sentinel(seq uint64, label string, m *nats.Msg) {
	now := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	key := d.bySeq[seq]
	d.pushFeedLocked(feedLine{
		t: now, kind: fkEnd, key: key,
		typ: "END", typCol: colEnd,
		flow: d.flowToInboxLocked(key, m.Subject), detail: "0 B stream terminator",
		seq: seq, subject: d.promptSubjectLocked(key), reply: m.Subject,
		instance: d.instanceLocked(key),
	})
}

func (d *dashView) queryReply(seq uint64, label, qid, subject, prompt, answer string) {
	now := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	key := d.bySeq[seq]
	p, full := capPayload(sanitize(answer))
	d.pushFeedLocked(feedLine{
		t: now, kind: fkCtrl, key: key,
		typ: "Q->A", typCol: colHeartWarn,
		flow:   d.flowToInboxLocked(key, subject),
		detail: "Q: " + sanitize(prompt) + "  ->  " + sanitize(answer),
		seq:    seq, subject: d.promptSubjectLocked(key), reply: subject, instance: d.instanceLocked(key),
		frameType: "query-reply", payload: p, payloadFull: full,
	})
}

func (d *dashView) done(seq uint64, label string, chunks int, reason string) {
	now := time.Now()
	d.mu.Lock()
	defer d.mu.Unlock()
	key := d.bySeq[seq]
	delete(d.bySeq, seq)

	flow := label
	detail := fmt.Sprintf("%d chunks  %s", chunks, reason)
	inbox, bytesN := "", 0
	if ex := d.exch[key]; ex != nil {
		ex.done = true
		ex.reason = reason
		ex.lastAt = now
		c := ex.c
		if c.parsed {
			flow = agentTag(c, label)
		}
		frames := fmt.Sprintf("%d fr", chunks)
		if c.ctrlFrames > 0 {
			frames = fmt.Sprintf("%d fr (%d txt/%d ctrl)", chunks, c.textFrames, c.ctrlFrames)
		}
		detail = fmt.Sprintf("%s  %s  %s  %s", frames, humanBytes(c.bytesN), humanDur(now.Sub(c.started)), reason)
		inbox, bytesN = ex.replyInbox, c.bytesN
	}
	col := colOnline
	if reason != doneSentinel {
		col = colEnd
	}
	d.pushFeedLocked(feedLine{
		t: now, kind: fkDone, key: key,
		typ: "DONE", typCol: col, flow: flow, detail: detail,
		seq: seq, subject: d.promptSubjectLocked(key), reply: inbox,
		instance: d.instanceLocked(key), bytesN: bytesN,
	})
}

func (d *dashView) warnf(format string, args ...any) {
	d.mu.Lock()
	defer d.mu.Unlock()
	msg := fmt.Sprintf(format, args...)
	payload, full := capPayload(msg)
	d.pushFeedLocked(feedLine{
		t: time.Now(), kind: fkWarn, key: "",
		typ: "WARN", typCol: colWarn, detail: clipRunes(sanitize(msg), 240),
		payload: payload, payloadFull: full, bytesN: len(msg),
	})
}

func (d *dashView) pushFeedLocked(l feedLine) { d.feed.push(l) }

// flowToInboxLocked formats "agent -> <short inbox>" for a reply-stream event.
func (d *dashView) flowToInboxLocked(key, inbox string) string {
	name := key
	if ex := d.exch[key]; ex != nil && ex.c != nil && ex.c.agent != "" {
		name = ex.c.agent
	} else if a := d.agents[key]; a != nil && a.agent != "" {
		name = a.agent
	}
	return name + " -> " + shortInbox(inbox)
}

// instanceLocked is the most-recent instance_id seen for an agent key (its
// "agent id"), or "" if none has heartbeated yet.
func (d *dashView) instanceLocked(key string) string {
	if a := d.agents[key]; a != nil {
		return a.instance
	}
	return ""
}

// promptSubjectLocked is the originating prompt subject for an agent key's
// current exchange, or "" if unknown.
func (d *dashView) promptSubjectLocked(key string) string {
	if ex := d.exch[key]; ex != nil {
		return ex.promptSubject
	}
	return ""
}

// classifyChunk turns a reply-stream message into a type tag, color, kind, and
// a short detail snippet. Response/raw frames show their text; control frames
// (status/ack/tool_call/…) show their type and a hint.
func classifyChunk(data []byte, n int) (typ, typCol string, kind feedKind, detail string) {
	if t, text, isStr, ok := parseFrame(data); ok {
		switch {
		case t == "response" && isStr:
			return fmt.Sprintf("resp %d", n), colChunk, fkChunk, snippet(text)
		case t == "error":
			return "error", colWarn, fkCtrl, snippet(text)
		case isStr:
			return shortType(t), colMeta, fkCtrl, t + "=" + clipRunes(sanitize(firstLine(text)), 80)
		default:
			return shortType(t), colMeta, fkCtrl, t + " " + clipRunes(sanitize(string(data)), 120)
		}
	}
	return fmt.Sprintf("data %d", n), colChunk, fkChunk, snippet(string(data))
}

// framePayload returns a reply frame's type and the payload to keep for the
// packet-detail view: the decoded string for string-data frames, otherwise the
// raw bytes (e.g. a tool_call's JSON object). Empty type means it wasn't a
// typed frame, so the whole message is the payload.
func framePayload(data []byte) (frameType, payload string) {
	if t, s, isStr, ok := parseFrame(data); ok {
		if isStr {
			return t, s
		}
		return t, string(data)
	}
	return "", string(data)
}

func shortType(t string) string {
	switch t {
	case "tool_call":
		return "tool"
	case "tool_result":
		return "toolres"
	case "metadata":
		return "meta"
	default:
		return t
	}
}

// --- rendering --------------------------------------------------------------

func (d *dashView) render() {
	d.mu.Lock()
	W, H := d.width, d.height
	clear := d.clearNext
	d.clearNext = false
	frame := d.buildFrameLocked(time.Now(), W, H)
	d.mu.Unlock()

	var b strings.Builder
	if clear {
		b.WriteString(clearAll)
	}
	// Absolute-position each line (row;1) and clear to EOL. One write per frame,
	// no full clear — this is what keeps the redraw flicker-free.
	for i, ln := range frame {
		fmt.Fprintf(&b, "\033[%d;1H", i+1)
		b.WriteString(ln)
		b.WriteString(clearEOL)
	}
	fmt.Fprint(d.out, b.String())
}

// layout holds the panel geometry derived from the terminal size.
type layout struct {
	W, H       int
	lw, rw     int // inner widths of the left (roster) and right columns
	bodyTop    int // first body line index (0-based)
	bodyRows   int
	detailRows int
	streamRows int
}

func computeLayout(W, H int) layout {
	lw := clamp(W/4, 24, 36)
	if W-3-lw < 30 { // guarantee the right column has room
		lw = W - 3 - 30
	}
	lw = clamp(lw, 12, W-4)
	rw := W - 3 - lw
	if rw < 1 {
		rw = 1
	}
	// Body occupies lines 1 .. H-3 (line 0 = top bar, H-2 = bottom border,
	// H-1 = help line).
	bodyRows := H - 3
	detailRows := clamp(bodyRows/3, 4, 12)
	if detailRows > bodyRows-3 { // leave a separator + at least 2 stream rows
		detailRows = bodyRows - 3
	}
	if detailRows < 1 {
		detailRows = 1
	}
	streamRows := bodyRows - detailRows - 1
	if streamRows < 1 {
		streamRows = 1
	}
	return layout{W: W, H: H, lw: lw, rw: rw, bodyTop: 1, bodyRows: bodyRows, detailRows: detailRows, streamRows: streamRows}
}

func (d *dashView) buildFrameLocked(now time.Time, W, H int) []string {
	if W < 40 || H < 9 {
		return d.tooSmallFrame(W, H)
	}
	L := computeLayout(W, H)
	lines := make([]string, H)
	lines[0] = d.statusBar(now, L)

	roster := d.rosterLinesLocked(now, L)
	detail := d.detailLinesLocked(now, L)
	stream := d.streamLinesLocked(L)

	for r := 0; r < L.bodyRows; r++ {
		left := ""
		if r < len(roster) {
			left = roster[r]
		}
		idx := L.bodyTop + r
		switch {
		case r < L.detailRows:
			lines[idx] = d.frameRow(left, detail[r], L)
		case r == L.detailRows:
			lines[idx] = d.sepRow(left, L)
		default:
			si := r - L.detailRows - 1
			right := ""
			if si < len(stream) {
				right = stream[si]
			}
			lines[idx] = d.frameRow(left, right, L)
		}
	}
	lines[H-2] = d.bottomBorder(L)
	lines[H-1] = d.helpLine(W)
	return lines
}

func (d *dashView) statusBar(now time.Time, L layout) string {
	total, live := d.countsLocked(now)
	title := " agenttap "
	right := fmt.Sprintf(" %s   %d agents | %d live ", now.Format("15:04:05"), total, live)
	// The bar is "+=" + title + fill + right + "=+", exactly L.W wide. Reserve at
	// least one '=' of fill and truncate the right status on a narrow terminal.
	remaining := L.W - 4 - dispWidth(title) // shared by fill + right
	if remaining < 1 {
		remaining = 1
	}
	if maxRight := remaining - 1; dispWidth(right) > maxRight {
		right = takeCells(right, max(maxRight, 0))
	}
	fill := remaining - dispWidth(right)
	if fill < 1 {
		fill = 1
	}
	var b strings.Builder
	b.WriteString(d.pt.c(colBorder, "+="))
	b.WriteString(d.title(title))
	b.WriteString(d.pt.c(colBorder, strings.Repeat("=", fill)))
	b.WriteString(d.pt.c(colMeta, right))
	b.WriteString(d.pt.c(colBorder, "=+"))
	return b.String()
}

func (d *dashView) title(s string) string {
	if !d.pt.on {
		return s
	}
	return cBold + colText + s + cReset
}

func (d *dashView) countsLocked(now time.Time) (total, live int) {
	for _, a := range d.agents {
		if a.seenHB {
			switch heartStage(now.Sub(a.lastSeen), a.interval) {
			case stageDead:
				continue // dropped/offline
			case stageLive:
				live++
			}
		}
		total++
	}
	return
}

func (d *dashView) frameRow(left, right string, L layout) string {
	v := d.pt.c(colBorder, "|")
	return v + padRight(left, L.lw) + v + padRight(right, L.rw) + v
}

// sepRow is the horizontal rule between the DETAIL and TRAFFIC panels; it spans
// only the right column, so the left (roster) column continues uninterrupted.
func (d *dashView) sepRow(left string, L layout) string {
	v := d.pt.c(colBorder, "|")
	plus := d.pt.c(colBorder, "+")
	return v + padRight(left, L.lw) + plus + d.pt.c(colBorder, strings.Repeat("-", L.rw)) + plus
}

func (d *dashView) bottomBorder(L layout) string {
	return d.pt.c(colBorder, "+"+strings.Repeat("=", L.W-2)+"+")
}

func (d *dashView) helpLine(W int) string {
	hb := "on"
	if !d.showHB {
		hb = "off"
	}
	move := "select agent"
	if d.focus == focusFeed {
		move = "scrub packets"
	}
	help := fmt.Sprintf("  left/right pane   up/down %s   space mute   s solo   a all   h hb(%s)   q quit", move, hb)
	return d.cell(W, span{help, colMeta})
}

func (d *dashView) rosterLinesLocked(now time.Time, L layout) []string {
	focused := d.focus == focusRoster
	titleCol := colLabel
	if focused {
		titleCol = colSel
	}
	out := make([]string, 0, L.bodyRows)
	out = append(out, d.cell(L.lw, span{"AGENTS", titleCol}))
	keys := d.rosterKeysLocked()
	avail := L.bodyRows - 1
	if avail < 1 {
		avail = 1
	}
	// Scroll so the selected row stays visible.
	start := 0
	if sel := indexOf(keys, d.selected); sel >= avail {
		start = sel - avail + 1
	}
	for i := start; i < len(keys) && len(out) < L.bodyRows; i++ {
		out = append(out, d.rosterRow(now, keys[i], L.lw, keys[i] == d.selected, focused))
	}
	for len(out) < L.bodyRows {
		out = append(out, blank(L.lw))
	}
	return out
}

func (d *dashView) rosterRow(now time.Time, key string, lw int, selected, focused bool) string {
	a := d.agents[key]
	cursor, curCol := "  ", colMeta
	if selected {
		cursor = "> "
		if focused {
			curCol = colSel
		}
	}
	mark, markCol := "[x] ", colMeta
	if d.muted[key] {
		mark, markCol = "[ ] ", colDim
	}
	heartCol := colHeartStale
	if a != nil && a.seenHB {
		age := now.Sub(a.lastSeen)
		heartCol = heartColor(heartStage(age, a.interval), age < 300*time.Millisecond)
	}
	labelCol := colText
	switch {
	case selected && focused:
		labelCol = colSel
	case d.muted[key]:
		labelCol = colMute
	}
	return d.cell(lw,
		span{cursor, curCol},
		span{mark, markCol},
		span{heartGlyph + " ", heartCol},
		span{key, labelCol},
	)
}

// detailLinesLocked is the readout that follows focus: the selected agent's
// conversation (roster focus) or the selected packet's full detail (feed focus).
func (d *dashView) detailLinesLocked(now time.Time, L layout) []string {
	if d.focus == focusFeed {
		return d.packetDetailLinesLocked(L)
	}
	return d.conversationLinesLocked(now, L)
}

func (d *dashView) conversationLinesLocked(now time.Time, L layout) []string {
	w := L.rw
	var ls [][]span
	key := d.selected
	if key == "" {
		ls = append(ls, []span{{"DETAIL", colLabel}}, []span{{"waiting for traffic...", colMeta}})
		return d.renderSpanLines(w, ls, L.detailRows)
	}

	a := d.agents[key]
	header := []span{{"DETAIL  ", colLabel}, {key, colText}}
	if a != nil && a.seenHB {
		age := now.Sub(a.lastSeen)
		header = append(header,
			span{"  ", ""},
			span{heartGlyph, heartColor(heartStage(age, a.interval), age < 300*time.Millisecond)},
			span{" " + humanDur(age), colMeta},
			span{" ~" + humanDur(a.interval), colDim},
		)
	} else if a != nil {
		header = append(header, span{"  ", ""}, span{heartGlyph + " no hb", colHeartStale})
	}
	ls = append(ls, header)

	if a != nil {
		inst := "-"
		if a.instance != "" {
			inst = shortID(a.instance)
		}
		more := ""
		if len(a.instSet) > 1 {
			more = fmt.Sprintf(" +%d", len(a.instSet)-1)
		}
		ls = append(ls, []span{{fmt.Sprintf("inst %s%s   %d beats", inst, more, a.beats), colMeta}})
	}

	ex := d.exch[key]
	if ex == nil || ex.c == nil {
		ls = append(ls, []span{{"no exchanges yet", colDim}})
		return d.renderSpanLines(w, ls, L.detailRows)
	}
	c := ex.c
	if ex.replyInbox != "" {
		ls = append(ls, []span{{"reply " + shortInbox(ex.replyInbox), colDim}})
	}

	ptext := sanitize(c.promptText)
	if strings.TrimSpace(ptext) == "" {
		ptext = "<no prompt captured>"
	}
	ls = append(ls, wrapSpanLines("client> ", colPrompt, ptext, colText, w)...)
	if len(c.atts) > 0 {
		ls = append(ls, []span{{"attach: " + attachmentsString(c.atts), colMeta}})
	}

	reply := strings.TrimSpace(c.prose.String())
	tail, tailCol := "  (streaming)", colChunk
	if ex.done {
		tail, tailCol = "  ["+ex.reason+"]", colOnline
		if ex.reason != doneSentinel {
			tailCol = colEnd
		}
	}
	if reply == "" {
		if c.ctrlFrames > 0 {
			reply = fmt.Sprintf("<%d ctrl frame(s), no text yet>", c.ctrlFrames)
		} else {
			reply = "<no reply yet>"
		}
	}
	rl := wrapSpanLines("agent > ", colChunk, sanitize(reply), colText, w)
	rl[len(rl)-1] = append(rl[len(rl)-1], span{tail, tailCol})
	ls = append(ls, rl...)

	dur := now.Sub(c.started)
	if ex.done {
		dur = ex.lastAt.Sub(c.started)
	}
	ls = append(ls, []span{{fmt.Sprintf("%d fr (%d txt/%d ctrl)  %s  %s",
		c.chunks, c.textFrames, c.ctrlFrames, humanBytes(c.bytesN), humanDur(dur)), colMeta}})

	return d.renderSpanLines(w, ls, L.detailRows)
}

// packetDetailLinesLocked decodes the selected event into a per-type field list
// — "click a packet in Wireshark". Metadata (subject, full reply inbox, headers)
// is shown in full; only the payload is capped (with a bytes-total note).
func (d *dashView) packetDetailLinesLocked(L layout) []string {
	w := L.rw
	id, ok := d.effectiveCursorLocked()
	if !ok {
		return d.renderSpanLines(w, [][]span{{{"PACKET", colLabel}}, {{"no packets yet", colDim}}}, L.detailRows)
	}
	fl, ok := d.feed.byID(id)
	if !ok {
		return d.renderSpanLines(w, [][]span{{{"PACKET", colLabel}}, {{"(scrolled out of buffer)", colDim}}}, L.detailRows)
	}

	var ls [][]span
	ls = append(ls, []span{
		{"PACKET ", colLabel},
		{fmt.Sprintf("#%d  ", fl.id), colMeta},
		{fl.typ, fl.typCol},
		{"   " + fl.t.Format("15:04:05.000"), colMeta},
	})
	field := func(label, value, valCol string) { ls = append(ls, fieldRows(label, value, valCol, w)...) }
	// Every exchange packet (prompt, reply frame, END, DONE) carries the same
	// identity block: which agent (full identity + instance "agent id"), and
	// which exchange (originating prompt subject + reply inbox).
	agentCtx := func(reply, replyCol string) {
		field("agent", fl.key, colText)
		if fl.instance != "" {
			field("instance", fl.instance, colMeta)
		}
		if fl.subject != "" {
			field("subject", fl.subject, colText)
		}
		if reply != "" {
			field("reply", reply, replyCol)
		}
	}

	switch fl.kind {
	case fkPrompt:
		reply, rcol := fl.reply, colChunk
		if reply == "" {
			reply, rcol = "(none -- fire-and-forget)", colEnd
		}
		agentCtx(reply, rcol)
		if fl.headers != "" {
			field("headers", fl.headers, colMeta)
		}
		if fl.atts != "" {
			field("attach", fl.atts, colMeta)
		}
		ls = append(ls, d.payloadRows("prompt", fl, w)...)
	case fkChunk, fkCtrl:
		ft := fl.frameType
		if ft == "" {
			ft = "(raw text)"
		}
		field("type", fmt.Sprintf("%s  (frame %d)", ft, fl.chunkN), fl.typCol)
		agentCtx(fl.reply, colChunk)
		if fl.headers != "" {
			field("headers", fl.headers, colMeta)
		}
		field("bytes", humanBytes(fl.bytesN), colMeta)
		if fl.sinceStart > 0 {
			field("t+", humanDur(fl.sinceStart), colDim)
		}
		ls = append(ls, d.payloadRows("data", fl, w)...)
	case fkEnd:
		agentCtx(fl.reply, colChunk)
		field("note", "empty message, no headers -- stream terminator", colMeta)
	case fkDone:
		agentCtx(fl.reply, colChunk)
		field("summary", fl.detail, colMeta)
	case fkHB, fkOnline:
		field("agent", fl.key, colText)
		if fl.instance != "" {
			field("instance", fl.instance, colMeta)
		}
		if fl.ts != "" {
			field("ts", fl.ts, colMeta)
		}
	case fkWarn:
		ls = append(ls, d.payloadRows("message", fl, w)...)
	default:
		field("detail", fl.detail, colText)
	}
	return d.renderSpanLines(w, ls, L.detailRows)
}

// fieldRows renders "label   value" with the value wrapped under a hanging
// indent. Values are shown in full (subjects, inboxes, headers don't truncate).
func fieldRows(label, value, valCol string, w int) [][]span {
	const lw = 9
	avail := w - lw - 1
	if avail < 4 {
		avail = 4
	}
	rows := wrapText(sanitize(value), avail)
	if len(rows) == 0 {
		rows = []string{""}
	}
	out := make([][]span, 0, len(rows))
	for i, r := range rows {
		lab := ""
		if i == 0 {
			lab = label
		}
		out = append(out, []span{{fitPlain(lab, lw), colLabel}, {" ", ""}, {r, valCol}})
	}
	return out
}

// payloadRows renders a (possibly capped) payload under a label, noting the byte
// total when the stored copy was truncated.
func (d *dashView) payloadRows(label string, fl feedLine, w int) [][]span {
	if strings.TrimSpace(fl.payload) == "" {
		return fieldRows(label, "<empty>", colDim, w)
	}
	rows := fieldRows(label, fl.payload, colText, w)
	if !fl.payloadFull {
		rows = append(rows, []span{
			{fitPlain("", 9), ""}, {" ", ""},
			{fmt.Sprintf("... (%s total, showing first %d chars)", humanBytes(fl.bytesN), maxPayloadStore), colDim},
		})
	}
	return rows
}

func (d *dashView) streamLinesLocked(L layout) []string {
	w, rows := L.rw, L.streamRows
	out := make([]string, 0, rows)

	ids := d.visibleIDsLocked()
	feedRows := rows - 1
	if feedRows < 1 {
		feedRows = 1
	}

	// Locate the cursor among the visible events (feed focus only).
	cursorIdx := -1
	if d.focus == focusFeed && len(ids) > 0 {
		if id, ok := d.effectiveCursorLocked(); ok {
			cursorIdx = indexOfU64(ids, id)
		}
	}

	// Header: label + hb/filter hint, plus a PAUSED marker when frozen.
	hb := "on"
	if !d.showHB {
		hb = "off"
	}
	hint := "hb:" + hb
	if d.anyMutedLocked() {
		hint += "  filtered"
	}
	if cursorIdx >= 0 && !d.followLive {
		hint = fmt.Sprintf("PAUSED +%d   %s", len(ids)-1-cursorIdx, hint)
	}
	titleCol := colLabel
	if d.focus == focusFeed {
		titleCol = colSel
	}
	out = append(out, d.cell(w, span{"LIVE TRAFFIC", titleCol}, span{"   ", ""}, span{hint, colDim}))

	if len(ids) == 0 {
		return padLines(out, rows, w)
	}

	// Window: the newest tail, scrolled just enough to keep the cursor visible.
	start := len(ids) - feedRows
	if start < 0 {
		start = 0
	}
	if cursorIdx >= 0 {
		if cursorIdx < start {
			start = cursorIdx
		} else if cursorIdx >= start+feedRows {
			start = cursorIdx - feedRows + 1
		}
	}
	for i := start; i < len(ids) && i < start+feedRows; i++ {
		if fl, ok := d.feed.byID(ids[i]); ok {
			out = append(out, d.feedRender(fl, w, i == cursorIdx))
		}
	}
	return padLines(out, rows, w)
}

// feedRender draws one traffic line. A 2-col gutter carries the selection cursor
// so focus toggling never shifts the columns; the selected packet is drawn in a
// single bright color to stand out.
func (d *dashView) feedRender(fl feedLine, w int, sel bool) string {
	if sel {
		return d.cell(w,
			span{"> ", colSel},
			span{fl.t.Format("15:04:05.000"), colSel},
			span{" ", ""},
			span{fitPlain(fl.typ, 7), colSel},
			span{" ", ""},
			span{fitPlain(fl.flow, flowWidth(w)), colSel},
			span{" ", ""},
			span{fl.detail, colSel},
		)
	}
	return d.cell(w,
		span{"  ", colMeta},
		span{fl.t.Format("15:04:05.000"), colMeta},
		span{" ", ""},
		span{fitPlain(fl.typ, 7), fl.typCol},
		span{" ", ""},
		span{fitPlain(fl.flow, flowWidth(w)), colDim},
		span{" ", ""},
		span{fl.detail, colText},
	)
}

func (d *dashView) anyMutedLocked() bool {
	for _, m := range d.muted {
		if m {
			return true
		}
	}
	return false
}

func (d *dashView) tooSmallFrame(W, H int) []string {
	lines := make([]string, H)
	msg := "terminal too small (need >= 40x9)"
	for i := range lines {
		if i == H/2 {
			lines[i] = strings.Repeat(" ", max((W-dispWidth(msg))/2, 0)) + d.pt.c(colWarn, takeCells(msg, W))
		}
	}
	return lines
}

// --- cell / span composition ------------------------------------------------
//
// A cell is a fixed-width run built from PLAIN-text spans (no escapes), so width
// math is exact: each span is clipped on a display-cell boundary as needed, the
// color is applied here, and the result is padded to the cell width.

type span struct{ s, col string }

func (d *dashView) cell(w int, spans ...span) string {
	used := 0
	var b strings.Builder
	for _, sp := range spans {
		if used >= w {
			break
		}
		txt := sp.s
		if used+dispWidth(txt) > w {
			txt = takeCells(txt, w-used)
		}
		b.WriteString(d.pt.c(sp.col, txt))
		used += dispWidth(txt)
	}
	if used < w {
		b.WriteString(strings.Repeat(" ", w-used))
	}
	return b.String()
}

// renderSpanLines renders up to `rows` span-lines into fixed-width cells,
// padding short and dropping overflow.
func (d *dashView) renderSpanLines(w int, lines [][]span, rows int) []string {
	out := make([]string, 0, rows)
	for _, ln := range lines {
		if len(out) == rows {
			break
		}
		out = append(out, d.cell(w, ln...))
	}
	for len(out) < rows {
		out = append(out, blank(w))
	}
	return out
}

// wrapSpanLines word-wraps text to width w under a colored label, hanging the
// continuation lines under the label.
func wrapSpanLines(label, labelCol, text, textCol string, w int) [][]span {
	indent := dispWidth(label)
	avail := w - indent
	if avail < 4 {
		avail = 4
	}
	rows := wrapText(text, avail)
	out := make([][]span, 0, len(rows))
	for i, r := range rows {
		if i == 0 {
			out = append(out, []span{{label, labelCol}, {r, textCol}})
		} else {
			out = append(out, []span{{strings.Repeat(" ", indent), ""}, {r, textCol}})
		}
	}
	if len(out) == 0 {
		out = append(out, []span{{label, labelCol}})
	}
	return out
}

// --- small helpers ----------------------------------------------------------

func promptFlow(pm promptMeta) string {
	s := pm.owner + " -> " + pm.agent
	if pm.session != "" {
		s += " (" + pm.session + ")"
	}
	return s
}

// snippet renders payload text as a single sanitized, length-capped line.
func snippet(s string) string { return clipRunes(sanitize(firstLine(s)), 240) }

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return strings.TrimRight(s[:i], "\r") + " ..."
	}
	return s
}

func clipRunes(s string, max int) string {
	if r := []rune(s); len(r) > max {
		return string(r[:max]) + "..."
	}
	return s
}

// capPayload sanitizes s and caps it to maxPayloadStore runes for retention,
// reporting whether the whole (sanitized) text fit.
func capPayload(s string) (text string, full bool) {
	san := sanitize(s)
	if r := []rune(san); len(r) > maxPayloadStore {
		return string(r[:maxPayloadStore]), false
	}
	return san, true
}

// shortInbox abbreviates a reply inbox subject to its distinguishing tail,
// e.g. "_INBOX.agents.FYFA2ZE57WAKEQRTQGNFGI" -> "_IN..FYFA2Z..GNFGI".
func shortInbox(s string) string {
	if i := strings.LastIndexByte(s, '.'); i >= 0 && i+1 < len(s) {
		tail := s[i+1:]
		if len(tail) > 12 {
			tail = tail[:6] + ".." + tail[len(tail)-4:]
		}
		return "_IN.." + tail
	}
	return shortID(s)
}

// fitPlain clips or pads plain text to exactly w display columns.
func fitPlain(s string, w int) string {
	if dispWidth(s) > w {
		s = takeCells(s, w)
	}
	if d := dispWidth(s); d < w {
		return s + strings.Repeat(" ", w-d)
	}
	return s
}

func flowWidth(w int) int { return clamp(w*2/5, 14, 34) }

func blank(w int) string { return strings.Repeat(" ", w) }

// padLines pads short to `rows` blank cells of width w, and drops any overflow.
func padLines(out []string, rows, w int) []string {
	for len(out) < rows {
		out = append(out, blank(w))
	}
	if len(out) > rows {
		out = out[:rows]
	}
	return out
}

func indexOf(ss []string, s string) int {
	for i, v := range ss {
		if v == s {
			return i
		}
	}
	return -1
}

func indexOfU64(s []uint64, v uint64) int {
	for i, x := range s {
		if x == v {
			return i
		}
	}
	return -1
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// --- demo -------------------------------------------------------------------

// runDashDemo drives a small live traffic generator so `-demo -view dash` is a
// proper showcase: a few agents heartbeat at staggered phases (so hearts pulse
// out of sync), one goes quiet to demonstrate the fade-and-drop, and exchanges
// stream in periodically. It runs until the user quits.
func runDashDemo(d *dashView) {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	agents := []heartbeatPayload{
		{Agent: "hermes", Owner: "rs", InstanceID: "P6jQnJ56tGj61Ct0vj0KOQ", Session: "miami"},
		{Agent: "atlas", Owner: "rs", InstanceID: "Z1kFb9rNq2WxApL0", Session: "ops"},
		{Agent: "echo", Owner: "m64", InstanceID: "Ab12Cd34Ef56Gh78", Session: "m64"},
		{Agent: "nyx", Owner: "rs", InstanceID: "Qq88Ww77Ee66Rr55", Session: "research"},
	}
	for i := range agents {
		d.online(agents[i])
		d.heartbeat(agents[i])
	}

	hbTick := time.NewTicker(1 * time.Second)
	defer hbTick.Stop()
	exTick := time.NewTicker(3500 * time.Millisecond)
	defer exTick.Stop()

	var seq uint64
	tickN := 0
	for {
		select {
		case <-d.stopCh:
			return
		case <-d.quitCh:
			return
		case <-hbTick.C:
			tickN++
			for i := 0; i < 3; i++ { // three steady agents, staggered ~every 4s
				if (tickN+i)%4 == 0 {
					d.heartbeat(agents[i])
				}
			}
			if tickN <= 8 && tickN%4 == 0 { // nyx beats early, then goes silent -> fades & drops
				d.heartbeat(agents[3])
			}
		case <-exTick.C:
			seq++
			go d.playExchange(seq, agents[rng.Intn(len(agents))], rng)
		}
	}
}

func (d *dashView) playExchange(seq uint64, hb heartbeatPayload, rng *rand.Rand) {
	subject := fmt.Sprintf("agents.prompt.%s.%s.%s", hb.Agent, hb.Owner, hb.Session)
	reply := "_INBOX.agents." + randID(rng, 22)
	m := &nats.Msg{Subject: subject, Reply: reply, Header: nats.Header{"X-Agent-Protocol": []string{"0.3"}}}
	pm := promptMeta{agent: hb.Agent, owner: hb.Owner, session: hb.Session, parsed: true}
	label := subjectLabel(hb.Agent, hb.Owner, hb.Session, true, subject)

	prompts := []string{
		"What's the weather in Miami for the F1 race this weekend, and is rain likely?",
		"Summarize the overnight ops alerts and flag anything that needs a human.",
		"Draft a short, friendly release note for v0.3 of the tracer.",
		"How many requests did we serve in the last hour, broken down by region?",
	}
	d.prompt(seq, label, pm, m, prompts[rng.Intn(len(prompts))], nil)

	d.chunk(seq, label, 1, &nats.Msg{Subject: reply, Data: []byte(`{"type":"status","data":"ack"}`)})
	if d.sleepOrStop(250 * time.Millisecond) {
		return
	}
	words := strings.Fields("Sure -- here's a concise answer streamed as several deltas, " +
		"so you can watch the reply grow token by token in the detail pane while the " +
		"traffic tail shows every frame on the wire.")
	n := 2
	for i, w := range words {
		fr := fmt.Sprintf(`{"type":"response","data":%q}`, w+" ")
		d.chunk(seq, label, n, &nats.Msg{Subject: reply, Data: []byte(fr)})
		n++
		if i == 2 && rng.Intn(2) == 0 { // sometimes the agent pauses to ask the caller (§7)
			qid := "q-" + randID(rng, 6)
			qsub := "_INBOX.q." + randID(rng, 10)
			const qPrompt = "Proceed with the full hourly breakdown? (yes/no)"
			d.chunk(seq, label, n, &nats.Msg{Subject: reply,
				Data: []byte(fmt.Sprintf(`{"type":"query","data":{"id":%q,"reply_subject":%q,"prompt":%q}}`,
					qid, qsub, qPrompt))})
			n++
			if d.sleepOrStop(450 * time.Millisecond) {
				return
			}
			d.queryReply(seq, label, qid, qsub, qPrompt, "yes")
		}
		if i == 4 {
			d.heartbeat(hb) // a beat arriving mid-stream
		}
		if d.sleepOrStop(170 * time.Millisecond) {
			return
		}
	}
	d.sentinel(seq, label, &nats.Msg{Subject: reply})
	d.done(seq, label, n-1, doneSentinel)
}

// sleepOrStop waits for dur, returning true early if the view is shutting down.
func (d *dashView) sleepOrStop(dur time.Duration) bool {
	select {
	case <-time.After(dur):
		return false
	case <-d.stopCh:
		return true
	case <-d.quitCh:
		return true
	}
}

func randID(rng *rand.Rand, n int) string {
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[rng.Intn(len(alphabet))]
	}
	return string(b)
}
