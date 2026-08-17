package main

import (
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestHeartStage(t *testing.T) {
	const iv = 5 * time.Second // thresholds at 6s (1.2x), 12.5s (2.5x), 20s (4x)
	cases := []struct {
		age  time.Duration
		want heartStageT
	}{
		{0, stageLive},
		{4 * time.Second, stageLive},
		{7 * time.Second, stageWarn},
		{13 * time.Second, stageStale},
		{21 * time.Second, stageDead},
		{time.Hour, stageDead},
	}
	for _, c := range cases {
		if got := heartStage(c.age, iv); got != c.want {
			t.Errorf("heartStage(%v, %v) = %d, want %d", c.age, iv, got, c.want)
		}
	}
	// A zero/negative interval must fall back to the 5s default (no divide-by-zero).
	if got := heartStage(3*time.Second, 0); got != stageLive {
		t.Errorf("heartStage with zero interval = %d, want stageLive", got)
	}
}

func TestAgentKeyOf(t *testing.T) {
	cases := []struct {
		agent, owner, session, want string
	}{
		{"hermes", "rs", "miami", "hermes/rs (miami)"},
		{"atlas", "rs", "", "atlas/rs"},
	}
	for _, c := range cases {
		if got := agentKeyOf(c.agent, c.owner, c.session); got != c.want {
			t.Errorf("agentKeyOf(%q,%q,%q) = %q, want %q", c.agent, c.owner, c.session, got, c.want)
		}
	}
	// The roster key MUST equal agentLabel so heartbeats and prompts collide on
	// the same roster row.
	hb := heartbeatPayload{Agent: "hermes", Owner: "rs", Session: "miami"}
	if agentKeyOf(hb.Agent, hb.Owner, hb.Session) != agentLabel(hb) {
		t.Errorf("agentKeyOf != agentLabel: %q vs %q", agentKeyOf(hb.Agent, hb.Owner, hb.Session), agentLabel(hb))
	}
}

func TestFeedRing(t *testing.T) {
	r := newFeedRing(3)
	for i := 1; i <= 5; i++ {
		r.push(feedLine{typ: string(rune('0' + i))})
	}
	if r.len() != 3 {
		t.Fatalf("len = %d, want 3", r.len())
	}
	// Oldest-first after wrap: 1 and 2 were evicted, leaving 3,4,5.
	for i, want := range []string{"3", "4", "5"} {
		if got := r.at(i).typ; got != want {
			t.Errorf("at(%d) = %q, want %q", i, got, want)
		}
	}
}

func TestClassifyChunk(t *testing.T) {
	cases := []struct {
		in       string
		n        int
		wantTyp  string
		wantKind feedKind
	}{
		{`{"type":"response","data":"Hello"}`, 7, "resp 7", fkChunk},
		{`{"type":"status","data":"ack"}`, 2, "status", fkCtrl},
		{`{"type":"error","data":"boom"}`, 3, "error", fkCtrl},
		{`{"type":"tool_call","data":{"name":"x"}}`, 4, "tool", fkCtrl},
		{`{"type":"metadata","data":{"k":1}}`, 5, "meta", fkCtrl},
		{`raw bytes, not a frame`, 9, "data 9", fkChunk},
	}
	for _, c := range cases {
		typ, _, kind, _ := classifyChunk([]byte(c.in), c.n)
		if typ != c.wantTyp || kind != c.wantKind {
			t.Errorf("classifyChunk(%q,%d) = (%q,%d), want (%q,%d)", c.in, c.n, typ, kind, c.wantTyp, c.wantKind)
		}
	}
	// A response frame's detail is the (sanitized) delta text.
	if _, _, _, detail := classifyChunk([]byte(`{"type":"response","data":"Hi there"}`), 1); detail != "Hi there" {
		t.Errorf("response detail = %q, want %q", detail, "Hi there")
	}
	// A string control frame surfaces its type and value.
	if _, _, _, detail := classifyChunk([]byte(`{"type":"status","data":"ack"}`), 1); detail != "status=ack" {
		t.Errorf("status detail = %q, want %q", detail, "status=ack")
	}
}

func TestShortInbox(t *testing.T) {
	cases := []struct{ in, want string }{
		{"_INBOX.agents.FYFA2ZE57WAKEQRTQGNFGI", "_IN..FYFA2Z..NFGI"},
		{"foo.bar", "_IN..bar"},
		{"abc", "abc"}, // no dot -> shortID passthrough (<=12)
	}
	for _, c := range cases {
		if got := shortInbox(c.in); got != c.want {
			t.Errorf("shortInbox(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestFitPlain(t *testing.T) {
	cases := []struct {
		in   string
		w    int
		want string
	}{
		{"hi", 5, "hi   "},    // pad
		{"abcdef", 4, "abcd"}, // clip
		{"你", 3, "你 "},        // wide rune (2 cells) + pad to 3
		{"你你", 3, "你 "},       // can't fit 2nd wide rune; clip to one, pad to 3
		{"exact", 5, "exact"}, // unchanged
	}
	for _, c := range cases {
		got := fitPlain(c.in, c.w)
		if got != c.want {
			t.Errorf("fitPlain(%q,%d) = %q, want %q", c.in, c.w, got, c.want)
		}
		if dispWidth(got) != c.w {
			t.Errorf("fitPlain(%q,%d) display width = %d, want %d", c.in, c.w, dispWidth(got), c.w)
		}
	}
}

func TestClamp(t *testing.T) {
	cases := []struct{ v, lo, hi, want int }{
		{5, 1, 10, 5},
		{-1, 0, 9, 0},
		{100, 0, 9, 9},
		{0, 0, 0, 0},
	}
	for _, c := range cases {
		if got := clamp(c.v, c.lo, c.hi); got != c.want {
			t.Errorf("clamp(%d,%d,%d) = %d, want %d", c.v, c.lo, c.hi, got, c.want)
		}
	}
}

// computeLayout must always produce a self-consistent, exactly-fitting layout:
// the two inner column widths plus the three vertical borders fill the width,
// and the detail rows + separator + stream rows fill the body height.
func TestComputeLayoutInvariants(t *testing.T) {
	sizes := []struct{ W, H int }{
		{40, 9}, {60, 12}, {80, 24}, {100, 30}, {120, 40}, {200, 60}, {300, 100},
	}
	for _, s := range sizes {
		L := computeLayout(s.W, s.H)
		if L.lw+L.rw != s.W-3 {
			t.Errorf("W=%d: lw(%d)+rw(%d) = %d, want %d", s.W, L.lw, L.rw, L.lw+L.rw, s.W-3)
		}
		if L.lw < 12 || L.lw > 36 {
			t.Errorf("W=%d: lw=%d out of [12,36]", s.W, L.lw)
		}
		if L.rw < 1 {
			t.Errorf("W=%d: rw=%d < 1", s.W, L.rw)
		}
		bodyRows := s.H - 3
		if L.detailRows+L.streamRows+1 != bodyRows {
			t.Errorf("H=%d: detail(%d)+stream(%d)+sep(1) = %d, want bodyRows %d",
				s.H, L.detailRows, L.streamRows, L.detailRows+L.streamRows+1, bodyRows)
		}
		if L.detailRows < 1 || L.streamRows < 1 {
			t.Errorf("H=%d: detailRows=%d streamRows=%d, both must be >= 1", s.H, L.detailRows, L.streamRows)
		}
	}
}

// cell must render to EXACTLY w display columns regardless of color, clipping,
// or wide runes — this is what keeps the framed layout's borders aligned.
func TestCellExactWidth(t *testing.T) {
	d := &dashView{pt: paint{on: true}} // color on: dispWidth must ignore the escapes
	cases := []struct {
		w     int
		spans []span
	}{
		{10, []span{{"hi", colText}}},
		{5, []span{{"abcdefghij", colChunk}}},            // overflow -> clipped
		{8, []span{{"a你b", colPrompt}, {" x", colMeta}}}, // wide rune
		{6, []span{{"你你你", colText}}},                    // wide-rune clip boundary
		{20, []span{{"left", colLabel}, {"  right", colMeta}}},
		{0, []span{{"anything", colText}}},
	}
	for _, c := range cases {
		if got := d.cell(c.w, c.spans...); dispWidth(got) != c.w {
			t.Errorf("cell(%d, %v) display width = %d, want %d", c.w, c.spans, dispWidth(got), c.w)
		}
	}
}

// buildFrameLocked must always return exactly H lines, each exactly W display
// columns wide (the frame fills the alt-screen with no ragged edges). We seed
// live state through the normal view methods first, and check BOTH focus modes
// (roster -> conversation pane, feed -> packet-detail pane).
func TestBuildFrameDimensions(t *testing.T) {
	d := seededDash()
	sizes := []struct{ W, H int }{
		{40, 9}, {80, 24}, {100, 30}, {120, 50}, {37, 8}, // last is below the minimum -> tooSmall
	}
	for _, focus := range []focusT{focusRoster, focusFeed} {
		d.focus = focus
		for _, s := range sizes {
			d.mu.Lock()
			lines := d.buildFrameLocked(time.Now(), s.W, s.H)
			d.mu.Unlock()
			if len(lines) != s.H {
				t.Errorf("focus=%d W=%d H=%d: got %d lines, want %d", focus, s.W, s.H, len(lines), s.H)
				continue
			}
			// Below the minimum we render a centered notice that is cleared-to-EOL
			// at paint time, so those lines need not be full width.
			if s.W < 40 || s.H < 9 {
				continue
			}
			for i, ln := range lines {
				if dispWidth(ln) != s.W {
					t.Errorf("focus=%d W=%d H=%d: line %d width = %d, want %d", focus, s.W, s.H, i, dispWidth(ln), s.W)
				}
			}
		}
	}
}

// seededDash returns a dashView with one agent and a small completed exchange,
// driven through the normal (locking) view methods.
func seededDash() *dashView {
	d := newDashView(true)
	hb := heartbeatPayload{Agent: "hermes", Owner: "rs", InstanceID: "ABC123XYZ789", Session: "miami"}
	d.online(hb)
	d.heartbeat(hb)
	pm := promptMeta{agent: "hermes", owner: "rs", session: "miami", parsed: true}
	reply := "_INBOX.agents.ABCDEF1234567890GHIJK"
	d.prompt(1, "hermes/rs (miami)", pm,
		&nats.Msg{Subject: "agents.prompt.hermes.rs.miami", Reply: reply},
		"What is the weather in Miami for the F1 race this weekend, in detail?", nil)
	d.chunk(1, "hermes/rs (miami)", 1, &nats.Msg{Subject: reply, Data: []byte(`{"type":"status","data":"ack"}`)})
	d.chunk(1, "hermes/rs (miami)", 2, &nats.Msg{Subject: reply, Data: []byte(`{"type":"response","data":"Mostly sunny."}`)})
	d.sentinel(1, "hermes/rs (miami)", &nats.Msg{Subject: reply})
	d.done(1, "hermes/rs (miami)", 2, doneSentinel)
	return d
}

func TestFeedRingIDs(t *testing.T) {
	r := newFeedRing(3)
	for i := 0; i < 5; i++ {
		if got := r.push(feedLine{typ: itoa(i)}); got != uint64(i) {
			t.Errorf("push %d returned id %d, want %d", i, got, i)
		}
	}
	if r.oldestID() != 2 || r.newestID() != 4 {
		t.Fatalf("oldest=%d newest=%d, want 2 and 4", r.oldestID(), r.newestID())
	}
	// Evicted ids are gone; retained ids resolve to the right entry.
	if _, ok := r.byID(1); ok {
		t.Errorf("byID(1) should be evicted")
	}
	if _, ok := r.byID(5); ok {
		t.Errorf("byID(5) should not exist yet")
	}
	for _, id := range []uint64{2, 3, 4} {
		fl, ok := r.byID(id)
		if !ok || fl.typ != itoa(int(id)) {
			t.Errorf("byID(%d) = (%q,%v), want typ %q", id, fl.typ, ok, itoa(int(id)))
		}
	}
}

func TestFramePayload(t *testing.T) {
	cases := []struct {
		in          string
		wantType    string
		wantPayload string
	}{
		{`{"type":"response","data":"Hi"}`, "response", "Hi"},
		{`{"type":"status","data":"ack"}`, "status", "ack"},
		{`{"type":"tool_call","data":{"name":"x"}}`, "tool_call", `{"type":"tool_call","data":{"name":"x"}}`},
		{`plain text`, "", "plain text"},
	}
	for _, c := range cases {
		ft, p := framePayload([]byte(c.in))
		if ft != c.wantType || p != c.wantPayload {
			t.Errorf("framePayload(%q) = (%q,%q), want (%q,%q)", c.in, ft, p, c.wantType, c.wantPayload)
		}
	}
}

func TestCapPayload(t *testing.T) {
	if got, full := capPayload("short"); got != "short" || !full {
		t.Errorf("capPayload(short) = (%q,%v), want (short,true)", got, full)
	}
	long := make([]rune, maxPayloadStore+50)
	for i := range long {
		long[i] = 'x'
	}
	got, full := capPayload(string(long))
	if full {
		t.Errorf("capPayload(long) reported full")
	}
	if n := len([]rune(got)); n != maxPayloadStore {
		t.Errorf("capPayload(long) kept %d runes, want %d", n, maxPayloadStore)
	}
}

// In feed focus, up/down scrub a packet cursor over the visible events. Moving
// up off the newest freezes the live tail; returning to the newest resumes it.
func TestFeedFreezeResume(t *testing.T) {
	d := newDashView(false)
	for i := 0; i < 5; i++ { // ids 0..4, all visible (fkPrompt)
		d.feed.push(feedLine{kind: fkPrompt, key: "a", typ: itoa(i)})
	}
	d.setFocus(focusFeed)
	if !d.followLive || d.cursorID != 4 {
		t.Fatalf("after focus: followLive=%v cursorID=%d, want true and 4", d.followLive, d.cursorID)
	}
	d.move(-1) // up: freeze at id 3
	if d.followLive || d.cursorID != 3 {
		t.Errorf("after up: followLive=%v cursorID=%d, want false and 3", d.followLive, d.cursorID)
	}
	d.move(-1) // up again: id 2
	if d.cursorID != 2 {
		t.Errorf("after up x2: cursorID=%d, want 2", d.cursorID)
	}
	d.move(1) // down: id 3 (still frozen)
	if d.followLive || d.cursorID != 3 {
		t.Errorf("after down: followLive=%v cursorID=%d, want false and 3", d.followLive, d.cursorID)
	}
	d.move(1) // down to newest: resumes live
	if !d.followLive || d.cursorID != 4 {
		t.Errorf("back at newest: followLive=%v cursorID=%d, want true and 4", d.followLive, d.cursorID)
	}
	d.moveEdge(true) // g: oldest, frozen
	if d.followLive || d.cursorID != 0 {
		t.Errorf("moveEdge(top): followLive=%v cursorID=%d, want false and 0", d.followLive, d.cursorID)
	}
	d.moveEdge(false) // G: newest, live
	if !d.followLive || d.cursorID != 4 {
		t.Errorf("moveEdge(bottom): followLive=%v cursorID=%d, want true and 4", d.followLive, d.cursorID)
	}
}

// A hidden (muted/hb-off) event is skipped by cursor navigation, and the live
// cursor resolves to the newest VISIBLE event.
func TestFeedNavSkipsHidden(t *testing.T) {
	d := newDashView(false)
	d.feed.push(feedLine{kind: fkPrompt, key: "a", typ: "p1"}) // id 0 visible
	d.feed.push(feedLine{kind: fkHB, key: "a", typ: "hb"})     // id 1 hidden when hb off
	d.feed.push(feedLine{kind: fkPrompt, key: "a", typ: "p2"}) // id 2 visible
	d.showHB = false
	d.setFocus(focusFeed)
	if id, ok := d.effectiveCursorLocked(); !ok || id != 2 {
		t.Fatalf("effective cursor = (%d,%v), want 2,true", id, ok)
	}
	d.move(-1) // skip the hidden hb -> land on id 0
	if d.cursorID != 0 {
		t.Errorf("after up: cursorID=%d, want 0 (hb skipped)", d.cursorID)
	}
}

func itoa(i int) string { return string(rune('0' + i)) }

// The packet-detail pane must show a PROMPT's subject and reply inbox IN FULL
// (only the payload may be shortened) — the headline requirement.
func TestPacketDetailPromptFull(t *testing.T) {
	d := seededDash() // pushes ONLINE(0), hb(1), PROMPT(2), status(3), resp(4), END(5), DONE(6)
	d.focus = focusFeed
	d.followLive = false
	d.cursorID = 2 // the PROMPT event

	d.mu.Lock()
	lines := d.packetDetailLinesLocked(computeLayout(100, 30))
	d.mu.Unlock()
	joined := strings.Join(lines, "\n")

	for _, want := range []string{
		"PACKET ",                             // header
		"agents.prompt.hermes.rs.miami",       // full subject
		"_INBOX.agents.ABCDEF1234567890GHIJK", // full reply inbox (not shortened)
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("packet detail missing %q", want)
		}
	}
	if strings.Contains(joined, "_IN..") {
		t.Errorf("reply inbox was shortened in the detail pane; want full text")
	}
}

// A REPLY packet's detail must identify the specific agent: full identity, the
// instance id ("agent id"), and the originating prompt subject — not just "pi".
func TestPacketDetailReplyShowsAgent(t *testing.T) {
	d := seededDash() // resp 2 is event id 4
	d.focus = focusFeed
	d.followLive = false
	d.cursorID = 4

	d.mu.Lock()
	lines := d.packetDetailLinesLocked(computeLayout(100, 30))
	d.mu.Unlock()
	joined := strings.Join(lines, "\n")

	for _, want := range []string{
		"hermes/rs (miami)",                   // full agent identity
		"ABC123XYZ789",                        // instance id (looked up from the roster)
		"agents.prompt.hermes.rs.miami",       // originating prompt subject
		"_INBOX.agents.ABCDEF1234567890GHIJK", // full reply inbox
	} {
		if !strings.Contains(joined, want) {
			t.Errorf("reply packet detail missing %q", want)
		}
	}
}
