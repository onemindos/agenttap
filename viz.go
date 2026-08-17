package main

// viz.go implements the "viz" view: a continuously scrolling protocol
// visualizer. Every message on the bus is rendered as an ASCII packet card
// (prompt / chunk / end-of-stream), heartbeats scroll as one dim line per beat,
// and when a reply stream finishes the accumulated chunks are assembled into a
// chat-style bubble exchange. The chrome is deliberately ASCII-forward; color
// (24-bit truecolor) does the visual work and degrades cleanly to plain text
// when disabled, so the output stays pipeable.

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"

	"github.com/nats-io/nats.go"
)

// rgb builds a 24-bit foreground SGR escape.
func rgb(r, g, b int) string { return fmt.Sprintf("\033[38;2;%d;%d;%dm", r, g, b) }

// Palette. Tuned for a dark terminal; only emitted when color is enabled.
var (
	colPrompt = rgb(240, 170, 70)  // prompts / user bubble
	colChunk  = rgb(80, 205, 200)  // reply chunks / agent bubble
	colEnd    = rgb(232, 120, 120) // end-of-stream / abnormal finish
	colOnline = rgb(120, 215, 140) // agent first-seen / clean finish
	colHb     = rgb(150, 140, 205) // heartbeat marker
	colBorder = rgb(95, 105, 125)  // box chrome
	colLabel  = rgb(135, 140, 158) // field labels
	colMeta   = rgb(120, 126, 142) // timestamps / sizes / meta
	colText   = rgb(226, 229, 236) // payload text
	colWarn   = rgb(240, 100, 100) // warnings
	colDim    = rgb(90, 96, 114)   // faint separators
)

// paint applies a color code if enabled.
type paint struct{ on bool }

func (p paint) c(code, s string) string {
	if !p.on || code == "" {
		return s
	}
	return code + s + cReset
}

// --- box drawing ------------------------------------------------------------

// boxStyle is the set of glyphs used to draw a frame. Packet cards use square
// corners; conversation bubbles use rounded-ASCII corners to read differently.
type boxStyle struct {
	tl, tr, bl, br, h, v, ml, mr string
}

var (
	cardStyle   = boxStyle{tl: "+", tr: "+", bl: "+", br: "+", h: "-", v: "|", ml: "+", mr: "+"}
	bubbleStyle = boxStyle{tl: ".", tr: ".", bl: "'", br: "'", h: "-", v: "|", ml: "+", mr: "+"}
)

// seg is one segment of a rendered line. Exactly one segment per line may be a
// "fill" segment, which expands with fillCh to pad the line to the box width.
type seg struct {
	s      string
	col    string
	fill   bool
	fillCh rune
}

type box struct {
	w    int
	pt   paint
	st   boxStyle
	bcol string
	out  []string
}

func newBox(w int, pt paint, st boxStyle, border string) *box {
	return &box{w: w, pt: pt, st: st, bcol: border}
}

func (b *box) hch() rune { return rune(b.st.h[0]) }

// line renders segments, expanding the fill segment so the whole line occupies
// exactly b.w display columns.
func (b *box) line(segs ...seg) {
	fixed := 0
	for _, s := range segs {
		if !s.fill {
			fixed += dispWidth(s.s)
		}
	}
	n := b.w - fixed
	if n < 0 {
		n = 0
	}
	var sb strings.Builder
	for _, s := range segs {
		if s.fill {
			sb.WriteString(b.pt.c(s.col, strings.Repeat(string(s.fillCh), n)))
		} else {
			sb.WriteString(b.pt.c(s.col, s.s))
		}
	}
	b.out = append(b.out, sb.String())
}

// top draws a titled top border: "+- TAG ------- meta -+".
func (b *box) top(tag, meta, tagCol string) {
	segs := []seg{
		{s: b.st.tl + b.st.h + " ", col: b.bcol},
		{s: tag + " ", col: tagCol},
		{fill: true, fillCh: b.hch(), col: b.bcol},
	}
	if meta != "" {
		segs = append(segs,
			seg{s: " ", col: b.bcol},
			seg{s: meta, col: colMeta},
			seg{s: " " + b.st.h + b.st.tr, col: b.bcol})
	} else {
		segs = append(segs, seg{s: b.st.h + b.st.tr, col: b.bcol})
	}
	b.line(segs...)
}

func (b *box) plainTop() {
	b.line(seg{s: b.st.tl, col: b.bcol}, seg{fill: true, fillCh: b.hch(), col: b.bcol}, seg{s: b.st.tr, col: b.bcol})
}

func (b *box) sep() {
	b.line(seg{s: b.st.ml, col: b.bcol}, seg{fill: true, fillCh: b.hch(), col: b.bcol}, seg{s: b.st.mr, col: b.bcol})
}

func (b *box) bottom() {
	b.line(seg{s: b.st.bl, col: b.bcol}, seg{fill: true, fillCh: b.hch(), col: b.bcol}, seg{s: b.st.br, col: b.bcol})
}

// row renders a single (already width-bounded) content line.
func (b *box) row(content, col string) {
	b.line(
		seg{s: b.st.v + " ", col: b.bcol},
		seg{s: content, col: col},
		seg{fill: true, fillCh: ' '},
		seg{s: " " + b.st.v, col: b.bcol},
	)
}

// wrapRow word-wraps text to the inner width and emits one row per line.
func (b *box) wrapRow(text, col string) {
	for _, ln := range wrapText(text, b.w-4) {
		b.row(ln, col)
	}
}

// field renders "label  value", wrapping the value with a hanging indent.
func (b *box) field(label, value, valCol string) {
	inner := b.w - 4
	lw := 8
	if d := dispWidth(label); d > lw {
		lw = d
	}
	avail := inner - lw - 1
	if avail < 4 {
		avail = 4
	}
	rows := wrapText(value, avail)
	if len(rows) == 0 {
		rows = []string{""}
	}
	for i, r := range rows {
		lab := ""
		if i == 0 {
			lab = label
		}
		b.line(
			seg{s: b.st.v + " ", col: b.bcol},
			seg{s: padRight(lab, lw), col: colLabel},
			seg{s: " ", col: ""},
			seg{s: r, col: valCol},
			seg{fill: true, fillCh: ' '},
			seg{s: " " + b.st.v, col: b.bcol},
		)
	}
}

// --- viz printer ------------------------------------------------------------

// convo accumulates one prompt and its streamed reply for the closing bubble.
type convo struct {
	seq                   uint64
	label                 string
	agent, owner, session string
	parsed                bool
	promptText            string
	atts                  []attachment
	started               time.Time
	chunks                int
	bytesN                int             // total reply wire bytes
	textFrames            int             // chunks that contributed reply text
	ctrlFrames            int             // control frames (status/ack/…), not shown as prose
	prose                 strings.Builder // assembled human-readable reply
}

// accumulate folds one reply chunk into the conversation. Agents stream their
// reply as a sequence of JSON frames ({"type":"response","data":"<delta>"}),
// interleaved with control frames ({"type":"status","data":"ack"}). For the
// bubble we want the message, so we keep the data of text frames and count the
// rest. A chunk that isn't a typed frame is treated as raw reply text.
func (c *convo) accumulate(data []byte) {
	if typ, text, dataIsString, ok := parseFrame(data); ok {
		if dataIsString && !ctrlFrameTypes[typ] {
			c.prose.WriteString(text)
			c.textFrames++
		} else {
			c.ctrlFrames++
		}
		return
	}
	c.prose.Write(data)
	c.textFrames++
}

// ctrlFrameTypes are reply-frame types that carry protocol control signals
// rather than user-facing text, so they're excluded from the assembled bubble.
var ctrlFrameTypes = map[string]bool{
	"status": true, "error": true, "ack": true, "ping": true,
	"start": true, "done": true, "end": true, "usage": true,
	"metadata": true, "tool_call": true, "tool_result": true,
}

// parseFrame decodes an agent SDK reply frame {"type":...,"data":...}. It
// returns the type, the data decoded as a string (when data is a string),
// whether data was a string, and whether the bytes were a typed frame at all.
func parseFrame(b []byte) (typ, data string, dataIsString, ok bool) {
	var f struct {
		Type string          `json:"type"`
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(b, &f); err != nil || f.Type == "" {
		return "", "", false, false
	}
	var s string
	if len(f.Data) > 0 && json.Unmarshal(f.Data, &s) == nil {
		return f.Type, s, true, true
	}
	return f.Type, "", false, true
}

type vizPrinter struct {
	mu       sync.Mutex
	pt       paint
	truncate int
	width    int
	convos   map[uint64]*convo
}

func newVizPrinter(color bool, truncate, width int) *vizPrinter {
	if width <= 0 {
		width = terminalWidth()
	}
	if width <= 0 {
		width = 100
	}
	width = min(max(width, 60), 160)
	return &vizPrinter{pt: paint{on: color}, truncate: truncate, width: width, convos: map[uint64]*convo{}}
}

// cardW caps packet-card width so very wide terminals stay readable.
func (v *vizPrinter) cardW() int { return min(v.width, 100) }

func (v *vizPrinter) flush(lines []string, indent int) {
	pad := strings.Repeat(" ", indent)
	for _, ln := range lines {
		fmt.Println(pad + ln)
	}
}

func (v *vizPrinter) banner() {
	v.mu.Lock()
	defer v.mu.Unlock()
	w := v.width
	fmt.Println()
	fmt.Println(v.gradientBar(w))
	fmt.Println(v.centered("A G E N T T A P   ::   live agent-protocol trace", w, colText, true))
	fmt.Println(v.gradientBar(w))
	legend := strings.Join([]string{
		v.pt.c(colPrompt, "[PROMPT] client>>agent"),
		v.pt.c(colChunk, "[CHUNK] agent>>client"),
		v.pt.c(colEnd, "[END] <empty>"),
		v.pt.c(colOnline, "[ONLINE]"),
		v.pt.c(colHb, "hb=heartbeat"),
	}, v.pt.c(colMeta, "   "))
	fmt.Println(v.centered(legend, w, colMeta, false))
	fmt.Println()
}

func (v *vizPrinter) online(hb heartbeatPayload) {
	v.mu.Lock()
	defer v.mu.Unlock()
	fmt.Println(strings.Join([]string{
		v.pt.c(colMeta, nowHMS()),
		v.pt.c(colOnline, "[ONLINE]"),
		v.pt.c(colText, agentLabel(hb)),
		v.pt.c(colMeta, "inst="+shortID(hb.InstanceID)),
	}, "  "))
}

func (v *vizPrinter) heartbeat(hb heartbeatPayload) {
	v.mu.Lock()
	defer v.mu.Unlock()
	fmt.Println(strings.Join([]string{
		v.pt.c(colMeta, nowHMS()),
		v.pt.c(colHb, "hb "),
		v.pt.c(colDim, agentLabel(hb)),
		v.pt.c(colDim, "inst="+shortID(hb.InstanceID)),
	}, "  "))
}

func (v *vizPrinter) prompt(seq uint64, label string, pm promptMeta, m *nats.Msg, text string, atts []attachment) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.convos[seq] = &convo{
		seq: seq, label: label,
		agent: pm.agent, owner: pm.owner, session: pm.session, parsed: pm.parsed,
		promptText: text, atts: atts, started: time.Now(),
	}

	b := newBox(v.cardW(), v.pt, cardStyle, colBorder)
	b.top("[PROMPT] client >> agent", idTag(seq)+"  "+nowHMS(), colPrompt)
	b.field("subject", m.Subject, colText)
	if m.Reply != "" {
		b.field("reply", m.Reply, colChunk)
	} else {
		b.field("reply", "(none -- fire-and-forget)", colEnd)
	}
	if h := headerString(m.Header); h != "" {
		b.field("headers", h, colMeta)
	}
	for _, a := range atts {
		b.field("attach", attOne(a), colText)
	}
	b.sep()
	body := text
	if strings.TrimSpace(body) == "" {
		body = "<empty>"
	}
	b.wrapRow(applyTrunc(sanitize(body), v.truncate), colText)
	b.bottom()
	v.flush(b.out, 0)
	fmt.Println()
}

func (v *vizPrinter) chunk(seq uint64, label string, n int, m *nats.Msg) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if c, ok := v.convos[seq]; ok {
		c.chunks = n
		c.bytesN += len(m.Data)
		c.accumulate(m.Data)
	}

	b := newBox(v.cardW(), v.pt, cardStyle, colBorder)
	meta := fmt.Sprintf("%s  %s  %s", idTag(seq), nowHMS(), humanBytes(len(m.Data)))
	b.top(fmt.Sprintf("[CHUNK %d] agent >> client", n), meta, colChunk)
	if h := headerString(m.Header); h != "" {
		b.field("headers", h, colMeta)
		b.sep()
	}
	body := string(m.Data)
	if strings.TrimSpace(body) == "" {
		body = "<empty>"
	}
	b.wrapRow(applyTrunc(sanitize(body), v.truncate), colText)
	b.bottom()
	v.flush(b.out, 0)
	fmt.Println()
}

func (v *vizPrinter) sentinel(seq uint64, label string, m *nats.Msg) {
	v.mu.Lock()
	defer v.mu.Unlock()
	b := newBox(v.cardW(), v.pt, cardStyle, colBorder)
	b.top("[END] agent >> client", idTag(seq)+"  "+nowHMS(), colEnd)
	b.field("subject", m.Subject, colChunk)
	b.field("data", "<empty>", colEnd)
	b.field("note", "stream terminator -- 0 bytes, no headers", colMeta)
	b.bottom()
	v.flush(b.out, 0)
	fmt.Println()
}

func (v *vizPrinter) queryReply(seq uint64, label, qid, subject, prompt, answer string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	b := newBox(v.cardW(), v.pt, cardStyle, colBorder)
	b.top("[QUERY] client >> agent", idTag(seq)+"  "+nowHMS(), colHeartWarn)
	b.field("reply", subject, colChunk)
	b.field("ask", applyTrunc(sanitize(prompt), v.truncate), colMeta)
	b.sep()
	b.wrapRow(applyTrunc(sanitize(answer), v.truncate), colText)
	b.bottom()
	v.flush(b.out, 0)
	fmt.Println()
}

func (v *vizPrinter) done(seq uint64, label string, chunks int, reason string) {
	v.mu.Lock()
	defer v.mu.Unlock()
	c := v.convos[seq]
	delete(v.convos, seq)
	v.renderBubble(seq, label, c, chunks, reason)
}

func (v *vizPrinter) warnf(format string, args ...any) {
	v.mu.Lock()
	defer v.mu.Unlock()
	fmt.Println(v.pt.c(colWarn, "!! WARN  "+fmt.Sprintf(format, args...)))
}

// stop is a no-op: the viz view scrolls normally and owns no screen state.
func (v *vizPrinter) stop() {}

// renderBubble draws the assembled exchange: a left-aligned user bubble (the
// prompt) and a right-aligned agent bubble (the concatenated reply chunks),
// followed by a stats footer.
func (v *vizPrinter) renderBubble(seq uint64, label string, c *convo, chunks int, reason string) {
	w := v.width
	bw := min(max(w*7/10, 36), 84)
	if bw > w {
		bw = w
	}

	fmt.Println(v.rule(w, fmt.Sprintf(" exchange %s : %s ", idTag(seq), label)))

	ub := newBox(bw, v.pt, bubbleStyle, colPrompt)
	ub.plainTop()
	ub.row("[USER] "+userTag(c, label), colPrompt)
	ub.sep()
	ptext := "<no prompt captured>"
	if c != nil && strings.TrimSpace(c.promptText) != "" {
		ptext = c.promptText
	}
	ub.wrapRow(applyTrunc(sanitize(ptext), v.truncate), colText)
	if c != nil {
		for _, a := range c.atts {
			ub.row("attach: "+attOne(a), colMeta)
		}
	}
	ub.bottom()
	v.flush(ub.out, 0)

	ab := newBox(bw, v.pt, bubbleStyle, colChunk)
	ab.plainTop()
	ab.row("[AGENT] "+agentTag(c, label), colChunk)
	ab.sep()
	atext := "<no reply received>"
	if c != nil {
		switch {
		case c.prose.Len() > 0:
			atext = c.prose.String()
		case c.ctrlFrames > 0:
			atext = fmt.Sprintf("<no text frames; %d control frame(s)>", c.ctrlFrames)
		}
	}
	ab.wrapRow(applyTrunc(sanitize(atext), v.truncate), colText)
	ab.bottom()
	indent := max(w-bw, 0)
	v.flush(ab.out, indent)

	rcol := colOnline
	if reason != doneSentinel {
		rcol = colEnd
	}
	bytesN, dur := 0, ""
	framesLabel := fmt.Sprintf("%d chunks", chunks)
	if c != nil {
		bytesN = c.bytesN
		dur = humanDur(time.Since(c.started))
		if c.ctrlFrames > 0 {
			framesLabel = fmt.Sprintf("%d frames (%d text, %d ctrl)", chunks, c.textFrames, c.ctrlFrames)
		}
	}
	stat := strings.Join([]string{
		v.pt.c(colMeta, framesLabel),
		v.pt.c(colMeta, humanBytes(bytesN)),
		v.pt.c(colMeta, dur),
		v.pt.c(rcol, reason),
	}, v.pt.c(colDim, " | "))
	fmt.Println(strings.Repeat(" ", indent) + stat)
	fmt.Println()
}

func userTag(c *convo, label string) string {
	if c != nil && c.parsed {
		s := c.owner
		if c.session != "" {
			s += " (" + c.session + ")"
		}
		return s + " -> " + c.agent
	}
	return label
}

func agentTag(c *convo, label string) string {
	if c != nil && c.parsed {
		if c.session != "" {
			return c.agent + " (" + c.session + ")"
		}
		return c.agent
	}
	return label
}

// gradientBar draws a full-width rule shading violet -> teal (or plain '=' when
// color is off).
func (v *vizPrinter) gradientBar(w int) string {
	if !v.pt.on {
		return strings.Repeat("=", w)
	}
	r0, g0, b0 := 150, 140, 205
	r1, g1, b1 := 80, 205, 200
	den := float64(max(1, w-1))
	var sb strings.Builder
	for i := 0; i < w; i++ {
		t := float64(i) / den
		sb.WriteString(rgb(
			int(float64(r0)+t*float64(r1-r0)),
			int(float64(g0)+t*float64(g1-g0)),
			int(float64(b0)+t*float64(b1-b0)),
		) + "=")
	}
	sb.WriteString(cReset)
	return sb.String()
}

func (v *vizPrinter) centered(s string, w int, col string, bold bool) string {
	pad := max((w-dispWidth(s))/2, 0)
	body := v.pt.c(col, s)
	if bold && v.pt.on {
		body = cBold + body
	}
	return strings.Repeat(" ", pad) + body
}

func (v *vizPrinter) rule(w int, label string) string {
	lw := dispWidth(label)
	left := max((w-lw)/2, 2)
	right := max(w-left-lw, 2)
	return v.pt.c(colBorder, strings.Repeat("-", left)) +
		v.pt.c(colMeta, label) +
		v.pt.c(colBorder, strings.Repeat("-", right))
}

// --- text + width helpers ---------------------------------------------------

func nowHMS() string { return time.Now().Format("15:04:05.000") }

func humanDur(d time.Duration) string {
	switch {
	case d < time.Millisecond:
		return fmt.Sprintf("%dus", d.Microseconds())
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Milliseconds())
	default:
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
}

func applyTrunc(s string, n int) string {
	if n > 0 && len(s) > n {
		return s[:n] + fmt.Sprintf("  ... +%d bytes", len(s)-n)
	}
	return s
}

func shortID(s string) string {
	if len(s) <= 12 {
		return s
	}
	return s[:10] + ".."
}

// sanitize neutralizes control bytes so payloads can't corrupt the layout or
// inject their own ANSI escapes. Newlines are kept; tabs become spaces.
func sanitize(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch {
		case r == '\n':
			b.WriteByte('\n')
		case r == '\t':
			b.WriteString("    ")
		case r == 0x1b: // ESC: drop so payload can't smuggle in escape sequences
			b.WriteByte('.')
		case r < 0x20 || (r >= 0x7f && r < 0xa0):
			b.WriteByte('.')
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// dispWidth is the terminal display width of s, skipping ANSI SGR sequences and
// accounting for wide (CJK/emoji) runes.
func dispWidth(s string) int {
	w, inEsc := 0, false
	for _, r := range s {
		if inEsc {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
				inEsc = false
			}
			continue
		}
		if r == 0x1b {
			inEsc = true
			continue
		}
		w += runeWidth(r)
	}
	return w
}

func runeWidth(r rune) int {
	switch {
	case r == 0:
		return 0
	case r == 0x200d || r == 0xfe0f || r == 0xfe0e: // ZWJ / variation selectors
		return 0
	case r >= 0x0300 && r <= 0x036f, r >= 0x1ab0 && r <= 0x1aff,
		r >= 0x1dc0 && r <= 0x1dff, r >= 0x20d0 && r <= 0x20ff,
		r >= 0xfe20 && r <= 0xfe2f: // combining marks
		return 0
	case r < 0x20 || (r >= 0x7f && r < 0xa0): // control (sanitized out anyway)
		return 0
	}
	switch {
	case r >= 0x1100 && r <= 0x115f, // Hangul Jamo
		r >= 0x2e80 && r <= 0x303e,   // CJK radicals .. Kangxi
		r >= 0x3041 && r <= 0x33ff,   // Hiragana .. CJK symbols
		r >= 0x3400 && r <= 0x4dbf,   // CJK ext A
		r >= 0x4e00 && r <= 0x9fff,   // CJK unified
		r >= 0xa000 && r <= 0xa4cf,   // Yi
		r >= 0xac00 && r <= 0xd7a3,   // Hangul syllables
		r >= 0xf900 && r <= 0xfaff,   // CJK compat
		r >= 0xfe30 && r <= 0xfe4f,   // CJK compat forms
		r >= 0xff00 && r <= 0xff60,   // fullwidth forms
		r >= 0xffe0 && r <= 0xffe6,   // fullwidth signs
		r >= 0x1f300 && r <= 0x1faff, // emoji & pictographs
		r >= 0x1f000 && r <= 0x1f02f, // mahjong/dominoes
		r >= 0x1f900 && r <= 0x1f9ff, // supplemental symbols
		r >= 0x1f1e6 && r <= 0x1f1ff: // regional indicators
		return 2
	}
	return 1
}

func padRight(s string, w int) string {
	if d := dispWidth(s); d < w {
		return s + strings.Repeat(" ", w-d)
	}
	return s
}

// takeCells returns the longest prefix of s whose display width is <= w.
func takeCells(s string, w int) string {
	used := 0
	var b strings.Builder
	for _, r := range s {
		rw := runeWidth(r)
		if used+rw > w {
			break
		}
		b.WriteRune(r)
		used += rw
	}
	return b.String()
}

// wrapText word-wraps s to width w (display columns), preserving existing
// newlines and hard-breaking words longer than w.
func wrapText(s string, w int) []string {
	if w < 1 {
		w = 1
	}
	var lines []string
	for _, para := range strings.Split(s, "\n") {
		words := strings.Fields(para)
		if len(words) == 0 {
			lines = append(lines, "")
			continue
		}
		cur, curw := "", 0
		for _, word := range words {
			ww := dispWidth(word)
			if ww > w { // hard-break an over-long token
				if curw > 0 {
					lines = append(lines, cur)
					cur, curw = "", 0
				}
				for dispWidth(word) > w {
					part := takeCells(word, w)
					if part == "" { // a single rune wider than w; force progress
						part = string([]rune(word)[:1])
					}
					lines = append(lines, part)
					word = word[len(part):]
				}
				cur, curw = word, dispWidth(word)
				continue
			}
			switch {
			case curw == 0:
				cur, curw = word, ww
			case curw+1+ww <= w:
				cur, curw = cur+" "+word, curw+1+ww
			default:
				lines = append(lines, cur)
				cur, curw = word, ww
			}
		}
		lines = append(lines, cur)
	}
	return lines
}

// terminalSize returns the (rows, cols) of stdout, or (0, 0) if it isn't a
// terminal, by reading the kernel window size via the TIOCGWINSZ ioctl.
func terminalSize() (rows, cols int) {
	type winsize struct{ row, col, xpix, ypix uint16 }
	ws := &winsize{}
	_, _, errno := syscall.Syscall(
		syscall.SYS_IOCTL,
		os.Stdout.Fd(),
		uintptr(syscall.TIOCGWINSZ),
		uintptr(unsafe.Pointer(ws)),
	)
	if errno != 0 {
		return 0, 0
	}
	return int(ws.row), int(ws.col)
}

// terminalWidth returns the column count of stdout, or 0 if it isn't a terminal.
func terminalWidth() int {
	_, cols := terminalSize()
	return cols
}

// --- demo -------------------------------------------------------------------

// runDemo plays a scripted exchange through the view so the visualizer can be
// rehearsed (and tested) without a live NATS connection. Small pauses make it
// feel like real traffic.
func runDemo(out view) {
	pause := func() { time.Sleep(420 * time.Millisecond) }

	hermes := heartbeatPayload{Agent: "hermes", Owner: "rs", InstanceID: "P6jQnJ56tGj61Ct0vj0KOQ", Session: "miami-demo", IntervalS: 30, Subject: "agents.hb.hermes.rs.miami-demo"}
	atlas := heartbeatPayload{Agent: "atlas", Owner: "rs", InstanceID: "Z1kFb9rNq2Wx", Session: "ops", IntervalS: 15, Subject: "agents.hb.atlas.rs.ops"}
	// the raw beacon body the inspector shows (the §8.3 payload; Subject/Raw are json:"-")
	hermes.Raw, _ = json.Marshal(hermes)
	atlas.Raw, _ = json.Marshal(atlas)

	out.online(hermes)
	out.heartbeat(hermes)
	pause()
	out.online(atlas)
	out.heartbeat(atlas)
	pause()

	const reply = "_INBOX.agents.FYFA2ZE57WAKEQRTQGNFGI"
	m := &nats.Msg{
		Subject: "agents.prompt.hermes.rs.miami-demo",
		Reply:   reply,
		Header: nats.Header{
			"Nats-Msg-Id":      []string{"01J8Z3K9QWERTYUIOP"},
			"X-Agent-Protocol": []string{"0.3"},
		},
	}
	pm := promptMeta{agent: "hermes", owner: "rs", session: "miami-demo", parsed: true}
	atts := []attachment{{Filename: "telemetry.json", Content: base64.StdEncoding.EncodeToString(make([]byte, 2412))}}
	out.prompt(1, "hermes/rs (miami-demo)", pm, m,
		"What's the weather in Miami for the F1 race this weekend, and is rain likely during the session?", atts)
	pause()

	// Agents stream framed deltas: a status ack, then response chunks whose
	// "data" fields concatenate into the reply.
	const queryReplySubj = "_INBOX.Xj7k9Q2pA"
	frames := []string{
		`{"type":"status","data":"ack"}`,
		`{"type":"response","data":"The Miami GP forecast looks great: "}`,
		`{"type":"response","data":"mostly sunny skies with highs around 30°C (86°F). "}`,
		`{"type":"query","data":{"id":"q-7f3a2b","reply_subject":"` + queryReplySubj + `","prompt":"Include the detailed hourly rain breakdown? (yes/no)"}}`,
		`{"type":"response","data":"Winds are light from the east at 10-15 km/h. "}`,
		`{"type":"response","data":"Rain probability during the race window is low, under 10%."}`,
	}
	for i, fr := range frames {
		out.chunk(1, "hermes/rs (miami-demo)", i+1, &nats.Msg{Subject: reply, Data: []byte(fr)})
		if i == 3 { // the caller answers the mid-stream query (§7)
			pause()
			out.queryReply(1, "hermes/rs (miami-demo)", "q-7f3a2b", queryReplySubj, "Include the detailed hourly rain breakdown? (yes/no)", "yes")
		}
		if i == 2 {
			out.heartbeat(atlas) // a heartbeat arriving mid-stream
		}
		pause()
	}
	out.sentinel(1, "hermes/rs (miami-demo)", &nats.Msg{Subject: reply})
	out.done(1, "hermes/rs (miami-demo)", len(frames), doneSentinel)
	out.heartbeat(hermes)
}
