// JSON view: emits the observed protocol as newline-delimited JSON (NDJSON) on
// stdout, one self-describing event per line. Every line carries a "type"
// discriminator and an observation "ts", so a recorded stream can be replayed at
// its original pace. This is the machine-readable seam feeding cmd/agentweb
// (live or from a recorded file); see tmp/visualisation-html.md.
//
// Unlike the log view it never filters and never truncates: every heartbeat is
// emitted and payloads are carried in full. Sanitizing for display is the
// consumer's job (the browser), not this stream's.

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// jsonSchema versions the NDJSON event shape; it rides on the opening meta line
// so a consumer can verify the stream before parsing the rest.
const jsonSchema = 1

var _ view = (*jsonView)(nil)

// jsonView renders events as NDJSON to stdout. Its methods run on multiple
// goroutines (the prompt, heartbeat, and per-reply subscriptions), so the mutex
// serializes whole-line writes; the timestamp is stamped under the same lock, so
// it never runs backwards relative to write order.
type jsonView struct {
	mu    sync.Mutex
	enc   *json.Encoder
	clock func() time.Time
}

func newJSONView() *jsonView {
	enc := json.NewEncoder(os.Stdout)
	// Payloads are sanitized by the consumer at render time, not here, so there
	// is no reason to escape <, > and & as \u sequences: keep the lines readable.
	enc.SetEscapeHTML(false)
	return &jsonView{enc: enc, clock: time.Now}
}

// event is the common head of every line. Embedded into each concrete event so
// "type" and "ts" always lead the object; stamp fills "ts" at emit time.
type event struct {
	Type string `json:"type"`
	TS   string `json:"ts"`
}

func (e *event) stamp(ts string) { e.TS = ts }

// stamped is satisfied by a pointer to any event-embedding struct.
type stamped interface{ stamp(ts string) }

// emit stamps the observation time and writes one NDJSON line, serialized so
// concurrent handlers never interleave. A stdout write error (e.g. a broken pipe
// when the consumer goes away) is not actionable here, so it is dropped.
func (v *jsonView) emit(e stamped) {
	v.mu.Lock()
	defer v.mu.Unlock()
	e.stamp(v.clock().UTC().Format(time.RFC3339Nano))
	_ = v.enc.Encode(e)
}

func (v *jsonView) banner() {
	v.emit(&metaEvent{event: event{Type: "meta"}, App: appName, Schema: jsonSchema})
}

func (v *jsonView) online(hb heartbeatPayload)    { v.emit(newAgentEvent("online", hb)) }
func (v *jsonView) heartbeat(hb heartbeatPayload) { v.emit(newAgentEvent("heartbeat", hb)) }

func (v *jsonView) prompt(seq uint64, label string, pm promptMeta, m *nats.Msg, text string, atts []attachment) {
	v.emit(&promptEvent{
		event:       event{Type: "prompt"},
		Seq:         seq,
		Label:       label,
		Agent:       pm.agent,
		Owner:       pm.owner,
		Session:     pm.session,
		Subject:     m.Subject,
		Reply:       m.Reply,
		Headers:     m.Header,
		Text:        text,
		Attachments: attachmentMetas(atts),
		Data:        promptRawForStream(m, text, atts),
	})
}

func (v *jsonView) chunk(seq uint64, label string, n int, m *nats.Msg) {
	v.emit(&chunkEvent{
		event:   event{Type: "chunk"},
		Seq:     seq,
		Label:   label,
		Subject: m.Subject,
		N:       n,
		Bytes:   len(m.Data),
		Headers: m.Header,
		Data:    string(m.Data),
	})
}

func (v *jsonView) sentinel(seq uint64, label string, m *nats.Msg) {
	v.emit(&sentinelEvent{event: event{Type: "sentinel"}, Seq: seq, Label: label, Subject: m.Subject})
}

func (v *jsonView) done(seq uint64, label string, chunks int, reason string) {
	v.emit(&doneEvent{event: event{Type: "done"}, Seq: seq, Label: label, Chunks: chunks, Reason: reason})
}

func (v *jsonView) queryReply(seq uint64, label, qid, subject, prompt, answer string) {
	v.emit(&queryReplyEvent{event: event{Type: "query_reply"}, Seq: seq, Label: label, QID: qid, Subject: subject, Prompt: prompt, Answer: answer})
}

func (v *jsonView) warnf(format string, args ...any) {
	v.emit(&warnEvent{event: event{Type: "warn"}, Msg: fmt.Sprintf(format, args...)})
}

// stop is a no-op: the JSON view owns no terminal state, and stdout is unbuffered.
func (v *jsonView) stop() {}

// --- event shapes (mirror the view interface) -------------------------------

type metaEvent struct {
	event
	App    string `json:"app"`
	Schema int    `json:"schema"`
}

// agentEvent backs both "online" and "heartbeat" (identical wire fields).
type agentEvent struct {
	event
	Agent      string      `json:"agent,omitempty"`
	Owner      string      `json:"owner,omitempty"`
	Session    string      `json:"session,omitempty"`
	InstanceID string      `json:"instance_id"`
	HBTS       string      `json:"hb_ts,omitempty"`      // the agent's own publication timestamp (§8.3)
	IntervalS  float64     `json:"interval_s,omitempty"` // the agent's advertised cadence (§8.3)
	Subject    string      `json:"subject,omitempty"`    // agents.hb.{agent}.{owner}.{name} — carries the routing identity (§2, §8.1)
	Headers    nats.Header `json:"headers,omitempty"`
	Bytes      int         `json:"bytes,omitempty"`
	Data       string      `json:"data,omitempty"` // raw heartbeat payload, for the inspector's raw-frame view
}

func newAgentEvent(typ string, hb heartbeatPayload) *agentEvent {
	return &agentEvent{
		event:      event{Type: typ},
		Agent:      hb.Agent,
		Owner:      hb.Owner,
		Session:    hb.Session,
		InstanceID: hb.InstanceID,
		HBTS:       hb.TS,
		IntervalS:  hb.IntervalS,
		Subject:    hb.Subject,
		Headers:    hb.Headers,
		Bytes:      len(hb.Raw),
		Data:       string(hb.Raw),
	}
}

type promptEvent struct {
	event
	Seq         uint64           `json:"seq"`
	Label       string           `json:"label"`
	Agent       string           `json:"agent,omitempty"`
	Owner       string           `json:"owner,omitempty"`
	Session     string           `json:"session,omitempty"`
	Subject     string           `json:"subject"`
	Reply       string           `json:"reply"` // "" means fire-and-forget (no reply inbox)
	Headers     nats.Header      `json:"headers,omitempty"`
	Text        string           `json:"text"`
	Attachments []attachmentMeta `json:"attachments,omitempty"`
	Data        string           `json:"data,omitempty"` // raw request body (attachment bytes elided), for the raw-frame view
}

// attachmentMeta is metadata only: the agent SDK envelope carries base64 content,
// but the stream deliberately drops the bytes (they bloat recordings and the
// consumer shows name + size). Inline content can be added later behind a cap.
type attachmentMeta struct {
	Filename string `json:"filename"`
	Bytes    int    `json:"bytes"`
}

func attachmentMetas(atts []attachment) []attachmentMeta {
	if len(atts) == 0 {
		return nil
	}
	out := make([]attachmentMeta, len(atts))
	for i, a := range atts {
		out[i] = attachmentMeta{Filename: a.Filename, Bytes: attSize(a)}
	}
	return out
}

// promptRawForStream renders the request body for the inspector's raw-frame view. Like
// attachmentMetas, it never lets attachment *bytes* into the stream: any attachment
// `content` is elided to its size. Plain text and attachment-free JSON pass through
// verbatim; when there are no raw bytes on the wire (the scripted demo) it synthesizes a
// faithful envelope from the decoded text + attachment metadata.
func promptRawForStream(m *nats.Msg, text string, atts []attachment) string {
	if len(m.Data) > 0 {
		return elideAttachmentContent(m.Data)
	}
	if len(atts) == 0 {
		return text
	}
	metas := make([]map[string]string, len(atts))
	for i, a := range atts {
		metas[i] = map[string]string{"filename": a.Filename, "content": fmt.Sprintf("<%d bytes elided>", attSize(a))}
	}
	b, _ := json.Marshal(map[string]any{"prompt": text, "attachments": metas})
	return string(b)
}

// elideAttachmentContent replaces attachment `content` values with a size marker, leaving
// every other field (including unknown ones, §5.6) intact. Non-JSON or attachment-free
// bodies are returned unchanged.
func elideAttachmentContent(data []byte) string {
	var obj map[string]json.RawMessage
	if json.Unmarshal(data, &obj) != nil {
		return string(data)
	}
	raw, ok := obj["attachments"]
	if !ok {
		return string(data)
	}
	var atts []map[string]json.RawMessage
	if json.Unmarshal(raw, &atts) == nil {
		for _, a := range atts {
			if c, ok := a["content"]; ok {
				a["content"], _ = json.Marshal(fmt.Sprintf("<%d bytes elided>", len(c)))
			}
		}
		if b, err := json.Marshal(atts); err == nil {
			obj["attachments"] = b
		}
	}
	if b, err := json.Marshal(obj); err == nil {
		return string(b)
	}
	return string(data)
}

type chunkEvent struct {
	event
	Seq     uint64      `json:"seq"`
	Label   string      `json:"label"`
	Subject string      `json:"subject"` // reply inbox the chunk arrived on (§6.1)
	N       int         `json:"n"`
	Bytes   int         `json:"bytes"`
	Headers nats.Header `json:"headers,omitempty"`
	Data    string      `json:"data"`
}

type sentinelEvent struct {
	event
	Seq     uint64 `json:"seq"`
	Label   string `json:"label"`
	Subject string `json:"subject"` // reply inbox the terminator arrived on
}

type doneEvent struct {
	event
	Seq    uint64 `json:"seq"`
	Label  string `json:"label"`
	Chunks int    `json:"chunks"` // always present, even when 0
	Reason string `json:"reason"`
}

type warnEvent struct {
	event
	Msg string `json:"msg"`
}

// queryReplyEvent is the caller's answer to a mid-stream query (§7), paired to its
// query chunk by qid.
type queryReplyEvent struct {
	event
	Seq     uint64 `json:"seq"`
	Label   string `json:"label"`
	QID     string `json:"qid"`
	Subject string `json:"subject"` // the reply_subject the caller's answer was published to (§7)
	Prompt  string `json:"prompt"`
	Answer  string `json:"answer"`
}
