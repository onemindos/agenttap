package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// fixedTS is the timestamp every line in the test stream should carry.
const fixedTS = "2026-05-27T12:00:00Z"

func newTestJSONView(buf *bytes.Buffer) *jsonView {
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	clock := func() time.Time { return time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC) }
	return &jsonView{enc: enc, clock: clock}
}

func decodeLine(t *testing.T, line string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("invalid JSON line %q: %v", line, err)
	}
	return m
}

func TestJSONViewStream(t *testing.T) {
	var buf bytes.Buffer
	v := newTestJSONView(&buf)

	v.banner()
	v.online(heartbeatPayload{Agent: "cc", Owner: "m64", Session: "s1", InstanceID: "i1",
		Subject: "agents.hb.cc.m64.s1", Raw: []byte(`{"agent":"cc","owner":"m64","session":"s1","instance_id":"i1","interval_s":30}`)})
	v.heartbeat(heartbeatPayload{Agent: "cc", Owner: "m64", InstanceID: "i1"}) // no session, no subject/raw
	m := &nats.Msg{
		Subject: "agents.prompt.cc.m64.s1",
		Reply:   "_INBOX.x",
		Header:  nats.Header{"Key": []string{"val"}},
	}
	pm := promptMeta{agent: "cc", owner: "m64", session: "s1", parsed: true}
	att := attachment{Filename: "a.txt", Content: base64.StdEncoding.EncodeToString([]byte("hi"))}
	v.prompt(7, "cc/m64 (s1)", pm, m, "hello <b>", []attachment{att})
	v.chunk(7, "cc/m64 (s1)", 1, &nats.Msg{Subject: "_INBOX.x", Data: []byte("world")}) // arrives on the reply inbox; no headers
	v.sentinel(7, "cc/m64 (s1)", &nats.Msg{})
	v.done(7, "cc/m64 (s1)", 1, doneSentinel)
	v.warnf("boom %d", 3)

	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	want := []string{"meta", "online", "heartbeat", "prompt", "chunk", "sentinel", "done", "warn"}
	if len(lines) != len(want) {
		t.Fatalf("got %d lines, want %d:\n%s", len(lines), len(want), buf.String())
	}

	// Every line is valid JSON and carries its type + the stamped ts.
	objs := make([]map[string]any, len(lines))
	for i, ln := range lines {
		objs[i] = decodeLine(t, ln)
		if objs[i]["type"] != want[i] {
			t.Errorf("line %d type = %v, want %s", i, objs[i]["type"], want[i])
		}
		if objs[i]["ts"] != fixedTS {
			t.Errorf("line %d ts = %v, want %s", i, objs[i]["ts"], fixedTS)
		}
	}

	if meta := objs[0]; meta["app"] != appName || meta["schema"] != float64(jsonSchema) {
		t.Errorf("meta = %v, want app=%s schema=%d", meta, appName, jsonSchema)
	}

	// online keeps the session; heartbeat with an empty session omits the field.
	if _, ok := objs[1]["session"]; !ok {
		t.Errorf("online should carry session: %v", objs[1])
	}
	if _, ok := objs[2]["session"]; ok {
		t.Errorf("heartbeat should omit empty session: %v", objs[2])
	}

	// "emit everything": online carries its hb subject (the routing identity) + raw frame;
	// a beacon with neither omits those fields.
	if objs[1]["subject"] != "agents.hb.cc.m64.s1" {
		t.Errorf("online should carry its hb subject: %v", objs[1])
	}
	if objs[1]["data"] == nil || objs[1]["bytes"] == nil {
		t.Errorf("online should carry the raw heartbeat frame + size: %v", objs[1])
	}
	if _, ok := objs[2]["subject"]; ok {
		t.Errorf("heartbeat without a subject should omit the field: %v", objs[2])
	}

	p := objs[3]
	if p["seq"] != float64(7) || p["subject"] != "agents.prompt.cc.m64.s1" || p["reply"] != "_INBOX.x" {
		t.Errorf("prompt core fields wrong: %v", p)
	}
	if p["text"] != "hello <b>" {
		t.Errorf("prompt text = %v, want literal 'hello <b>'", p["text"])
	}
	atts, ok := p["attachments"].([]any)
	if !ok || len(atts) != 1 {
		t.Fatalf("prompt attachments = %v, want one", p["attachments"])
	}
	a0 := atts[0].(map[string]any)
	if a0["filename"] != "a.txt" || a0["bytes"] != float64(2) {
		t.Errorf("attachment meta = %v, want a.txt / 2 (decoded size)", a0)
	}
	if _, leaked := a0["content"]; leaked {
		t.Errorf("attachment must carry metadata only, not content bytes: %v", a0)
	}
	// The raw-frame body is emitted too, but attachment bytes are elided there as well —
	// base64("hi") == "aGk=" must never reach the stream.
	if data, _ := p["data"].(string); !strings.Contains(data, "a.txt") || !strings.Contains(data, "elided") {
		t.Errorf("prompt should carry a raw body with attachment content elided: %v", p["data"])
	} else if strings.Contains(data, "aGk=") {
		t.Errorf("prompt raw body leaked attachment bytes: %v", data)
	}

	ch := objs[4]
	if ch["n"] != float64(1) || ch["bytes"] != float64(5) || ch["data"] != "world" {
		t.Errorf("chunk fields wrong: %v", ch)
	}
	if ch["subject"] != "_INBOX.x" {
		t.Errorf("chunk should carry the reply inbox subject, got %v", ch["subject"])
	}
	if _, ok := ch["headers"]; ok {
		t.Errorf("chunk with no headers should omit the field: %v", ch)
	}

	// sentinel carries no chunks/reason; done carries both (chunks even when 0).
	if _, ok := objs[5]["chunks"]; ok {
		t.Errorf("sentinel should not carry chunks: %v", objs[5])
	}
	if d := objs[6]; d["chunks"] != float64(1) || d["reason"] != doneSentinel {
		t.Errorf("done fields wrong: %v", d)
	}
	if objs[7]["msg"] != "boom 3" {
		t.Errorf("warn msg = %v, want 'boom 3'", objs[7]["msg"])
	}

	// Escaping decision: < and > are not \u-escaped in the raw stream.
	if !strings.Contains(buf.String(), "hello <b>") {
		t.Errorf("expected unescaped 'hello <b>' in stream; HTML escaping should be off")
	}
}
