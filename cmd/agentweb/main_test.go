package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestHubBacklogThenStreamThenEOF(t *testing.T) {
	h := newHub("replay")
	h.publish("a")
	h.publish("b")

	backlog, ch, eof := h.subscribe()
	if eof {
		t.Fatal("eof set before finish()")
	}
	if len(backlog) != 2 || backlog[0] != "a" || backlog[1] != "b" {
		t.Fatalf("backlog = %v, want [a b]", backlog)
	}

	h.publish("c")
	if m := <-ch; m.line != "c" || m.eof {
		t.Fatalf("stream msg = %+v, want line c", m)
	}

	h.finish()
	if m := <-ch; !m.eof {
		t.Fatalf("final msg = %+v, want eof", m)
	}
}

// A line published in the window around subscribe() must be delivered exactly
// once: in the backlog OR on the channel, never both, never neither.
func TestHubExactlyOnceAcrossSubscribe(t *testing.T) {
	h := newHub("live")
	h.publish("x")

	backlog, ch, _ := h.subscribe()
	h.publish("y")

	if len(backlog) != 1 || backlog[0] != "x" {
		t.Fatalf("backlog = %v, want [x]", backlog)
	}
	select {
	case m := <-ch:
		if m.line != "y" {
			t.Fatalf("channel msg = %+v, want y", m)
		}
	case <-time.After(time.Second):
		t.Fatal("expected y on the channel")
	}
}

func TestHandleEventsSSE(t *testing.T) {
	h := newHub("replay")
	h.publish(`{"type":"meta","schema":1}`)
	h.finish()

	srv := httptest.NewServer(http.HandlerFunc(h.handleEvents))
	defer srv.Close()

	// The handler streams and then blocks on keepalive; cancel after a beat and
	// inspect what was written.
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}
	body, _ := io.ReadAll(resp.Body) // returns when ctx cancels; partial body is fine
	s := string(body)
	for _, want := range []string{`event: control`, `"mode":"replay"`, `data: {"type":"meta","schema":1}`, `"eof":true`} {
		if !strings.Contains(s, want) {
			t.Errorf("SSE output missing %q\n--- got ---\n%s", want, s)
		}
	}
}
