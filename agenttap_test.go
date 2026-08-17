package main

import "testing"

func TestParsePromptSubject(t *testing.T) {
	cases := []struct {
		in                    string
		agent, owner, session string
		ok                    bool
	}{
		{"agents.prompt.hermes.rs.demo", "hermes", "rs", "demo", true},
		{"agents.prompt.pi.m64.m64", "pi", "m64", "m64", true},
		{"agents.prompt.a.b.c.d", "a", "b", "c.d", true}, // session keeps trailing tokens
		{"agents.prompt.a.b", "", "", "", false},         // too few tokens
		{"agents.prompt.a..c", "", "", "", false},        // empty token
		{"other.subject.x.y.z", "", "", "", false},       // wrong root
	}
	for _, c := range cases {
		a, o, s, ok := parsePromptSubject(c.in)
		if ok != c.ok || a != c.agent || o != c.owner || s != c.session {
			t.Errorf("parsePromptSubject(%q) = (%q,%q,%q,%v), want (%q,%q,%q,%v)",
				c.in, a, o, s, ok, c.agent, c.owner, c.session, c.ok)
		}
	}
}

func TestParseQueryFrame(t *testing.T) {
	qf, ok := parseQueryFrame([]byte(`{"type":"query","data":{"id":"q1","reply_subject":"_INBOX.x","prompt":"go?"}}`))
	if !ok || qf.Data.ID != "q1" || qf.Data.ReplySubject != "_INBOX.x" || qf.Data.Prompt != "go?" {
		t.Fatalf("parseQueryFrame(ok) = %+v, %v", qf, ok)
	}
	for _, bad := range []string{
		`{"type":"response","data":"hi"}`,             // not a query
		`{"type":"query","data":{"prompt":"no sub"}}`, // missing reply_subject
		`not json`,
	} {
		if _, ok := parseQueryFrame([]byte(bad)); ok {
			t.Errorf("parseQueryFrame(%q) = true, want false", bad)
		}
	}
}

func TestParseFrame(t *testing.T) {
	cases := []struct {
		in       string
		typ      string
		data     string
		isString bool
		ok       bool
	}{
		{`{"type":"response","data":"Hi"}`, "response", "Hi", true, true},
		{`{"type":"status","data":"ack"}`, "status", "ack", true, true},
		{`{"type":"tool_call","data":{"name":"x"}}`, "tool_call", "", false, true},
		{`{"foo":"bar"}`, "", "", false, false}, // no type
		{`not json`, "", "", false, false},
		{`"just a string"`, "", "", false, false},
	}
	for _, c := range cases {
		typ, data, isStr, ok := parseFrame([]byte(c.in))
		if typ != c.typ || data != c.data || isStr != c.isString || ok != c.ok {
			t.Errorf("parseFrame(%q) = (%q,%q,%v,%v), want (%q,%q,%v,%v)",
				c.in, typ, data, isStr, ok, c.typ, c.data, c.isString, c.ok)
		}
	}
}

func TestConvoAccumulate(t *testing.T) {
	var c convo
	c.accumulate([]byte(`{"type":"status","data":"ack"}`))
	c.accumulate([]byte(`{"type":"response","data":"Hi "}`))
	c.accumulate([]byte(`{"type":"response","data":"there"}`))
	c.accumulate([]byte(`{"type":"tool_call","data":{"name":"search"}}`))
	c.accumulate([]byte(`!`)) // non-framed chunk → treated as raw text

	if got, want := c.prose.String(), "Hi there!"; got != want {
		t.Errorf("prose = %q, want %q", got, want)
	}
	if c.textFrames != 3 { // two response frames + one raw chunk
		t.Errorf("textFrames = %d, want 3", c.textFrames)
	}
	if c.ctrlFrames != 2 { // status + tool_call
		t.Errorf("ctrlFrames = %d, want 2", c.ctrlFrames)
	}
}

func TestWrapText(t *testing.T) {
	cases := []struct {
		in   string
		w    int
		want []string
	}{
		{"a b c", 3, []string{"a b", "c"}},
		{"hello world", 5, []string{"hello", "world"}},
		{"abcdefgh", 3, []string{"abc", "def", "gh"}}, // hard-break long token
		{"keep\nlines", 20, []string{"keep", "lines"}},
		{"", 10, []string{""}},
	}
	for _, c := range cases {
		got := wrapText(c.in, c.w)
		if len(got) != len(c.want) {
			t.Errorf("wrapText(%q,%d) = %q, want %q", c.in, c.w, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("wrapText(%q,%d)[%d] = %q, want %q", c.in, c.w, i, got[i], c.want[i])
			}
		}
	}
}

func TestDispWidth(t *testing.T) {
	cases := []struct {
		in   string
		want int
	}{
		{"abc", 3},
		{"\033[31mabc\033[0m", 3}, // ANSI escapes are zero-width
		{"a你b", 4},                // 你 is a wide (2-cell) rune
		{"", 0},
	}
	for _, c := range cases {
		if got := dispWidth(c.in); got != c.want {
			t.Errorf("dispWidth(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestSanitize(t *testing.T) {
	cases := []struct{ in, want string }{
		{"plain", "plain"},
		{"a\tb", "a    b"}, // tab -> 4 spaces
		{"a\x1bb", "a.b"},  // ESC dropped (no ANSI injection)
		{"a\x07b", "a.b"},  // BEL -> dot
		{"keep\nnewline", "keep\nnewline"},
	}
	for _, c := range cases {
		if got := sanitize(c.in); got != c.want {
			t.Errorf("sanitize(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestHumanBytes(t *testing.T) {
	cases := []struct {
		in   int
		want string
	}{
		{0, "0 B"},
		{512, "512 B"},
		{1024, "1.0 KiB"},
		{1536, "1.5 KiB"},
		{1048576, "1.0 MiB"},
	}
	for _, c := range cases {
		if got := humanBytes(c.in); got != c.want {
			t.Errorf("humanBytes(%d) = %q, want %q", c.in, got, c.want)
		}
	}
}
