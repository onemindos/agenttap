// Command agentweb serves the agenttap "Stream" visualizer. It reads the NDJSON
// event stream produced by `agenttap -view json` — live on stdin, or from a file
// with -replay — and pushes it to the browser over Server-Sent Events, serving a
// single self-contained page.
//
// It is a pure presenter: it speaks no NATS (imports none) and writes nothing but
// the HTTP response. All the timeline/transport logic lives in the browser; the
// server is a dumb broadcast pipe with a replayable backlog so a page reload (or a
// second tab) still receives the full history before the live tail.
//
//	live:    agenttap -view json <ctx> | agentweb
//	replay:  agentweb -replay session.ndjson
//	canned:  agenttap -view json -demo | agentweb
package main

import (
	"bufio"
	"embed"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"
)

//go:embed index.html style.css app.js
var assets embed.FS

func main() {
	addr := flag.String("addr", "127.0.0.1:8088", "listen address; use 0.0.0.0:8088 to reach it from another machine")
	replay := flag.String("replay", "", "read the NDJSON stream from this file instead of stdin")
	open := flag.Bool("open", false, "open the page in a browser on start")
	flag.Parse()

	mode := "live"
	src := os.Stdin
	if *replay != "" {
		f, err := os.Open(*replay)
		if err != nil {
			fmt.Fprintln(os.Stderr, "agentweb:", err)
			os.Exit(1)
		}
		defer f.Close()
		src = f
		mode = "replay"
	}
	sc := bufio.NewScanner(src)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024) // payload lines can be large

	h := newHub(mode)
	go h.pump(sc)

	mux := http.NewServeMux()
	mux.HandleFunc("/events", h.handleEvents)
	mux.Handle("/", noStore(http.FileServer(http.FS(assets))))

	urls := reachableURLs(*addr)
	fmt.Fprintf(os.Stderr, "agentweb: listening on %s  (source: %s)\n", *addr, sourceDesc(*replay))
	for _, u := range urls {
		fmt.Fprintf(os.Stderr, "          %s\n", u)
	}
	if *open {
		go openBrowser(urls[len(urls)-1]) // the localhost entry, for an on-box browser
	}
	if err := http.ListenAndServe(*addr, mux); err != nil {
		fmt.Fprintln(os.Stderr, "agentweb:", err)
		os.Exit(1)
	}
}

func sourceDesc(replay string) string {
	if replay != "" {
		return "replay " + replay
	}
	return "stdin (live)"
}

// reachableURLs turns a listen address into the URLs you can actually open. For a
// concrete host it's just that host; for a wildcard bind (0.0.0.0, ::, or empty)
// it enumerates this machine's non-loopback IPv4 addresses — handy for remote dev,
// where 0.0.0.0 itself isn't browsable — and appends localhost.
func reachableURLs(addr string) []string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return []string{"http://" + addr}
	}
	if host != "" && host != "0.0.0.0" && host != "::" {
		return []string{"http://" + addr}
	}
	var urls []string
	if ifaces, err := net.InterfaceAddrs(); err == nil {
		for _, a := range ifaces {
			ipn, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			if ip4 := ipn.IP.To4(); ip4 != nil && !ip4.IsLoopback() {
				urls = append(urls, "http://"+ip4.String()+":"+port)
			}
		}
	}
	return append(urls, "http://localhost:"+port)
}

// --- broadcast hub ----------------------------------------------------------

// hubMsg carries either one NDJSON line or the end-of-stream marker.
type hubMsg struct {
	line string
	eof  bool
}

// hub keeps every line seen so far (the backlog) and fans new lines out to all
// connected SSE clients. A new subscriber atomically snapshots the backlog and
// registers under the same lock publish uses, so it sees each line exactly once.
type hub struct {
	mu    sync.Mutex
	lines []string
	subs  map[chan hubMsg]struct{}
	eof   bool
	mode  string
}

func newHub(mode string) *hub {
	return &hub{subs: map[chan hubMsg]struct{}{}, mode: mode}
}

func (h *hub) subscribe() (backlog []string, ch chan hubMsg, eof bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	backlog = append([]string(nil), h.lines...)
	ch = make(chan hubMsg, 1024)
	h.subs[ch] = struct{}{}
	return backlog, ch, h.eof
}

func (h *hub) unsubscribe(ch chan hubMsg) {
	h.mu.Lock()
	if _, ok := h.subs[ch]; ok {
		delete(h.subs, ch)
		close(ch)
	}
	h.mu.Unlock()
}

func (h *hub) publish(line string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.lines = append(h.lines, line)
	for ch := range h.subs {
		select {
		case ch <- hubMsg{line: line}:
		default: // a wedged client must not stall the source; it will resync on reconnect
		}
	}
}

func (h *hub) finish() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.eof = true
	for ch := range h.subs {
		select {
		case ch <- hubMsg{eof: true}:
		default:
		}
	}
}

// pump reads NDJSON lines from the source into the backlog until it ends.
func (h *hub) pump(sc *bufio.Scanner) {
	for sc.Scan() {
		ln := strings.TrimRight(sc.Text(), "\r")
		if strings.TrimSpace(ln) == "" {
			continue
		}
		h.publish(ln)
	}
	h.finish()
}

func (h *hub) handleEvents(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	hd := w.Header()
	hd.Set("Content-Type", "text/event-stream")
	hd.Set("Cache-Control", "no-cache")
	hd.Set("Connection", "keep-alive")
	hd.Set("X-Accel-Buffering", "no") // defeat proxy buffering

	backlog, ch, eof := h.subscribe()
	defer h.unsubscribe(ch)

	fmt.Fprintf(w, "event: control\ndata: {\"mode\":%q}\n\n", h.mode)
	for _, ln := range backlog {
		fmt.Fprintf(w, "data: %s\n\n", ln)
	}
	if eof {
		fmt.Fprint(w, "event: control\ndata: {\"eof\":true}\n\n")
	}
	fl.Flush()

	ctx := r.Context()
	keep := time.NewTicker(15 * time.Second)
	defer keep.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case m, ok := <-ch:
			if !ok {
				return
			}
			if m.eof {
				fmt.Fprint(w, "event: control\ndata: {\"eof\":true}\n\n")
			} else {
				fmt.Fprintf(w, "data: %s\n\n", m.line)
			}
			fl.Flush()
		case <-keep.C:
			fmt.Fprint(w, ": keepalive\n\n")
			fl.Flush()
		}
	}
}

// noStore disables caching of the embedded page assets so a rebuilt binary's
// HTML/CSS/JS is always served fresh on reload (no stale bundle during dev).
func noStore(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		h.ServeHTTP(w, r)
	})
}

// openBrowser is a best-effort convenience for -open; failures are ignored.
func openBrowser(url string) {
	var cmd string
	var args []string
	switch runtime.GOOS {
	case "darwin":
		cmd = "open"
	case "windows":
		cmd, args = "rundll32", []string{"url.dll,FileProtocolHandler"}
	default:
		cmd = "xdg-open"
	}
	_ = exec.Command(cmd, append(args, url)...).Start()
}
