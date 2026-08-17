package main

// natscontext.go reads the `nats context` files that the nats CLI manages
// (under $XDG_CONFIG_HOME/nats or ~/.config/nats) and connects with them.
//
// We parse the JSON ourselves rather than importing github.com/nats-io/jsm.go's
// natscontext package: that package transitively pulls the entire JetStream
// manager and the nats-server into the binary (tens of MB) just to load a small
// config file. agenttap only needs connection settings, so a ~60-line reader
// keeps the dependency surface down to nats.go itself.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/nats-io/nats.go"
)

// natsContext is the subset of a `nats context` JSON file that affects how we
// connect. Unknown fields (socks_proxy, jetstream_*, windows_*, …) are ignored.
type natsContext struct {
	Description string `json:"description"`
	URL         string `json:"url"`
	Token       string `json:"token"`
	User        string `json:"user"`
	Password    string `json:"password"`
	Creds       string `json:"creds"`
	NKey        string `json:"nkey"`
	Cert        string `json:"cert"`
	Key         string `json:"key"`
	CA          string `json:"ca"`
	InboxPrefix string `json:"inbox_prefix"`
	UserJWT     string `json:"user_jwt"`
	UserSeed    string `json:"user_seed"`
	TLSFirst    bool   `json:"tls_first"`
}

// natsConfigDir is the nats CLI config root ($XDG_CONFIG_HOME/nats or
// ~/.config/nats).
func natsConfigDir() (string, error) {
	if d := os.Getenv("XDG_CONFIG_HOME"); d != "" {
		return filepath.Join(d, "nats"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".config", "nats"), nil
}

// loadNATSContext reads and decodes the named context file.
func loadNATSContext(name string) (*natsContext, error) {
	dir, err := natsConfigDir()
	if err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "context", name+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("nats context %q not found at %s (see: nats context ls)", name, path)
		}
		return nil, fmt.Errorf("read context %q: %w", name, err)
	}
	var ctx natsContext
	if err := json.Unmarshal(data, &ctx); err != nil {
		return nil, fmt.Errorf("parse context %q (%s): %w", name, path, err)
	}
	return &ctx, nil
}

// connectContext connects to NATS using a named `nats context`.
func connectContext(name string) (*nats.Conn, error) {
	ctx, err := loadNATSContext(name)
	if err != nil {
		return nil, err
	}

	url := ctx.URL
	if url == "" {
		url = nats.DefaultURL
	}

	opts := []nats.Option{nats.Name(appName)}
	switch {
	case ctx.Creds != "":
		opts = append(opts, nats.UserCredentials(ctx.Creds))
	case ctx.UserJWT != "" && ctx.UserSeed != "":
		opts = append(opts, nats.UserJWTAndSeed(ctx.UserJWT, ctx.UserSeed))
	case ctx.NKey != "":
		opt, err := nats.NkeyOptionFromSeed(ctx.NKey)
		if err != nil {
			return nil, fmt.Errorf("context %q: nkey: %w", name, err)
		}
		opts = append(opts, opt)
	case ctx.User != "" || ctx.Password != "":
		opts = append(opts, nats.UserInfo(ctx.User, ctx.Password))
	case ctx.Token != "":
		opts = append(opts, nats.Token(ctx.Token))
	}

	if ctx.Cert != "" || ctx.Key != "" {
		opts = append(opts, nats.ClientCert(ctx.Cert, ctx.Key))
	}
	if ctx.CA != "" {
		opts = append(opts, nats.RootCAs(ctx.CA))
	}
	if ctx.InboxPrefix != "" {
		opts = append(opts, nats.CustomInboxPrefix(ctx.InboxPrefix))
	}
	if ctx.TLSFirst {
		opts = append(opts, nats.TLSHandshakeFirst())
	}

	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, fmt.Errorf("connect context %q (%s): %w", name, url, err)
	}
	return nc, nil
}
