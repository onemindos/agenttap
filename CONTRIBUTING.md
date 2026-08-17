# Contributing to agenttap

Thanks for your interest in improving `agenttap`! This is a small, focused tool
and contributions of all sizes are welcome.

## Getting started

```sh
git clone https://github.com/M64GitHub/agenttap
cd agenttap
make demo        # render the scripted demo in the viz view
```

`agenttap` has a single direct dependency (`nats.go`) and no build steps beyond
`go build`. Please keep it that way where reasonable — a big part of the value
here is staying lean (no JetStream, no embedded server).

## Before opening a pull request

Everything below should pass. `make ci` runs all of it:

```sh
make fmt-check   # gofmt -l reports nothing
make vet         # go vet ./...
make test        # go test ./...
make build       # go build
```

- Run `gofmt -w .` (or `make fmt`) to format.
- Add or update tests for behavior changes. The pure helpers (frame parsing,
  text wrapping, width math, sanitization) are unit-tested in
  `agenttap_test.go` — extend those.
- Keep the chrome **ASCII-forward** and make sure output still degrades cleanly
  with `-no-color` (it must stay pipeable).
- Treat payload data as untrusted — route any newly displayed payload through
  `sanitize`.

## Reporting issues

Please include the `agenttap` command you ran, what you expected, and what you
saw. A snippet captured with `-no-color` is especially helpful.

## License

By contributing, you agree that your contributions will be licensed under the
[MIT License](LICENSE).
