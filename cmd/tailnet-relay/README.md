# Tailnet inference relay

An interim, loopback-only entry for an existing CLIProxyAPI server. It does not
perform OAuth, credential selection, retries, or failover. No running service or
tailnet configuration is changed by building or testing it.

```sh
go build -o tailnet-relay ./cmd/tailnet-relay
./tailnet-relay --listen 127.0.0.1:18318 --upstream http://127.0.0.1:8317
```

Both flags are required. `--listen` accepts only literal loopback IP addresses
and explicit ports from 1024 through 65535; `localhost`, wildcard addresses and
port zero are refused. `--upstream` must be a fixed HTTP(S) origin without
credentials, a path prefix, a query or a fragment. A separately configured
tailnet ingress can forward to the listener. The relay itself does not enforce
tailnet identities; configure that ingress and the existing API authentication.

Existing client authorization headers and query parameters pass to the upstream.
No API key is added, removed, selected or logged. The original HTTP Host is
preserved for upstream virtual-host/origin logic, but never controls dialing;
inbound Forwarded, X-Forwarded-For, X-Forwarded-Host, X-Forwarded-Proto and
X-Real-IP are stripped. Other extension headers are forwarded unchanged.
The fixed origin controls HTTPS certificate verification and TLS server name.

Only the explicit inference method/path allowlist from `internal/api/server.go`
is forwarded, including Gemini model actions, video routes, responses WebSockets
and authenticated responses WebSockets. The separately attached `/v1/ws` route
is excluded because upstream configuration can disable its authentication.
Custom WebSocket attachment paths and plugin routes are unavailable. UI,
management, OAuth, telemetry, health,
unknown paths and wrong methods receive 404 locally. Paths must be canonical
ASCII and unescaped: dot segments, duplicate/trailing slashes, percent-encoded
paths (including encoded slashes), backslashes and absolute-form targets are
refused before forwarding. Query encoding is preserved verbatim.

The stdlib ReverseProxy streams without buffering a complete response, carries
WebSocket upgrades bidirectionally and propagates client cancellation. There
are no post-connect network timeouts. Each request uses a fresh upstream
connection to avoid automatic transport replay; this trades connection reuse
for a strict no-retry policy. Responses and protocol hop-by-hop headers follow
the stdlib reverse proxy rules. Proxy and HTTP-server errors produce generic
sanitized log events without URLs, query credentials or headers; clients
receive a generic 502 for upstream connection errors.

On Linux, SIGTERM stops new relay acquisitions and closes the listener, then
waits for acquired HTTP requests, including SSE and upgraded WebSocket proxy
handlers, to finish without canceling them or imposing a drain timeout. A
long-lived or stuck request can therefore delay exit indefinitely. This does not
protect against SIGKILL, SIGINT, a service manager's forced-stop deadline, host
failure, or requests accepted but not yet acquired by the handler. Other
platforms retain their default signal behavior. The `ready` lifecycle event
means the listener was bound and serving was initiated; it is not an upstream
health check. `http_drained` does not mean WebSockets have finished.

```sh
go test ./cmd/tailnet-relay
```

Tests use local ephemeral listeners and synthetic auth values only.
