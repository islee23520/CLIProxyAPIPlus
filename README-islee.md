# islee fork of CLIProxyAPIPlus

`main` = [kaitranntt/CLIProxyAPIPlus](https://github.com/kaitranntt/CLIProxyAPIPlus) `main` + the patches below.
`.github/workflows/islee-upstream-sync.yml` merges upstream every 6 hours. When the merge is clean and the build and tests are green, the result is pushed straight to `main`. Otherwise a `sync/upstream-<sha>` PR labelled `upstream-sync` is opened.
Workflows inherited from upstream that sync or publish for kaitranntt are disabled in this repository's Actions settings, not edited, so that upstream merges stay conflict-free.

## Fork patches

| Patch | Where |
| --- | --- |
| `routing.disabled-auth-reroute` | `sdk/cliproxy/auth/disabled_auth_reroute.go`, hooks in `conductor_lifecycle.go` (disable transition) and `conductor_execution.go` (attempt binding), config in `internal/config/config_types.go` |
| SO_REUSEPORT listener for blue/green rolling updates (`CLIPROXY_LISTEN_REUSEPORT=1`) and graceful drain | `internal/api/listen_reuseport_*.go`, `internal/api/server.go`, `sdk/cliproxy/service_lifecycle.go` |
| Token files encoded before the file is opened (no truncated JSON on encode errors) | `internal/auth/*/token.go` |
| Tailnet relay | `cmd/tailnet-relay` |
| Management latest-version check against this lineage with release-page fallback | `internal/api/handlers/management/config_basic.go` |

Claude requests use upstream's Claude Code CLI identity unchanged.

## routing.disabled-auth-reroute

```yaml
routing:
  session-affinity: true
  disabled-auth-reroute: true
```

When a credential is disabled, through `PATCH /v0/management/auth-files/status {"disabled": true}` or by setting `"disabled": true` in its auth file:

1. Its session-affinity bindings are dropped, so the next turn of each bound session is picked again by `routing.strategy` (round-robin by default).
2. Upstream attempts still running on it are cancelled. An attempt that has not streamed yet is retried on another credential. A stream that is already sending ends with a retryable `auth_disabled` error.

## Deploy (WSL blue/green)

```sh
CGO_ENABLED=1 /home/oliver/toolchains/go1.26.5/bin/go build -o CLIProxyAPIPlus ./cmd/server
sudo cliproxy-rolling-update ./CLIProxyAPIPlus    # --rollback to return to the previous slot
```
