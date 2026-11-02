// tailnet-relay exposes only the inference API of a fixed CLIProxyAPI origin.
// A separate tailnet ingress must forward to this loopback listener.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/netip"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"

	logger "github.com/sirupsen/logrus"
)

// Log only a stable event; stdlib diagnostics may contain request secrets.
type sanitizedLogWriter struct{}

func (sanitizedLogWriter) Write(p []byte) (int, error) {
	logger.Error("relay HTTP transport or server error")
	return len(p), nil
}

func main() {
	listen := flag.String("listen", "", "Required literal loopback address and port (1024-65535), e.g. 127.0.0.1:18318")
	upstream := flag.String("upstream", "", "Required fixed http(s) origin, without credentials, path, query or fragment")
	flag.Parse()
	if flag.NArg() != 0 {
		logger.Error("unexpected positional arguments")
		os.Exit(2)
	}
	if err := run(*listen, *upstream); err != nil {
		// Configuration and transport errors can contain credentials or URLs.
		logger.Error(err)
		os.Exit(1)
	}
}

func run(listen, upstream string) error {
	if err := validateListen(listen); err != nil {
		return err
	}
	controller, err := newGenerationController(upstream)
	if err != nil {
		return err
	}
	defer controller.Close()
	shutdown, stop := relayShutdownSignal()
	defer stop()
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return fmt.Errorf("cannot bind relay listener")
	}
	return serveRelay(listener, controller, shutdown)
}

// serveRelay owns listener and controller. Its caller keeps signal notification
// registered until it returns, so additional SIGTERMs cannot interrupt a drain.
// SIGKILL and a service manager's forced-stop deadline are not protected.
func serveRelay(listener net.Listener, controller *generationController, shutdown <-chan os.Signal) error {
	server := &http.Server{Handler: controller, ErrorLog: log.New(sanitizedLogWriter{}, "", 0)}
	served := make(chan error, 1)
	var errServe error
	started := false
	stopping := false
	select {
	case <-shutdown:
		stopping = true
		// A signal received during startup must not start accepting requests.
	default:
		started = true
		go func() { served <- server.Serve(listener) }()
		logger.WithField("event", "ready").Info("relay lifecycle")
		select {
		case <-shutdown:
			stopping = true
		case errServe = <-served:
			started = false
		}
	}
	drained := controller.Close()
	errClose := listener.Close()
	logger.WithField("event", "draining").Info("relay lifecycle")
	// Do not derive this context from the signal: existing requests, including
	// long-lived SSE streams, retain their original cancellation semantics.
	errShutdown := server.Shutdown(context.Background())
	logger.WithField("event", "http_drained").Info("relay lifecycle")
	// Shutdown does not wait for hijacked connections. ReverseProxy keeps their
	// generation acquired until the WebSocket handler has actually returned.
	<-drained
	if started {
		errServe = <-served
	}
	if errServe != nil && !errors.Is(errServe, http.ErrServerClosed) && !(stopping && errors.Is(errServe, net.ErrClosed)) {
		return fmt.Errorf("relay server stopped unexpectedly")
	}
	if errClose != nil && !errors.Is(errClose, net.ErrClosed) {
		return fmt.Errorf("cannot close relay listener")
	}
	// The explicit listener close can be observed again by Shutdown's wrapper.
	if errShutdown != nil && !errors.Is(errShutdown, net.ErrClosed) {
		return fmt.Errorf("cannot drain relay server")
	}
	return nil
}

func validateListen(address string) error {
	host, portText, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("listen must be a literal loopback address and explicit high port")
	}
	ip, err := netip.ParseAddr(host)
	port, errPort := strconv.Atoi(portText)
	if err != nil || !ip.IsLoopback() || ip.Zone() != "" || errPort != nil || port < 1024 || port > 65535 || strconv.Itoa(port) != portText {
		return fmt.Errorf("listen must be a literal loopback address and explicit port in 1024-65535")
	}
	return nil
}

func newRelay(origin string) (http.Handler, *http.Transport, error) {
	target, err := url.Parse(origin)
	if err != nil || target == nil || (target.Scheme != "http" && target.Scheme != "https") || target.Hostname() == "" || target.User != nil || target.Opaque != "" || (target.Path != "" && target.Path != "/") || target.RawPath != "" || target.RawQuery != "" || target.ForceQuery || target.Fragment != "" || strings.ContainsAny(target.Host, "\\% \t\r\n") {
		return nil, nil, fmt.Errorf("upstream must be an http(s) origin without credentials, path, query or fragment")
	}
	if port := target.Port(); port != "" {
		n, errPort := strconv.Atoi(port)
		if errPort != nil || n < 1 || n > 65535 {
			return nil, nil, fmt.Errorf("upstream port must be in 1-65535")
		}
	}
	target.Path = ""
	// A fresh connection per request also prevents Transport's automatic retry
	// of idempotent requests on a previously used connection. Do not inherit
	// environment proxies, compression, or any post-connect deadlines.
	transport := &http.Transport{DisableKeepAlives: true, DisableCompression: true}
	proxy := &httputil.ReverseProxy{
		Transport: transport,
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(target)
			// Dial only the configured origin, but retain the client's Host for
			// upstream virtual-host and same-origin logic. Never trust inbound
			// forwarding headers as assertions of client identity or TLS state.
			r.Out.Host = r.In.Host
			r.Out.URL.RawQuery = r.In.URL.RawQuery
			r.Out.Header.Del("X-Real-IP")
		},
		FlushInterval: -1,
		ErrorLog:      log.New(sanitizedLogWriter{}, "", 0),
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			logger.Error("relay upstream request failed")
			http.Error(w, "upstream unavailable", http.StatusBadGateway)
		},
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !canonicalRequest(r) || !allowedRoute(r.Method, r.URL.Path) {
			http.NotFound(w, r)
			return
		}
		proxy.ServeHTTP(w, r)
	}), transport, nil
}

func canonicalRequest(r *http.Request) bool {
	u := r.URL
	if u == nil || u.IsAbs() || u.Host != "" || u.Opaque != "" || u.RawPath != "" || u.Fragment != "" || u.Path == "" || u.Path[0] != '/' || path.Clean(u.Path) != u.Path || strings.Contains(u.Path, "//") {
		return false
	}
	// Refuse every escaped or ambiguous path spelling rather than letting the
	// relay, upstream router and any intervening ingress normalize differently.
	for _, c := range u.Path {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || strings.ContainsRune("/-._~:@", c)) {
			return false
		}
	}
	requestTarget := u.Path
	if u.RawQuery != "" || u.ForceQuery {
		requestTarget += "?" + u.RawQuery
	}
	return r.RequestURI == requestTarget
}

// These method/path pairs mirror the inference routes in internal/api/server.go
// setupRoutes. The separately attached /v1/ws route can disable authentication
// and is intentionally excluded. Management, UI, OAuth,
// telemetry, health and dynamically registered plugin routes are not exposed.
func allowedRoute(method, p string) bool {
	if method == http.MethodPost {
		switch p {
		case "/v1/chat/completions", "/v1/completions", "/v1/images/generations", "/v1/images/edits",
			"/v1/videos", "/v1/videos/generations", "/v1/videos/edits", "/v1/videos/extensions",
			"/v1/messages", "/v1/messages/count_tokens", "/v1/responses", "/v1/responses/compact",
			"/openai/v1/videos", "/backend-api/codex/responses", "/backend-api/codex/responses/compact", "/v1beta/interactions":
			return true
		}
	}
	if method == http.MethodGet {
		switch p {
		case "/v1/models", "/v1/responses", "/backend-api/codex/responses", "/v1beta/models":
			return true
		}
		if strings.HasPrefix(p, "/v1/videos/") {
			id := strings.TrimPrefix(p, "/v1/videos/")
			return id != "" && !strings.Contains(id, "/")
		}
		if strings.HasPrefix(p, "/openai/v1/videos/") {
			tail := strings.TrimPrefix(p, "/openai/v1/videos/")
			id := strings.TrimSuffix(tail, "/content")
			return id != "" && !strings.Contains(id, "/")
		}
	}
	return (method == http.MethodGet || method == http.MethodPost) && strings.HasPrefix(p, "/v1beta/models/") && len(p) > len("/v1beta/models/")
}
