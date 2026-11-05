package api

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

func newReadinessServer() *Server {
	return &Server{server: &http.Server{
		Addr: "127.0.0.1:0",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
	}}
}

func assertReadinessOpen(t *testing.T, ready <-chan struct{}) {
	t.Helper()
	select {
	case <-ready:
		t.Fatal("startup failure closed readiness channel")
	default:
	}
}

func awaitReadinessSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for lifecycle signal")
	}
}

func awaitReadinessResult(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for server exit")
		return nil
	}
}

func stopReadinessServer(t *testing.T, s *Server) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Stop(ctx); err != nil {
		t.Errorf("Stop: %v", err)
		if errClose := s.server.Close(); errClose != nil {
			t.Errorf("Close after failed Stop: %v", errClose)
		}
	}
}

func TestServerStartReadyOccupiedAddress(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if errClose := listener.Close(); errClose != nil {
			t.Error(errClose)
		}
	})
	s := newReadinessServer()
	s.server.Addr = listener.Addr().String()
	ready := make(chan struct{})
	if errStart := s.StartReady(ready); errStart == nil {
		t.Fatal("expected occupied address error")
	}
	assertReadinessOpen(t, ready)
	if s.muxBaseListener != nil || s.muxHTTPListener != nil {
		t.Fatal("failed startup published listeners")
	}
}

func TestServerStartReadyInvalidTLS(t *testing.T) {
	for _, missingFiles := range []bool{false, true} {
		name := "empty paths"
		if missingFiles {
			name = "missing files"
		}
		t.Run(name, func(t *testing.T) {
			s := newReadinessServer()
			s.cfg = &config.Config{}
			s.cfg.TLS.Enable = true
			if missingFiles {
				s.cfg.TLS.Cert = filepath.Join(t.TempDir(), "missing.crt")
				s.cfg.TLS.Key = filepath.Join(t.TempDir(), "missing.key")
			}
			ready := make(chan struct{})
			if err := s.StartReady(ready); err == nil {
				t.Fatal("expected TLS validation error")
			}
			assertReadinessOpen(t, ready)
			if s.muxHTTPListener != nil {
				t.Fatal("invalid TLS published HTTP listener")
			}
			if conn, err := s.muxBaseListener.Accept(); !errors.Is(err, net.ErrClosed) {
				if conn != nil {
					if errClose := conn.Close(); errClose != nil {
						t.Error(errClose)
					}
				}
				t.Fatalf("listener after TLS failure = %v, want net.ErrClosed", err)
			}
		})
	}
}

func TestServerStartReadyServesHTTPAndRejectsSecondStart(t *testing.T) {
	s := newReadinessServer()
	ready := make(chan struct{})
	result := make(chan error, 1)
	go func() { result <- s.StartReady(ready) }()
	t.Cleanup(func() {
		stopReadinessServer(t, s)
		if err := awaitReadinessResult(t, result); err != nil {
			t.Error(err)
		}
	})
	awaitReadinessSignal(t, ready)
	secondReady := make(chan struct{})
	if err := s.StartReady(secondReady); !errors.Is(err, errServerStarted) {
		t.Fatalf("second StartReady = %v, want %v", err, errServerStarted)
	}
	assertReadinessOpen(t, secondReady)
	s.lifecycleMu.Lock()
	addr := s.muxBaseListener.Addr().String()
	s.lifecycleMu.Unlock()
	transport := &http.Transport{DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			t.Error(errClose)
		}
	}()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HTTP status = %d, want 200", resp.StatusCode)
	}
}

func TestServerStopDoesNotWaitForHijackedWebSocketDrainAndConnectionSurvivesStartReadyReturn(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := newReadinessServer()
	handlerDone := make(chan struct{})
	handlerErrors := make(chan error, 2)
	var handlerMu sync.Mutex
	var handlerConn *websocket.Conn
	handlerStarted, handlerClosing := false, false
	s.server.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		handlerMu.Lock()
		if handlerClosing {
			handlerMu.Unlock()
			return
		}
		handlerStarted = true
		handlerMu.Unlock()
		defer close(handlerDone)
		upgrader := websocket.Upgrader{}
		conn, errUpgrade := upgrader.Upgrade(w, req, nil)
		if errUpgrade != nil {
			handlerErrors <- errUpgrade
			return
		}
		defer func() {
			if errClose := conn.Close(); errClose != nil && !errors.Is(errClose, net.ErrClosed) {
				handlerErrors <- errClose
			}
		}()
		handlerMu.Lock()
		closing := handlerClosing
		handlerConn = conn
		handlerMu.Unlock()
		if closing {
			return
		}
		for range 2 {
			messageType, message, errRead := conn.ReadMessage()
			if errRead != nil {
				handlerErrors <- errRead
				return
			}
			if errWrite := conn.WriteMessage(messageType, message); errWrite != nil {
				handlerErrors <- errWrite
				return
			}
		}
	})
	ready := make(chan struct{})
	startResult := make(chan error, 1)
	startAwaited := false
	stopResult := make(chan error, 1)
	stopStarted, stopAwaited := false, false
	var client *websocket.Conn
	t.Cleanup(func() {
		handlerMu.Lock()
		handlerClosing = true
		started, conn := handlerStarted, handlerConn
		handlerMu.Unlock()
		if client != nil {
			if errClose := client.Close(); errClose != nil && !errors.Is(errClose, net.ErrClosed) {
				t.Errorf("close WebSocket client: %v", errClose)
			}
		}
		if conn != nil {
			if errClose := conn.Close(); errClose != nil && !errors.Is(errClose, net.ErrClosed) {
				t.Errorf("close hijacked WebSocket: %v", errClose)
			}
		}
		stopReadinessServer(t, s)
		if errClose := s.server.Close(); errClose != nil {
			t.Errorf("Close: %v", errClose)
		}
		if stopStarted && !stopAwaited {
			if errStop := awaitReadinessResult(t, stopResult); errStop != nil {
				t.Errorf("Stop: %v", errStop)
			}
		}
		if !startAwaited {
			if errStart := awaitReadinessResult(t, startResult); errStart != nil {
				t.Errorf("StartReady: %v", errStart)
			}
		}
		if started {
			awaitReadinessSignal(t, handlerDone)
		}
	})
	go func() {
		startResult <- s.startReady(ready, func(string, string) (net.Listener, error) {
			return listener, nil
		})
	}()
	awaitReadinessSignal(t, ready)
	dialer := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	client, response, err := dialer.Dial("ws://"+listener.Addr().String(), nil)
	if response != nil && response.Body != nil {
		if errClose := response.Body.Close(); errClose != nil {
			t.Errorf("close handshake response: %v", errClose)
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	echo := func(message string) {
		t.Helper()
		if errDeadline := client.SetWriteDeadline(time.Now().Add(5 * time.Second)); errDeadline != nil {
			t.Fatal(errDeadline)
		}
		if errWrite := client.WriteMessage(websocket.TextMessage, []byte(message)); errWrite != nil {
			t.Fatalf("write %q: %v", message, errWrite)
		}
		if errDeadline := client.SetReadDeadline(time.Now().Add(5 * time.Second)); errDeadline != nil {
			t.Fatal(errDeadline)
		}
		messageType, got, errRead := client.ReadMessage()
		if errRead != nil {
			t.Fatalf("read %q echo: %v", message, errRead)
		}
		if messageType != websocket.TextMessage || string(got) != message {
			t.Fatalf("echo = (%d, %q), want (%d, %q)", messageType, got, websocket.TextMessage, message)
		}
	}
	echo("before Stop")
	stopStarted = true
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		stopResult <- s.Stop(ctx)
	}()
	errStop := awaitReadinessResult(t, stopResult)
	stopAwaited = true
	if errStop != nil {
		t.Fatalf("Stop: %v", errStop)
	}
	errStart := awaitReadinessResult(t, startResult)
	startAwaited = true
	if errStart != nil {
		t.Fatalf("StartReady: %v", errStart)
	}
	// Stop and StartReady have returned while the handler still needs its second message.
	echo("after Stop and StartReady returned")
	if errClose := client.Close(); errClose != nil {
		t.Fatal(errClose)
	}
	client = nil
	awaitReadinessSignal(t, handlerDone)
	select {
	case errHandler := <-handlerErrors:
		t.Fatalf("WebSocket handler: %v", errHandler)
	default:
	}
}

func TestServerStopBeforeStartReady(t *testing.T) {
	s := newReadinessServer()
	stopReadinessServer(t, s)
	ready := make(chan struct{})
	if err := s.StartReady(ready); !errors.Is(err, errServerStopped) {
		t.Fatalf("StartReady after Stop = %v, want %v", err, errServerStopped)
	}
	assertReadinessOpen(t, ready)
	if s.muxBaseListener != nil || s.muxHTTPListener != nil {
		t.Fatal("stopped server published listeners")
	}
}

func TestServerStopWhileStartReadyEntersServe(t *testing.T) {
	s := newReadinessServer()
	listener := newMuxListener(&net.TCPAddr{}, 1)
	enteredServe := make(chan struct{})
	releaseServe := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseServe) }) }
	s.server.BaseContext = func(net.Listener) context.Context {
		close(enteredServe)
		<-releaseServe
		return context.Background()
	}
	ready := make(chan struct{})
	result := make(chan error, 1)
	stopResult := make(chan error, 1)
	stopStarted := false
	startAwaited := false
	stopAwaited := false
	stop := func() {
		stopStarted = true
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stopResult <- s.Stop(ctx)
		}()
	}
	go func() {
		result <- s.startReady(ready, func(string, string) (net.Listener, error) {
			return listener, nil
		})
	}()
	t.Cleanup(func() {
		// Unblock Serve even when an assertion fails before Stop.
		release()
		if !stopStarted {
			stop()
		}
		if !stopAwaited {
			if err := awaitReadinessResult(t, stopResult); err != nil {
				t.Errorf("Stop: %v", err)
			}
		}
		if !startAwaited {
			if err := awaitReadinessResult(t, result); err != nil {
				t.Error(err)
			}
		}
	})
	awaitReadinessSignal(t, ready)
	awaitReadinessSignal(t, enteredServe)
	// Stop must close the base listener while BaseContext is blocked, but
	// Shutdown waits for Serve to return before Stop can finish.
	stop()
	awaitReadinessSignal(t, listener.closeCh)
	if conn, err := listener.Accept(); !errors.Is(err, net.ErrClosed) {
		if conn != nil {
			if errClose := conn.Close(); errClose != nil {
				t.Error(errClose)
			}
		}
		t.Fatalf("listener Accept after Stop = %v, want net.ErrClosed", err)
	}
	release()
	stopAwaited = true
	if err := awaitReadinessResult(t, stopResult); err != nil {
		t.Errorf("Stop: %v", err)
	}
	startAwaited = true
	if err := awaitReadinessResult(t, result); err != nil {
		t.Error(err)
	}
}

func TestServerStopCannotReturnBeforePendingBind(t *testing.T) {
	s := newReadinessServer()
	beforeBind := make(chan struct{})
	releaseBind := make(chan struct{})
	stopEntered := make(chan struct{})
	stopReturned := make(chan struct{})
	startResult := make(chan error, 1)
	stopResult := make(chan error, 1)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseBind) }) }
	ready := make(chan struct{})
	bound := make(chan net.Listener, 1)
	go func() {
		startResult <- s.startReady(ready, func(network, addr string) (net.Listener, error) {
			close(beforeBind)
			<-releaseBind
			select {
			case <-stopReturned:
				return nil, errors.New("Stop returned before pending bind")
			default:
			}
			listener, err := net.Listen(network, addr)
			if err == nil {
				bound <- listener
			}
			return listener, err
		})
	}()
	t.Cleanup(func() {
		release()
		stopReadinessServer(t, s)
		err := awaitReadinessResult(t, startResult)
		if err != nil && !errors.Is(err, errServerStopped) {
			t.Errorf("StartReady: %v", err)
		}
	})
	awaitReadinessSignal(t, beforeBind)
	// This assertion observes the exact pre-bind gap rather than relying on
	// the scheduler to run Stop before the listener factory is released.
	if s.lifecycleMu.TryLock() {
		s.lifecycleMu.Unlock()
		t.Fatal("startup does not serialize the pre-bind gap with Stop")
	}
	go func() {
		close(stopEntered)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := s.Stop(ctx)
		close(stopReturned)
		stopResult <- err
	}()
	t.Cleanup(func() {
		release()
		if err := awaitReadinessResult(t, stopResult); err != nil {
			t.Errorf("concurrent Stop: %v", err)
		}
	})
	awaitReadinessSignal(t, stopEntered)
	release()
	awaitReadinessSignal(t, stopReturned)
	// Stop clears the published listener after closing it, so observe the
	// listener the pending bind returned.
	var listener net.Listener
	select {
	case listener = <-bound:
	default:
		t.Fatal("pending bind did not publish its listener")
	}
	if conn, err := listener.Accept(); !errors.Is(err, net.ErrClosed) {
		if conn != nil {
			if errClose := conn.Close(); errClose != nil {
				t.Error(errClose)
			}
		}
		t.Fatalf("Accept after Stop = %v, want net.ErrClosed", err)
	}
}

type readinessCloseListener struct {
	net.Listener
	closed chan struct{}
	once   sync.Once
}

func (l *readinessCloseListener) Close() error {
	err := l.Listener.Close()
	l.once.Do(func() { close(l.closed) })
	return err
}

type readinessShutdownContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func (c *readinessShutdownContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

func TestServerStopDrainsActiveRequest(t *testing.T) {
	t.Run("complete drain", func(t *testing.T) { testServerStopDrainsActiveRequest(t, false) })
	t.Run("canceled shutdown preserves request", func(t *testing.T) { testServerStopDrainsActiveRequest(t, true) })
}

func testServerStopDrainsActiveRequest(t *testing.T, cancelShutdown bool) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	observed := &readinessCloseListener{Listener: listener, closed: make(chan struct{})}
	releaseHandler := make(chan struct{})
	release := sync.OnceFunc(func() { close(releaseHandler) })
	entered := make(chan context.Context, 1)
	handlerDone := make(chan struct{})
	var handlerMu sync.Mutex
	handlerStarted := false
	handlerClosing := false
	s := newReadinessServer()
	const first = "first chunk\n"
	const last = "last chunk\n"
	s.server.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		handlerMu.Lock()
		if handlerClosing {
			handlerMu.Unlock()
			return
		}
		handlerStarted = true
		handlerMu.Unlock()
		defer close(handlerDone)
		if _, errWrite := io.WriteString(w, first); errWrite != nil {
			return
		}
		if errFlush := http.NewResponseController(w).Flush(); errFlush != nil {
			return
		}
		entered <- req.Context()
		select {
		case <-releaseHandler:
			if _, errWrite := io.WriteString(w, last); errWrite != nil {
				t.Errorf("write final chunk: %v", errWrite)
			}
		case <-req.Context().Done():
		case <-time.After(5 * time.Second):
			t.Error("timed out waiting for handler release")
		}
	})
	ready := make(chan struct{})
	startResult := make(chan error, 1)
	stopResult := make(chan error, 1)
	stopContext, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelStop()
	shutdownContext := &readinessShutdownContext{Context: stopContext, observed: make(chan struct{})}
	stopStarted, startAwaited, stopAwaited := false, false, false
	stop := func() {
		stopStarted = true
		go func() {
			stopResult <- s.Stop(shutdownContext)
		}()
	}
	go func() {
		startResult <- s.startReady(ready, func(string, string) (net.Listener, error) {
			return observed, nil
		})
	}()
	t.Cleanup(func() {
		handlerMu.Lock()
		handlerClosing = true
		started := handlerStarted
		handlerMu.Unlock()
		defer func() {
			if started {
				awaitReadinessSignal(t, handlerDone)
			}
		}()
		defer func() {
			if errClose := s.server.Close(); errClose != nil {
				t.Errorf("Close: %v", errClose)
			}
		}()
		release()
		if !stopStarted {
			stop()
		}
		if !stopAwaited {
			if errStop := awaitReadinessResult(t, stopResult); errStop != nil {
				t.Errorf("Stop: %v", errStop)
			}
		}
		if !startAwaited {
			if errStart := awaitReadinessResult(t, startResult); errStart != nil {
				t.Errorf("StartReady: %v", errStart)
			}
		}
	})
	awaitReadinessSignal(t, ready)
	transport := &http.Transport{DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+listener.Addr().String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			t.Error(errClose)
		}
	}()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HTTP status = %d, want 200", resp.StatusCode)
	}
	prefix := make([]byte, len(first))
	if _, errRead := io.ReadFull(resp.Body, prefix); errRead != nil {
		t.Fatal(errRead)
	}
	var requestContext context.Context
	select {
	case requestContext = <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	stop()
	awaitReadinessSignal(t, observed.closed)
	// Shutdown consults Done only after finding the active connection still
	// prevents quiescence. Listener closure alone does not establish this.
	awaitReadinessSignal(t, shutdownContext.observed)
	select {
	case errStop := <-stopResult:
		stopAwaited = true
		t.Fatalf("Stop returned while handler blocked: %v", errStop)
	default:
	}
	select {
	case <-requestContext.Done():
		t.Fatal("Stop canceled the active request")
	default:
	}
	if cancelShutdown {
		// Once the shutdown context ends, Stop force-closes the remaining
		// connection (upstream #6094) and reports a clean stop.
		cancelStop()
		stopAwaited = true
		if errStop := awaitReadinessResult(t, stopResult); errStop != nil {
			t.Fatalf("Stop after cancellation = %v, want nil", errStop)
		}
		select {
		case <-requestContext.Done():
		case <-ctx.Done():
			t.Fatal("forced close did not end the active request")
		}
		release()
		if suffix, errRead := io.ReadAll(resp.Body); errRead == nil && string(prefix)+string(suffix) == first+last {
			t.Fatal("request completed after Stop force-closed it")
		}
		startAwaited = true
		if errStart := awaitReadinessResult(t, startResult); errStart != nil {
			t.Errorf("StartReady: %v", errStart)
		}
		return
	}
	release()
	suffix, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(prefix) + string(suffix); got != first+last {
		t.Fatalf("response = %q, want %q", got, first+last)
	}
	if !stopAwaited {
		stopAwaited = true
		if errStop := awaitReadinessResult(t, stopResult); errStop != nil {
			t.Errorf("Stop: %v", errStop)
		}
	}
	startAwaited = true
	if errStart := awaitReadinessResult(t, startResult); errStart != nil {
		t.Errorf("StartReady: %v", errStart)
	}
}

func TestServerStartReadyServesVerifiedTLS(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "readiness.test"},
		DNSNames:              []string{"readiness.test"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certPath, keyPath := filepath.Join(dir, "server.crt"), filepath.Join(dir, "server.key")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	if errWrite := os.WriteFile(certPath, certPEM, 0600); errWrite != nil {
		t.Fatal(errWrite)
	}
	if errWrite := os.WriteFile(keyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}), 0600); errWrite != nil {
		t.Fatal(errWrite)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certPEM) {
		t.Fatal("failed to trust generated certificate")
	}
	s := newReadinessServer()
	s.cfg = &config.Config{}
	s.cfg.TLS.Enable = true
	s.cfg.TLS.Cert, s.cfg.TLS.Key = certPath, keyPath
	ready := make(chan struct{})
	result := make(chan error, 1)
	go func() { result <- s.StartReady(ready) }()
	t.Cleanup(func() {
		stopReadinessServer(t, s)
		if errStart := awaitReadinessResult(t, result); errStart != nil {
			t.Errorf("StartReady: %v", errStart)
		}
	})
	awaitReadinessSignal(t, ready)
	s.lifecycleMu.Lock()
	addr := s.muxBaseListener.Addr().String()
	s.lifecycleMu.Unlock()
	transport := &http.Transport{
		DisableKeepAlives: true,
		TLSClientConfig:   &tls.Config{RootCAs: roots, ServerName: "readiness.test", MinVersion: tls.VersionTLS12},
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+addr, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			t.Error(errClose)
		}
	}()
	if _, errRead := io.Copy(io.Discard, resp.Body); errRead != nil {
		t.Fatal(errRead)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HTTPS status = %d, want 200", resp.StatusCode)
	}
	if resp.TLS == nil || len(resp.TLS.VerifiedChains) == 0 {
		t.Fatal("HTTPS response has no verified certificate chain")
	}
}
