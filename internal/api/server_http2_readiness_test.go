package api

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"golang.org/x/net/http2"
)

func TestServerStopDrainsHTTP2Streams(t *testing.T) {
	for _, cancelAt := range []int{-1, 0, 1} {
		name := "complete drain"
		if cancelAt >= 0 {
			name = fmt.Sprintf("cancel with %d active streams", 2-cancelAt)
		}
		t.Run(name, func(t *testing.T) { testServerStopDrainsHTTP2Streams(t, cancelAt) })
	}
}

func testServerStopDrainsHTTP2Streams(t *testing.T, cancelAt int) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	wait := func(signal <-chan struct{}) {
		t.Helper()
		select {
		case <-signal:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	s := newReadinessServer()
	cfg, roots := http2ReadinessTLS(t)
	s.cfg = cfg
	const first, last = "first chunk\n", "last chunk\n"
	var gates, entered, handlerDone, prefixes, bodies [2]chan struct{}
	var release [2]func()
	var requestContexts [2]context.Context
	var handlerErrors, clientErrors [2]error
	var responseBodies [2]string
	var handlerMu sync.Mutex
	var handlerStarted [2]bool
	closing := false
	for i := range gates {
		gates[i], entered[i], handlerDone[i] = make(chan struct{}), make(chan struct{}), make(chan struct{})
		prefixes[i], bodies[i] = make(chan struct{}), make(chan struct{})
		release[i] = sync.OnceFunc(func() { close(gates[i]) })
	}
	s.server.Handler = http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		i := 0
		if req.URL.Path == "/second" {
			i = 1
		}
		handlerMu.Lock()
		if closing || handlerStarted[i] {
			handlerMu.Unlock()
			return
		}
		handlerStarted[i] = true
		handlerMu.Unlock()
		defer close(handlerDone[i])
		requestContexts[i] = req.Context()
		signalEntered := sync.OnceFunc(func() { close(entered[i]) })
		defer signalEntered()
		if req.ProtoMajor != 2 {
			handlerErrors[i] = fmt.Errorf("request protocol = %s, want HTTP/2", req.Proto)
			return
		}
		if _, err := io.WriteString(w, first); err != nil {
			handlerErrors[i] = err
			return
		}
		if err := http.NewResponseController(w).Flush(); err != nil {
			handlerErrors[i] = err
			return
		}
		// Publish the context only once the flushed stream is active.
		signalEntered()
		select {
		case <-gates[i]:
			_, handlerErrors[i] = io.WriteString(w, last)
		case <-req.Context().Done():
			handlerErrors[i] = req.Context().Err()
		case <-ctx.Done():
			handlerErrors[i] = ctx.Err()
		}
	})
	ready, startDone, stopDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var startErr, stopErr error
	stopCtx, cancelStop := context.WithCancel(ctx)
	defer cancelStop()
	shutdown := &readinessShutdownContext{Context: stopCtx, observed: make(chan struct{})}
	var conn *tls.Conn
	var clients sync.WaitGroup
	stopStarted := false
	go func() { defer close(startDone); startErr = s.StartReady(ready) }()
	t.Cleanup(func() {
		handlerMu.Lock()
		closing = true
		started := handlerStarted
		handlerMu.Unlock()
		release[0]()
		release[1]()
		cancel()
		cancelStop()
		if conn != nil {
			if err := conn.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				t.Errorf("close TLS connection: %v", err)
			}
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		if !stopStarted {
			if err := s.Stop(cleanupCtx); err != nil {
				t.Errorf("cleanup Stop: %v", err)
			}
		}
		if err := s.server.Close(); err != nil {
			t.Errorf("cleanup Close: %v", err)
		}
		joined := make(chan struct{})
		go func() {
			clients.Wait()
			for i, active := range started {
				if active {
					<-handlerDone[i]
				}
			}
			<-startDone
			if stopStarted {
				<-stopDone
			}
			close(joined)
		}()
		select {
		case <-joined:
		case <-cleanupCtx.Done():
			t.Error("timed out joining HTTP/2 lifecycle goroutines")
		}
	})
	wait(ready)
	s.lifecycleMu.Lock()
	addr := s.muxBaseListener.Addr().String()
	s.lifecycleMu.Unlock()
	dialer := tls.Dialer{Config: &tls.Config{
		RootCAs: roots, ServerName: "readiness.test", NextProtos: []string{"h2"}, MinVersion: tls.VersionTLS12,
	}}
	raw, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	conn = raw.(*tls.Conn)
	if state := conn.ConnectionState(); state.NegotiatedProtocol != "h2" || len(state.VerifiedChains) == 0 {
		t.Fatalf("TLS did not negotiate verified h2: protocol %q", state.NegotiatedProtocol)
	}
	transport := &http2.Transport{}
	clientConn, err := transport.NewClientConn(conn)
	if err != nil {
		t.Fatal(err)
	}
	for i, path := range []string{"/first", "/second"} {
		clients.Add(1)
		go func() {
			defer clients.Done()
			defer close(bodies[i])
			req, errRequest := http.NewRequestWithContext(ctx, http.MethodGet, "https://"+addr+path, nil)
			if errRequest != nil {
				clientErrors[i] = errRequest
				return
			}
			resp, errRoundTrip := clientConn.RoundTrip(req)
			if errRoundTrip != nil {
				clientErrors[i] = errRoundTrip
				return
			}
			defer func() {
				if errClose := resp.Body.Close(); errClose != nil && clientErrors[i] == nil {
					clientErrors[i] = errClose
				}
			}()
			if resp.ProtoMajor != 2 || resp.StatusCode != http.StatusOK {
				clientErrors[i] = fmt.Errorf("response = %s %d", resp.Proto, resp.StatusCode)
				return
			}
			prefix := make([]byte, len(first))
			if _, errRead := io.ReadFull(resp.Body, prefix); errRead != nil {
				clientErrors[i] = errRead
				return
			}
			close(prefixes[i])
			suffix, errRead := io.ReadAll(resp.Body)
			responseBodies[i], clientErrors[i] = string(prefix)+string(suffix), errRead
		}()
	}
	for i := range prefixes {
		select {
		case <-prefixes[i]:
		case <-bodies[i]:
			t.Fatalf("stream %d ended before first chunk: %v", i, clientErrors[i])
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		wait(entered[i])
	}
	stopStarted = true
	go func() { defer close(stopDone); stopErr = s.Stop(shutdown) }()
	wait(shutdown.observed)
	for i := range gates {
		if cancelAt != 0 || i == 0 {
			select {
			case <-stopDone:
				t.Fatalf("Stop returned with %d active streams: %v", 2-i, stopErr)
			default:
			}
		}
		select {
		case <-requestContexts[i].Done():
			t.Fatalf("Stop canceled stream %d", i)
		default:
		}
		if cancelAt == i {
			// Once the shutdown context ends, Stop force-closes the remaining
			// streams (upstream #6094) and reports a clean stop.
			cancelStop()
			wait(stopDone)
			if stopErr != nil {
				t.Fatalf("Stop after cancellation = %v, want nil", stopErr)
			}
			for j := i; j < len(gates); j++ {
				release[j]()
				wait(bodies[j])
				if clientErrors[j] == nil && responseBodies[j] == first+last {
					t.Fatalf("stream %d completed after Stop force-closed it", j)
				}
			}
			wait(startDone)
			if startErr != nil {
				t.Fatalf("StartReady: %v", startErr)
			}
			return
		}
		release[i]()
		wait(bodies[i])
		wait(handlerDone[i])
		if clientErrors[i] != nil || handlerErrors[i] != nil || responseBodies[i] != first+last {
			t.Fatalf("stream %d: body %q, client error %v, handler error %v", i, responseBodies[i], clientErrors[i], handlerErrors[i])
		}
	}
	wait(stopDone)
	if cancelAt < 0 && stopErr != nil {
		t.Fatalf("Stop: %v", stopErr)
	}
	wait(startDone)
	if startErr != nil {
		t.Fatalf("StartReady: %v", startErr)
	}
}

func http2ReadinessTLS(t *testing.T) (*config.Config, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), DNSNames: []string{"readiness.test"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.Config{}
	cfg.TLS.Enable = true
	dir := t.TempDir()
	cfg.TLS.Cert, cfg.TLS.Key = filepath.Join(dir, "server.crt"), filepath.Join(dir, "server.key")
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	for path, data := range map[string][]byte{cfg.TLS.Cert: certPEM, cfg.TLS.Key: pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})} {
		if errWrite := os.WriteFile(path, data, 0600); errWrite != nil {
			t.Fatal(errWrite)
		}
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certPEM) {
		t.Fatal("failed to trust generated certificate")
	}
	return cfg, roots
}
