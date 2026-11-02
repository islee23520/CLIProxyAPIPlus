//go:build linux

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	logger "github.com/sirupsen/logrus"
)

type signalDrainHook struct {
	draining chan struct{}
	once     sync.Once
}

func (*signalDrainHook) Levels() []logger.Level { return []logger.Level{logger.InfoLevel} }

func (hook *signalDrainHook) Fire(entry *logger.Entry) error {
	if entry.Data["event"] == "draining" {
		hook.once.Do(func() { close(hook.draining) })
	}
	return nil
}

// The helper shares both the signal registration and serving path with run.
// Only binding differs: an inherited listener avoids a reserve/rebind race.
func TestRelaySignalHelper(t *testing.T) {
	if os.Getenv("RELAY_SIGNAL_HELPER") != "1" {
		return
	}
	logger.SetOutput(os.Stdout)
	logger.SetFormatter(&logger.JSONFormatter{DisableTimestamp: true})
	file := os.NewFile(3, "relay-listener")
	listener, err := net.FileListener(file)
	if errClose := file.Close(); err != nil || errClose != nil {
		os.Exit(2)
	}
	controller, err := newGenerationController(os.Getenv("RELAY_SIGNAL_UPSTREAM"))
	if err != nil {
		os.Exit(2)
	}
	shutdown, stop := relayShutdownSignal()
	helperDone := make(chan struct{})
	observerDone := make(chan struct{})
	if os.Getenv("RELAY_SIGNAL_MODE") == "repeated-sigterm" {
		hook := &signalDrainHook{draining: make(chan struct{})}
		logger.AddHook(hook)
		go func() {
			defer close(observerDone)
			select {
			case <-hook.draining:
			case <-helperDone:
				return
			}
			for sequence := 1; sequence <= 2; sequence++ {
				select {
				case <-shutdown:
					logger.WithField("event", fmt.Sprintf("repeated_sigterm_%d", sequence)).Info("relay test signal observed")
				case <-helperDone:
					return
				}
			}
		}()
	} else {
		close(observerDone)
	}
	err = serveRelay(listener, controller, shutdown)
	close(helperDone)
	<-observerDone
	stop()
	if err != nil {
		os.Exit(2)
	}
	os.Exit(0)
}

func TestRelaySignalPreservesStreams(t *testing.T) {
	for _, mode := range []string{"sse", "websocket", "sse-first", "websocket-first", "repeated-sigterm"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			next := make(chan string)
			finish := make(chan struct{})
			upstreamSSEDone := make(chan struct{})
			upstreamWSDone := make(chan struct{})
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v1/responses" {
					http.NotFound(w, r)
					return
				}
				if websocket.IsWebSocketUpgrade(r) {
					defer close(upstreamWSDone)
					upgrader := websocket.Upgrader{}
					conn, err := upgrader.Upgrade(w, r, nil)
					if err != nil {
						return
					}
					defer conn.Close()
					stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
					defer stop()
					for {
						kind, payload, err := conn.ReadMessage()
						if err != nil {
							return
						}
						if err := conn.WriteMessage(kind, payload); err != nil {
							return
						}
					}
				}
				defer close(upstreamSSEDone)
				w.Header().Set("Content-Type", "text/event-stream")
				if _, err := io.WriteString(w, "data: established\n\n"); err != nil {
					return
				}
				w.(http.Flusher).Flush()
				for {
					select {
					case nonce := <-next:
						if _, err := fmt.Fprintf(w, "data: %s\n\n", nonce); err != nil {
							return
						}
						w.(http.Flusher).Flush()
					case <-finish:
						return
					case <-r.Context().Done():
						return
					case <-ctx.Done():
						return
					}
				}
			}))
			t.Cleanup(upstream.Close)
			listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			address := listener.Addr().String()
			file, err := listener.File()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = file.Close() })
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			cmd := exec.CommandContext(ctx, executable, "-test.run=^TestRelaySignalHelper$")
			cmd.Dir = t.TempDir()
			cmd.Env = []string{"RELAY_SIGNAL_HELPER=1", "RELAY_SIGNAL_UPSTREAM=" + upstream.URL, "RELAY_SIGNAL_MODE=" + mode, "HOME=" + cmd.Dir, "TMPDIR=" + cmd.Dir}
			cmd.ExtraFiles = []*os.File{file}
			output, outputWriter, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = output.Close(); _ = outputWriter.Close() })
			cmd.Stdout, cmd.Stderr = outputWriter, outputWriter
			events := make(chan string, 8)
			go func() {
				defer close(events)
				scanner := bufio.NewScanner(output)
				for scanner.Scan() {
					var event struct {
						Event string `json:"event"`
					}
					if json.Unmarshal(scanner.Bytes(), &event) != nil {
						continue
					}
					select {
					case events <- event.Event:
					case <-ctx.Done():
						return
					}
				}
			}()
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			_ = outputWriter.Close()
			_ = file.Close()
			_ = listener.Close()
			processDone := make(chan struct{})
			var processErr error
			go func() { processErr = cmd.Wait(); close(processDone) }()
			t.Cleanup(func() {
				cancel()
				select {
				case <-processDone:
				case <-time.After(5 * time.Second):
					t.Error("owned relay subprocess did not exit after cancellation")
				}
			})
			await := func(ch <-chan struct{}) {
				t.Helper()
				select {
				case <-ch:
				case <-ctx.Done():
					t.Fatal("relay test event deadline exceeded")
				}
			}
			awaitEvent := func(expected string) {
				t.Helper()
				for {
					select {
					case event, ok := <-events:
						if !ok {
							t.Fatal("relay lifecycle output ended before expected event")
						}
						if event == expected {
							return
						}
					case <-ctx.Done():
						t.Fatal("relay lifecycle event deadline exceeded")
					}
				}
			}
			awaitEvent("ready")
			var response *http.Response
			var reader *bufio.Reader
			readSSE := func(nonce string) {
				t.Helper()
				for _, want := range []string{"data: " + nonce + "\n", "\n"} {
					line, err := reader.ReadString('\n')
					if err != nil || line != want {
						t.Fatalf("SSE event mismatch: %v", err)
					}
				}
			}
			if mode != "websocket" {
				request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+address+"/v1/responses", nil)
				if err != nil {
					t.Fatal(err)
				}
				transport := &http.Transport{DisableKeepAlives: true}
				t.Cleanup(transport.CloseIdleConnections)
				response, err = (&http.Client{Transport: transport}).Do(request)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = response.Body.Close() })
				if response.StatusCode != http.StatusOK {
					t.Fatal("SSE response status is not OK")
				}
				reader = bufio.NewReader(response.Body)
				readSSE("established")
			}
			var conn *websocket.Conn
			var closeWS func()
			echo := func(nonce string) {
				t.Helper()
				if err := conn.WriteMessage(websocket.BinaryMessage, []byte(nonce)); err != nil {
					t.Fatal(err)
				}
				kind, payload, err := conn.ReadMessage()
				if err != nil || kind != websocket.BinaryMessage || string(payload) != nonce {
					t.Fatalf("WebSocket echo mismatch: %v", err)
				}
			}
			if mode != "sse" {
				dialer := websocket.Dialer{}
				conn, _, err = dialer.DialContext(ctx, "ws://"+address+"/v1/responses", nil)
				if err != nil {
					t.Fatal(err)
				}
				var once sync.Once
				closeWS = func() { once.Do(func() { _ = conn.Close() }) }
				t.Cleanup(closeWS)
				stop := context.AfterFunc(ctx, closeWS)
				t.Cleanup(func() { stop() })
				echo("established")
			}
			if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
				t.Fatal(err)
			}
			awaitEvent("draining")
			probeSSE := func(nonce string) {
				t.Helper()
				select {
				case next <- nonce:
				case <-ctx.Done():
					t.Fatal("SSE producer deadline exceeded")
				}
				readSSE(nonce)
			}
			if response != nil {
				probeSSE("after-signal")
			}
			if conn != nil {
				echo("after-signal")
			}
			if mode == "repeated-sigterm" {
				// Each acknowledgement confirms a channel receive, not channel saturation.
				for sequence := 1; sequence <= 2; sequence++ {
					if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
						t.Fatal(err)
					}
					awaitEvent(fmt.Sprintf("repeated_sigterm_%d", sequence))
					probeSSE(fmt.Sprintf("sse-after-repeated-sigterm-%d", sequence))
					echo(fmt.Sprintf("websocket-after-repeated-sigterm-%d", sequence))
				}
			}
			finishSSE := func() {
				t.Helper()
				close(finish)
				if rest, err := io.ReadAll(reader); err != nil || len(rest) != 0 {
					t.Fatalf("SSE completion mismatch: %v", err)
				}
				_ = response.Body.Close()
				await(upstreamSSEDone)
				awaitEvent("http_drained")
			}
			finishWS := func() {
				t.Helper()
				if err := conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")); err != nil {
					t.Fatal(err)
				}
				if _, _, err := conn.ReadMessage(); !websocket.IsCloseError(err, websocket.CloseNormalClosure) {
					t.Fatalf("WebSocket close handshake: %v", err)
				}
				closeWS()
				await(upstreamWSDone)
			}
			switch mode {
			case "sse":
				finishSSE()
			case "websocket":
				awaitEvent("http_drained")
				echo("after-http-drained")
				finishWS()
			case "sse-first", "repeated-sigterm":
				finishSSE()
				echo("after-sse-closed")
				finishWS()
			case "websocket-first":
				finishWS()
				probeSSE("after-websocket-closed")
				finishSSE()
			}
			await(processDone)
			if processErr != nil {
				t.Fatalf("relay subprocess exit: %v", processErr)
			}
		})
	}
}
