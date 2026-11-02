package management

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/pluginhost"
)

func TestAuthenticateManagementKey_LocalhostNeverBanned(t *testing.T) {
	h := &Handler{
		cfg:            &config.Config{},
		failedAttempts: make(map[string]*attemptInfo),
		envSecret:      "test-secret",
	}

	for i := 0; i < 5; i++ {
		allowed, statusCode, errMsg := h.AuthenticateManagementKey("127.0.0.1", true, "wrong-secret")
		if allowed {
			t.Fatalf("expected auth to be denied at attempt %d", i+1)
		}
		if statusCode != http.StatusUnauthorized || errMsg != "invalid management key" {
			t.Fatalf("unexpected auth failure at attempt %d: status=%d msg=%q", i+1, statusCode, errMsg)
		}
	}

	allowed, statusCode, errMsg := h.AuthenticateManagementKey("127.0.0.1", true, "test-secret")
	if !allowed || statusCode != 0 || errMsg != "" {
		t.Fatalf("expected localhost correct key to remain accepted: allowed=%t status=%d msg=%q", allowed, statusCode, errMsg)
	}
}

func TestMiddlewareSetsSupportPluginHeader(t *testing.T) {

	h := &Handler{
		cfg:            &config.Config{},
		failedAttempts: make(map[string]*attemptInfo),
		envSecret:      "test-secret",
	}
	middleware := h.Middleware()

	t.Run("invalid key", func(t *testing.T) {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodGet, "/v0/management/config", nil)
		c.Request.RemoteAddr = "127.0.0.1:12345"
		c.Request.Header.Set("X-Management-Key", "wrong-secret")

		middleware(c)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
		}
		if got := rec.Header().Get("X-CPA-SUPPORT-PLUGIN"); got != pluginhost.SupportPluginHeaderValue() {
			t.Fatalf("X-CPA-SUPPORT-PLUGIN = %q, want %q", got, pluginhost.SupportPluginHeaderValue())
		}
	})

	t.Run("valid key", func(t *testing.T) {
		engine := gin.New()
		engine.GET("/v0/management/config", middleware, func(c *gin.Context) {
			c.Status(http.StatusOK)
		})

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/v0/management/config", nil)
		req.RemoteAddr = "127.0.0.1:12345"
		req.Header.Set("X-Management-Key", "test-secret")
		engine.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
		}
		if got := rec.Header().Get("X-CPA-SUPPORT-PLUGIN"); got != pluginhost.SupportPluginHeaderValue() {
			t.Fatalf("X-CPA-SUPPORT-PLUGIN = %q, want %q", got, pluginhost.SupportPluginHeaderValue())
		}
	})
}

func TestAuthenticateManagementKey_TrustedIPNeverBanned(t *testing.T) {
	h := &Handler{
		cfg:            &config.Config{RemoteManagement: config.RemoteManagement{AllowRemote: true, TrustedIPs: []string{"100.71.30.111"}}},
		failedAttempts: make(map[string]*attemptInfo),
		envSecret:      "test-secret",
	}

	for i := 0; i < 6; i++ {
		allowed, statusCode, errMsg := h.AuthenticateManagementKey("100.71.30.111", false, "wrong-secret")
		if allowed || statusCode != http.StatusUnauthorized || errMsg != "invalid management key" {
			t.Fatalf("trusted attempt %d: allowed=%t status=%d message=%q", i+1, allowed, statusCode, errMsg)
		}
	}
}

func TestAuthenticateManagementKey_UntrustedIPStillBanned(t *testing.T) {
	h := &Handler{
		cfg:            &config.Config{RemoteManagement: config.RemoteManagement{AllowRemote: true, TrustedIPs: []string{"100.71.30.111"}}},
		failedAttempts: make(map[string]*attemptInfo),
		envSecret:      "test-secret",
	}

	for i := 0; i < 5; i++ {
		_, statusCode, _ := h.AuthenticateManagementKey("100.71.30.112", false, "wrong-secret")
		if statusCode != http.StatusUnauthorized {
			t.Fatalf("untrusted attempt %d status = %d, want %d", i+1, statusCode, http.StatusUnauthorized)
		}
	}
	_, statusCode, _ := h.AuthenticateManagementKey("100.71.30.112", false, "test-secret")
	if statusCode != http.StatusForbidden {
		t.Fatalf("untrusted client status = %d, want %d", statusCode, http.StatusForbidden)
	}
}
