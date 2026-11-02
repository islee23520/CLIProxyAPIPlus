package management

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func useLatestVersionEndpoints(t *testing.T, apiURL, pageURL string) {
	t.Helper()
	prevAPI, prevPage := latestReleaseURL, latestReleasePageURL
	latestReleaseURL, latestReleasePageURL = apiURL, pageURL
	resetLatestVersionCache()
	t.Cleanup(func() {
		latestReleaseURL, latestReleasePageURL = prevAPI, prevPage
		resetLatestVersionCache()
	})
}

func resetLatestVersionCache() {
	latestVersionCache.Lock()
	latestVersionCache.version = ""
	latestVersionCache.fetchedAt = time.Time{}
	latestVersionCache.Unlock()
}

func callGetLatestVersion(t *testing.T) (int, map[string]string) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodGet, "/v0/management/latest-version", nil)
	(&Handler{}).GetLatestVersion(c)
	var body map[string]string
	if errDecode := json.Unmarshal(rec.Body.Bytes(), &body); errDecode != nil {
		t.Fatalf("decode response %q: %v", rec.Body.String(), errDecode)
	}
	return rec.Code, body
}

func TestGetLatestVersionFallsBackToReleasePageWhenAPIRateLimited(t *testing.T) {
	apiCalls := 0
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		apiCalls++
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"message":"You have triggered an abuse detection mechanism."}`))
	}))
	defer api.Close()
	page := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/owner/repo/releases/tag/v9.9.9-1", http.StatusFound)
	}))
	defer page.Close()
	useLatestVersionEndpoints(t, api.URL, page.URL+"/owner/repo/releases/latest")

	code, body := callGetLatestVersion(t)
	if code != http.StatusOK || body["latest-version"] != "v9.9.9-1" {
		t.Fatalf("got %d %v, want 200 latest-version=v9.9.9-1", code, body)
	}

	code, body = callGetLatestVersion(t)
	if code != http.StatusOK || body["latest-version"] != "v9.9.9-1" || apiCalls != 1 {
		t.Fatalf("got %d %v apiCalls=%d, want cached v9.9.9-1 with 1 API call", code, body, apiCalls)
	}
}

func TestGetLatestVersionReportsAPIErrorWhenFallbackFails(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer api.Close()
	page := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer page.Close()
	useLatestVersionEndpoints(t, api.URL, page.URL)

	code, body := callGetLatestVersion(t)
	if code != http.StatusBadGateway || body["error"] != "unexpected_status" {
		t.Fatalf("got %d %v, want 502 unexpected_status", code, body)
	}
}
