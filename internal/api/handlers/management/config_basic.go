package management

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/util"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
	log "github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

// The original router-for-me/CLIProxyAPIPlus repository was deleted; releases are
// published by the maintained fork (see README-ccs-fork.md).
var (
	latestReleaseURL     = "https://api.github.com/repos/kaitranntt/CLIProxyAPIPlus/releases/latest"
	latestReleasePageURL = "https://github.com/kaitranntt/CLIProxyAPIPlus/releases/latest"
)

const (
	latestReleaseUserAgent = "CLIProxyAPIPlus"
	latestVersionCacheTTL  = 10 * time.Minute
)

// latestVersionCache keeps the last successful lookup so repeated update checks
// do not exhaust the unauthenticated GitHub API rate limit.
var latestVersionCache struct {
	sync.Mutex
	version   string
	fetchedAt time.Time
}

type latestVersionError struct {
	code    string
	message string
}

func (e *latestVersionError) Error() string { return e.code + ": " + e.message }

func (h *Handler) GetConfig(c *gin.Context) {
	if h == nil || h.cfg == nil {
		c.JSON(200, gin.H{})
		return
	}
	c.JSON(200, new(*h.cfg))
}

type releaseInfo struct {
	TagName string `json:"tag_name"`
	Name    string `json:"name"`
}

func setLatestReleaseRequestHeaders(req *http.Request) {
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", latestReleaseUserAgent)
	if token := util.ResolveGitHubToken(); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
}

// GetLatestVersion returns the latest release version from GitHub without downloading assets.
// When the GitHub API fails (for example 403/429 rate limiting), it falls back to the
// redirect of the public releases/latest page, which is not subject to API rate limits.
func (h *Handler) GetLatestVersion(c *gin.Context) {
	latestVersionCache.Lock()
	defer latestVersionCache.Unlock()
	if latestVersionCache.version != "" && time.Since(latestVersionCache.fetchedAt) < latestVersionCacheTTL {
		c.JSON(http.StatusOK, gin.H{"latest-version": latestVersionCache.version})
		return
	}

	client := &http.Client{Timeout: 10 * time.Second}
	proxyURL := ""
	if h != nil && h.cfg != nil {
		proxyURL = strings.TrimSpace(h.cfg.ProxyURL)
	}
	if proxyURL != "" {
		sdkCfg := &sdkconfig.SDKConfig{ProxyURL: proxyURL}
		util.SetProxy(sdkCfg, client)
	}

	version, errAPI := fetchLatestVersionFromAPI(c.Request.Context(), client)
	if errAPI != nil {
		pageVersion, errPage := fetchLatestVersionFromReleasePage(c.Request.Context(), client)
		if errPage != nil {
			log.WithError(errPage).Debug("latest version release page fallback failed")
			status := http.StatusBadGateway
			if errAPI.code == "request_create_failed" {
				status = http.StatusInternalServerError
			}
			c.JSON(status, gin.H{"error": errAPI.code, "message": errAPI.message})
			return
		}
		version = pageVersion
	}

	latestVersionCache.version = version
	latestVersionCache.fetchedAt = time.Now()
	c.JSON(http.StatusOK, gin.H{"latest-version": version})
}

func fetchLatestVersionFromAPI(ctx context.Context, client *http.Client) (string, *latestVersionError) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, latestReleaseURL, nil)
	if err != nil {
		return "", &latestVersionError{code: "request_create_failed", message: err.Error()}
	}
	setLatestReleaseRequestHeaders(req)

	resp, err := client.Do(req)
	if err != nil {
		return "", &latestVersionError{code: "request_failed", message: err.Error()}
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.WithError(errClose).Debug("failed to close latest version response body")
		}
	}()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return "", &latestVersionError{code: "unexpected_status", message: fmt.Sprintf("status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))}
	}

	var info releaseInfo
	if errDecode := json.NewDecoder(resp.Body).Decode(&info); errDecode != nil {
		return "", &latestVersionError{code: "decode_failed", message: errDecode.Error()}
	}

	version := strings.TrimSpace(info.TagName)
	if version == "" {
		version = strings.TrimSpace(info.Name)
	}
	if version == "" {
		return "", &latestVersionError{code: "invalid_response", message: "missing release version"}
	}
	return version, nil
}

// fetchLatestVersionFromReleasePage resolves the latest tag from the
// "/releases/latest" -> "/releases/tag/<tag>" redirect of the GitHub web UI.
func fetchLatestVersionFromReleasePage(ctx context.Context, client *http.Client) (string, error) {
	noRedirect := *client
	noRedirect.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, latestReleasePageURL, nil)
	if err != nil {
		return "", fmt.Errorf("create release page request: %w", err)
	}
	req.Header.Set("User-Agent", latestReleaseUserAgent)

	resp, err := noRedirect.Do(req)
	if err != nil {
		return "", fmt.Errorf("request release page: %w", err)
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.WithError(errClose).Debug("failed to close release page response body")
		}
	}()

	location, err := resp.Location()
	if err != nil {
		return "", fmt.Errorf("release page returned status %d without redirect", resp.StatusCode)
	}
	_, tag, found := strings.Cut(location.Path, "/releases/tag/")
	tag = strings.Trim(tag, "/")
	if !found || tag == "" {
		return "", fmt.Errorf("unexpected release page redirect %q", location.String())
	}
	return tag, nil
}

func WriteConfig(path string, data []byte) error {
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return err
	}
	if len(doc.Content) > 0 && config.IsV8ConfigLayout(doc.Content[0]) {
		var err error
		data, _, err = config.NormalizeConfigLayout(data, true)
		if err != nil {
			return err
		}
	}
	data = config.NormalizeCommentIndentation(data)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, errWrite := f.Write(data); errWrite != nil {
		_ = f.Close()
		return errWrite
	}
	if errSync := f.Sync(); errSync != nil {
		_ = f.Close()
		return errSync
	}
	return f.Close()
}

func (h *Handler) PutConfigYAML(c *gin.Context) {
	body, err := io.ReadAll(c.Request.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_yaml", "message": "cannot read request body"})
		return
	}
	var cfg config.Config
	if err = yaml.Unmarshal(body, &cfg); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid_yaml", "message": err.Error()})
		return
	}
	// Validate config using LoadConfigOptional with optional=false to enforce parsing
	tmpDir := filepath.Dir(h.configFilePath)
	tmpFile, err := os.CreateTemp(tmpDir, "config-validate-*.yaml")
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "write_failed", "message": err.Error()})
		return
	}
	tempFile := tmpFile.Name()
	if _, errWrite := tmpFile.Write(body); errWrite != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tempFile)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "write_failed", "message": errWrite.Error()})
		return
	}
	if errClose := tmpFile.Close(); errClose != nil {
		_ = os.Remove(tempFile)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "write_failed", "message": errClose.Error()})
		return
	}
	defer func() {
		_ = os.Remove(tempFile)
	}()
	_, err = config.LoadConfigOptional(tempFile, false)
	if err != nil {
		c.JSON(http.StatusUnprocessableEntity, gin.H{"error": "invalid_config", "message": err.Error()})
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if WriteConfig(h.configFilePath, body) != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "write_failed", "message": "failed to write config"})
		return
	}
	// Reload into handler to keep memory in sync
	newCfg, err := config.LoadConfig(h.configFilePath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "reload_failed", "message": err.Error()})
		return
	}
	h.cfg = newCfg
	c.JSON(http.StatusOK, gin.H{"ok": true, "changed": []string{"config"}})
}

// GetConfigYAML returns the raw config.yaml file bytes without re-encoding.
// It preserves comments and original formatting/styles.
func (h *Handler) GetConfigYAML(c *gin.Context) {
	data, err := os.ReadFile(h.configFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			c.JSON(http.StatusNotFound, gin.H{"error": "not_found", "message": "config file not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "read_failed", "message": err.Error()})
		return
	}
	c.Header("Content-Type", "application/yaml; charset=utf-8")
	c.Header("Cache-Control", "no-store")
	c.Header("X-Content-Type-Options", "nosniff")
	// Write raw bytes as-is
	_, _ = c.Writer.Write(data)
}

// Debug
func (h *Handler) GetDebug(c *gin.Context) { c.JSON(200, gin.H{"debug": h.cfg.Debug}) }
func (h *Handler) PutDebug(c *gin.Context) { h.updateBoolField(c, func(v bool) { h.cfg.Debug = v }) }

// UsageStatisticsEnabled
func (h *Handler) GetUsageStatisticsEnabled(c *gin.Context) {
	c.JSON(200, gin.H{"usage-statistics-enabled": h.cfg.UsageStatisticsEnabled})
}
func (h *Handler) PutUsageStatisticsEnabled(c *gin.Context) {
	h.updateBoolField(c, func(v bool) { h.cfg.UsageStatisticsEnabled = v })
}

// UsageStatisticsEnabled
func (h *Handler) GetLoggingToFile(c *gin.Context) {
	c.JSON(200, gin.H{"logging-to-file": h.cfg.LoggingToFile})
}
func (h *Handler) PutLoggingToFile(c *gin.Context) {
	h.updateBoolField(c, func(v bool) { h.cfg.LoggingToFile = v })
}

// LogsMaxTotalSizeMB
func (h *Handler) GetLogsMaxTotalSizeMB(c *gin.Context) {
	c.JSON(200, gin.H{"logs-max-total-size-mb": h.cfg.LogsMaxTotalSizeMB})
}
func (h *Handler) PutLogsMaxTotalSizeMB(c *gin.Context) {
	var body struct {
		Value *int `json:"value"`
	}
	if errBindJSON := c.ShouldBindJSON(&body); errBindJSON != nil || body.Value == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	value := *body.Value
	if value < 0 {
		value = 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cfg.LogsMaxTotalSizeMB = value
	h.persistLocked(c)
}

// ErrorLogsMaxFiles
func (h *Handler) GetErrorLogsMaxFiles(c *gin.Context) {
	c.JSON(200, gin.H{"error-logs-max-files": h.cfg.ErrorLogsMaxFiles})
}
func (h *Handler) PutErrorLogsMaxFiles(c *gin.Context) {
	var body struct {
		Value *int `json:"value"`
	}
	if errBindJSON := c.ShouldBindJSON(&body); errBindJSON != nil || body.Value == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	value := *body.Value
	if value < 0 {
		value = 10
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cfg.ErrorLogsMaxFiles = value
	h.persistLocked(c)
}

// Request log
func (h *Handler) GetRequestLog(c *gin.Context) { c.JSON(200, gin.H{"request-log": h.cfg.RequestLog}) }
func (h *Handler) PutRequestLog(c *gin.Context) {
	h.updateBoolField(c, func(v bool) { h.cfg.RequestLog = v })
}

// Websocket auth
func (h *Handler) GetWebsocketAuth(c *gin.Context) {
	c.JSON(200, gin.H{"ws-auth": h.cfg.WebsocketAuth})
}
func (h *Handler) PutWebsocketAuth(c *gin.Context) {
	h.updateBoolField(c, func(v bool) { h.cfg.WebsocketAuth = v })
}

// Request retry
func (h *Handler) GetRequestRetry(c *gin.Context) {
	c.JSON(200, gin.H{"request-retry": h.cfg.RequestRetry})
}
func (h *Handler) PutRequestRetry(c *gin.Context) {
	h.updateIntField(c, func(v int) { h.cfg.RequestRetry = v })
}

// Max retry credentials
func (h *Handler) GetMaxRetryCredentials(c *gin.Context) {
	c.JSON(200, gin.H{"max-retry-credentials": h.cfg.MaxRetryCredentials})
}
func (h *Handler) PutMaxRetryCredentials(c *gin.Context) {
	h.updateIntField(c, func(v int) { h.cfg.MaxRetryCredentials = v })
}

// Max retry interval
func (h *Handler) GetMaxRetryInterval(c *gin.Context) {
	c.JSON(200, gin.H{"max-retry-interval": h.cfg.MaxRetryInterval})
}
func (h *Handler) PutMaxRetryInterval(c *gin.Context) {
	h.updateIntField(c, func(v int) { h.cfg.MaxRetryInterval = v })
}

// ForceModelPrefix
func (h *Handler) GetForceModelPrefix(c *gin.Context) {
	c.JSON(200, gin.H{"force-model-prefix": h.cfg.ForceModelPrefix})
}
func (h *Handler) PutForceModelPrefix(c *gin.Context) {
	h.updateBoolField(c, func(v bool) { h.cfg.ForceModelPrefix = v })
}

func normalizeRoutingStrategy(strategy string) (string, bool) {
	normalized := strings.ToLower(strings.TrimSpace(strategy))
	switch normalized {
	case "", "round-robin", "roundrobin", "rr":
		return "round-robin", true
	case "weighted-round-robin", "weightedroundrobin", "wrr":
		return "weighted-round-robin", true
	case "fill-first", "fillfirst", "ff":
		return "fill-first", true
	default:
		return "", false
	}
}

// RoutingStrategy
func (h *Handler) GetRoutingStrategy(c *gin.Context) {
	strategy, ok := normalizeRoutingStrategy(h.cfg.Routing.Strategy)
	if !ok {
		c.JSON(200, gin.H{"strategy": strings.TrimSpace(h.cfg.Routing.Strategy)})
		return
	}
	c.JSON(200, gin.H{"strategy": strategy})
}
func (h *Handler) PutRoutingStrategy(c *gin.Context) {
	var body struct {
		Value *string `json:"value"`
	}
	if errBindJSON := c.ShouldBindJSON(&body); errBindJSON != nil || body.Value == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid body"})
		return
	}
	normalized, ok := normalizeRoutingStrategy(*body.Value)
	if !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid strategy"})
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cfg.Routing.Strategy = normalized
	h.persistLocked(c)
}

// Proxy URL
func (h *Handler) GetProxyURL(c *gin.Context) { c.JSON(200, gin.H{"proxy-url": h.cfg.ProxyURL}) }
func (h *Handler) PutProxyURL(c *gin.Context) {
	h.updateStringField(c, func(v string) { h.cfg.ProxyURL = v })
}
func (h *Handler) DeleteProxyURL(c *gin.Context) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.cfg.ProxyURL = ""
	h.persistLocked(c)
}
