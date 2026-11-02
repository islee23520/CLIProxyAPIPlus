package openai

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	apihandlers "github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
)

func TestOpenAIModelsPreservesContextWindowMetadata(t *testing.T) {
	gin.SetMode(gin.TestMode)

	models := []*registry.ModelInfo{
		{
			ID:                  "test-openai-models-ctx",
			OwnedBy:             "openai",
			ContextLength:       372000,
			MaxCompletionTokens: 128000,
		},
		{
			ID:      "test-openai-models-noctx",
			OwnedBy: "openai",
		},
	}
	globalRegistry := registry.GetGlobalRegistry()
	globalRegistry.RegisterClient("test-openai-models-handler", "openai", models)
	t.Cleanup(func() { globalRegistry.UnregisterClient("test-openai-models-handler") })

	handler := NewOpenAIAPIHandler(apihandlers.NewBaseAPIHandlers(nil, nil))
	router := gin.New()
	router.GET("/v1/models", handler.OpenAIModels)

	resp := httptest.NewRecorder()
	router.ServeHTTP(resp, httptest.NewRequest(http.MethodGet, "/v1/models", nil))
	if resp.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.Code, http.StatusOK)
	}

	var payload struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(resp.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	byID := make(map[string]map[string]any, len(payload.Data))
	for _, model := range payload.Data {
		byID[model["id"].(string)] = model
	}

	withCtx, ok := byID["test-openai-models-ctx"]
	if !ok {
		t.Fatalf("model test-openai-models-ctx missing from %v", payload.Data)
	}
	if got := int(withCtx["context_length"].(float64)); got != 372000 {
		t.Fatalf("context_length = %d, want 372000", got)
	}
	if got := int(withCtx["max_completion_tokens"].(float64)); got != 128000 {
		t.Fatalf("max_completion_tokens = %d, want 128000", got)
	}

	withoutCtx, ok := byID["test-openai-models-noctx"]
	if !ok {
		t.Fatalf("model test-openai-models-noctx missing from %v", payload.Data)
	}
	if _, exists := withoutCtx["context_length"]; exists {
		t.Fatalf("context_length unexpectedly present for model without context info: %v", withoutCtx)
	}
}
