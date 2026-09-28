package openai

import (
	"bytes"
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/tidwall/gjson"
)

func TestProviderScopedCodexImageGuard(t *testing.T) {
	tests := []struct {
		name string
		path string
		body string
		want bool
	}{
		{name: "GPT image path", path: "/v1/images/generations", body: `{"model":"gpt-image-2","prompt":"x"}`, want: true},
		{name: "Codex response tool", path: "/v1/responses", body: `{"model":"gpt-5.6-sol","tools":[{"type":"image_generation"}]}`, want: true},
		{name: "OpenAI function tool", path: "/v1/chat/completions", body: `{"model":"gpt-5.4-mini","tools":[{"type":"function","function":{"name":"image_generation"}}]}`, want: true},
		{name: "Grok image path", path: "/v1/images/generations", body: `{"model":"grok-imagine-image","prompt":"x"}`, want: false},
		{name: "Grok response tool", path: "/v1/responses", body: `{"model":"grok-4.3","tools":[{"type":"image_generation"}]}`, want: false},
		{name: "Gemini response tool", path: "/v1/responses", body: `{"model":"gemini-3-pro-image","tools":[{"type":"image_generation"}]}`, want: false},
		{name: "Claude response tool", path: "/v1/responses", body: `{"model":"claude-opus-4-6","tools":[{"type":"image_generation"}]}`, want: false},
		{name: "model omitted", path: "/v1/images/generations", body: `{"prompt":"x"}`, want: false},
		{name: "vision input is not generation", path: "/v1/responses", body: `{"model":"gpt-5.6-sol","input":[{"type":"input_image","image_url":"x"}]}`, want: false},
	}
	for _, testCase := range tests {
		t.Run(testCase.name, func(t *testing.T) {
			if got := providerScopedCodexImageRequest(testCase.path, []byte(testCase.body)); got != testCase.want {
				t.Fatalf("providerScopedCodexImageRequest() = %v, want %v", got, testCase.want)
			}
		})
	}
}

func TestProviderScopedImageDefaultKeepsModelLessGrokTestingAvailable(t *testing.T) {
	cfg := &internalconfig.SDKConfig{DisableImageGeneration: internalconfig.DisableImageGenerationCodex}
	if got := providerScopedDefaultImageModel(cfg); got != defaultXAIImagesModel {
		t.Fatalf("provider-scoped model-less image default = %q, want %q", got, defaultXAIImagesModel)
	}
	if got := providerScopedDefaultImageModel(&internalconfig.SDKConfig{}); got != defaultImagesToolModel {
		t.Fatalf("ordinary model-less image default = %q, want %q", got, defaultImagesToolModel)
	}
}

func TestSanitizeCodexCompatibilityRequestIsProviderScoped(t *testing.T) {
	codex := []byte(`{"model":"gpt-5.6-sol","tools":[{"type":"function","parameters":{"type":"array","uniqueItems":true,"items":{"type":"string"}}}]}`)
	got, removed := sanitizeCodexCompatibilityRequest(codex)
	if removed != 1 || bytes.Contains(got, []byte("uniqueItems")) {
		t.Fatalf("Codex schema sanitize = %s, removed %d; want uniqueItems removed", got, removed)
	}
	grok := []byte(`{"model":"grok-4.3","tools":[{"type":"function","parameters":{"type":"array","uniqueItems":true}}]}`)
	got, removed = sanitizeCodexCompatibilityRequest(grok)
	if removed != 0 || !bytes.Equal(got, grok) {
		t.Fatalf("Grok schema changed = %s, removed %d", got, removed)
	}
}

func TestSanitizeCodexCompatibilityRequestRemovesSummaryOnlyForSpark(t *testing.T) {
	spark := []byte(`{"model":"gpt-5.3-codex-spark","reasoning":{"effort":"high","summary":"auto"},"input":"hi"}`)
	got, removed := sanitizeCodexCompatibilityRequest(spark)
	if removed != 1 || gjson.GetBytes(got, "reasoning.summary").Exists() {
		t.Fatalf("Spark sanitize = %s, removed %d; want reasoning.summary removed", got, removed)
	}
	sibling := []byte(`{"model":"gpt-5.6-sol","reasoning":{"summary":"auto"},"input":"hi"}`)
	got, removed = sanitizeCodexCompatibilityRequest(sibling)
	if removed != 0 || !bytes.Equal(got, sibling) {
		t.Fatalf("sibling Codex request changed = %s, removed %d", got, removed)
	}
}
