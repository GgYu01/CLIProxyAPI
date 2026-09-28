package openai

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func applyCodexRequestPolicy(c *gin.Context, cfg *internalconfig.SDKConfig, rawJSON []byte) ([]byte, bool) {
	path := ""
	if c != nil && c.Request != nil && c.Request.URL != nil {
		path = c.Request.URL.Path
	}
	if cfg != nil && cfg.DisableImageGeneration == internalconfig.DisableImageGenerationCodex &&
		providerScopedCodexImageRequest(path, rawJSON) {
		c.AbortWithStatusJSON(http.StatusForbidden, handlers.ErrorResponse{
			Error: handlers.ErrorDetail{
				Message: "Codex/OpenAI image generation is disabled on this CPA route",
				Type:    "codex_image_disabled",
			},
		})
		return rawJSON, true
	}

	updated, removed := sanitizeCodexCompatibilityRequest(rawJSON)
	if removed > 0 {
		log.Infof("codex request compatibility normalization removed %d unsupported field(s) for model %s", removed, gjson.GetBytes(rawJSON, "model").String())
	}
	return updated, false
}

func (h *OpenAIAPIHandler) requestPolicyConfig() *internalconfig.SDKConfig {
	if h == nil || h.BaseAPIHandler == nil {
		return nil
	}
	return h.Cfg
}

func (h *OpenAIResponsesAPIHandler) requestPolicyConfig() *internalconfig.SDKConfig {
	if h == nil || h.BaseAPIHandler == nil {
		return nil
	}
	return h.Cfg
}

func providerScopedCodexImageRequest(path string, rawJSON []byte) bool {
	model := normalizedPolicyModel(gjson.GetBytes(rawJSON, "model").String())
	if model == "" || !isCodexOpenAIModelName(model) {
		return false
	}
	path = strings.ToLower(strings.TrimSuffix(strings.SplitN(path, "?", 2)[0], "/"))
	if strings.Contains(path, "/v1/images") || strings.Contains(path, "/images/") {
		return true
	}
	if hasImageGenerationIntent(rawJSON) {
		return true
	}
	return strings.HasPrefix(model, "gpt-image") || strings.HasPrefix(model, "dall-e") || strings.HasPrefix(model, "dalle")
}

func hasImageGenerationIntent(rawJSON []byte) bool {
	root := gjson.ParseBytes(rawJSON)
	matched := false
	if tools := root.Get("tools"); tools.IsArray() {
		tools.ForEach(func(_, tool gjson.Result) bool {
			if imageGenerationChoice(tool) {
				matched = true
				return false
			}
			return true
		})
	}
	return matched || imageGenerationChoice(root.Get("tool_choice"))
}

func imageGenerationChoice(choice gjson.Result) bool {
	if !choice.Exists() {
		return false
	}
	if choice.Type == gjson.String {
		return strings.Contains(strings.ToLower(strings.TrimSpace(choice.String())), "image_generation")
	}
	for _, field := range []string{"type", "name", "function.name"} {
		if strings.EqualFold(strings.TrimSpace(choice.Get(field).String()), "image_generation") {
			return true
		}
	}
	return false
}

func sanitizeCodexCompatibilityRequest(rawJSON []byte) ([]byte, int) {
	model := normalizedPolicyModel(gjson.GetBytes(rawJSON, "model").String())
	if model == "" || !isCodexOpenAIModelName(model) || !json.Valid(rawJSON) {
		return rawJSON, 0
	}

	updated := rawJSON
	removed := 0
	if model == "gpt-5.3-codex-spark" && gjson.GetBytes(updated, "reasoning.summary").Exists() {
		if candidate, err := sjson.DeleteBytes(updated, "reasoning.summary"); err == nil {
			updated = candidate
			removed++
		}
	}
	if !bytes.Contains(updated, []byte("uniqueItems")) && !bytes.Contains(updated, []byte("uniqueitems")) {
		return updated, removed
	}

	var payload any
	decoder := json.NewDecoder(bytes.NewReader(updated))
	decoder.UseNumber()
	if err := decoder.Decode(&payload); err != nil {
		return updated, removed
	}
	removedSchema := stripUnsupportedCodexSchemaKeys(payload)
	if removedSchema == 0 {
		return updated, removed
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return updated, removed
	}
	return encoded, removed + removedSchema
}

func stripUnsupportedCodexSchemaKeys(node any) int {
	removed := 0
	switch value := node.(type) {
	case map[string]any:
		for key, child := range value {
			if strings.EqualFold(strings.TrimSpace(key), "uniqueItems") {
				delete(value, key)
				removed++
				continue
			}
			removed += stripUnsupportedCodexSchemaKeys(child)
		}
	case []any:
		for _, child := range value {
			removed += stripUnsupportedCodexSchemaKeys(child)
		}
	}
	return removed
}

func normalizedPolicyModel(model string) string {
	model = strings.ToLower(strings.TrimSpace(model))
	if slash := strings.LastIndexByte(model, '/'); slash >= 0 {
		model = model[slash+1:]
	}
	return model
}

func isCodexOpenAIModelName(model string) bool {
	model = normalizedPolicyModel(model)
	return strings.HasPrefix(model, "gpt-") || strings.HasPrefix(model, "codex") ||
		strings.HasPrefix(model, "chatgpt") || strings.HasPrefix(model, "dall-e") ||
		strings.HasPrefix(model, "dalle") ||
		(len(model) >= 2 && model[0] == 'o' && model[1] >= '1' && model[1] <= '9')
}

func providerScopedDefaultImageModel(cfg *internalconfig.SDKConfig) string {
	if cfg != nil && cfg.DisableImageGeneration == internalconfig.DisableImageGenerationCodex {
		return defaultXAIImagesModel
	}
	return defaultImagesToolModel
}
