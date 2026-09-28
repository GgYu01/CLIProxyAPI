package config

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/proxyutil"
	"gopkg.in/yaml.v3"
)

// InspectConfigPayload rejects empty or truncated config bytes before they are
// unmarshaled into Config. Complete YAML that fails schema validation is left
// to ParseConfigBytes / LoadConfig.
func InspectConfigPayload(data []byte) error {
	if len(bytes.TrimSpace(data)) == 0 {
		return fmt.Errorf("config payload is empty")
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(false)
	var doc yaml.Node
	if errDecode := dec.Decode(&doc); errDecode != nil {
		return fmt.Errorf("partial or invalid config payload: %w", errDecode)
	}
	if doc.Kind == 0 && len(doc.Content) == 0 {
		return fmt.Errorf("config payload is empty")
	}
	return nil
}

// CheckMonotonicRevision reports whether next would overwrite a newer committed
// generation. Zero revisions are treated as "not participating" so legacy files
// without a revision field still load.
func CheckMonotonicRevision(current, next *Config) error {
	if next == nil {
		return nil
	}
	var currentRevision int64
	if current != nil {
		currentRevision = current.Revision
	}
	if currentRevision > 0 && next.Revision > 0 && next.Revision < currentRevision {
		return fmt.Errorf("stale config revision %d (current %d)", next.Revision, currentRevision)
	}
	return nil
}

// ValidateProxyURLs rejects malformed proxy-url values on the global setting
// and every per-credential override. Empty and "direct"/"none" are valid.
func (cfg *Config) ValidateProxyURLs() error {
	if cfg == nil {
		return nil
	}
	return cfg.eachProxyURL(func(path, raw string) error {
		if _, errParse := proxyutil.Parse(raw); errParse != nil {
			return fmt.Errorf("%s: %w", path, errParse)
		}
		return nil
	})
}

// ProxyURLs returns the unique, trimmed proxy URLs from the global setting and
// every per-credential override, preserving first-seen order.
func (cfg *Config) ProxyURLs() []string {
	if cfg == nil {
		return nil
	}
	out := make([]string, 0, 4)
	seen := make(map[string]struct{}, 4)
	_ = cfg.eachProxyURL(func(_, raw string) error {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" {
			return nil
		}
		if _, exists := seen[trimmed]; exists {
			return nil
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
		return nil
	})
	return out
}

func (cfg *Config) eachProxyURL(fn func(path, raw string) error) error {
	if cfg == nil || fn == nil {
		return nil
	}
	if err := fn("proxy-url", cfg.ProxyURL); err != nil {
		return err
	}
	for i := range cfg.GeminiKey {
		if err := fn(fmt.Sprintf("gemini-api-key[%d].proxy-url", i), cfg.GeminiKey[i].ProxyURL); err != nil {
			return err
		}
	}
	for i := range cfg.InteractionsKey {
		if err := fn(fmt.Sprintf("interactions-api-key[%d].proxy-url", i), cfg.InteractionsKey[i].ProxyURL); err != nil {
			return err
		}
	}
	for i := range cfg.ClaudeKey {
		if err := fn(fmt.Sprintf("claude-api-key[%d].proxy-url", i), cfg.ClaudeKey[i].ProxyURL); err != nil {
			return err
		}
	}
	for i := range cfg.CodexKey {
		if err := fn(fmt.Sprintf("codex-api-key[%d].proxy-url", i), cfg.CodexKey[i].ProxyURL); err != nil {
			return err
		}
	}
	for i := range cfg.XAIKey {
		if err := fn(fmt.Sprintf("xai-api-key[%d].proxy-url", i), cfg.XAIKey[i].ProxyURL); err != nil {
			return err
		}
	}
	for i := range cfg.VertexCompatAPIKey {
		if err := fn(fmt.Sprintf("vertex-api-key[%d].proxy-url", i), cfg.VertexCompatAPIKey[i].ProxyURL); err != nil {
			return err
		}
	}
	for providerIndex := range cfg.OpenAICompatibility {
		for keyIndex := range cfg.OpenAICompatibility[providerIndex].APIKeyEntries {
			path := fmt.Sprintf("openai-compatibility[%d].api-key-entries[%d].proxy-url", providerIndex, keyIndex)
			if err := fn(path, cfg.OpenAICompatibility[providerIndex].APIKeyEntries[keyIndex].ProxyURL); err != nil {
				return err
			}
		}
	}
	return nil
}
