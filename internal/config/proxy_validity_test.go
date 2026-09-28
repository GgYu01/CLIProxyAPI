package config

import (
	"context"
	"errors"
	"testing"
)

func TestInspectConfigPayloadRejectsEmptyAndPartial(t *testing.T) {
	t.Parallel()

	if err := InspectConfigPayload(nil); err == nil {
		t.Fatal("expected empty payload error")
	}
	if err := InspectConfigPayload([]byte("   \n\t")); err == nil {
		t.Fatal("expected whitespace-only payload error")
	}
	if err := InspectConfigPayload([]byte("proxy-url: \"http://proxy.local")); err == nil {
		t.Fatal("expected truncated payload error")
	}
	if err := InspectConfigPayload([]byte("port: 8080\n")); err != nil {
		t.Fatalf("complete payload rejected: %v", err)
	}
}

func TestValidateProxyURLsRejectsMalformed(t *testing.T) {
	t.Parallel()

	valid := &Config{}
	valid.ProxyURL = "socks5://egress:1080"
	valid.CodexKey = []CodexKey{{APIKey: "k", ProxyURL: "http://proxy.local:8080"}}
	if err := valid.ValidateProxyURLs(); err != nil {
		t.Fatalf("valid proxy URLs rejected: %v", err)
	}

	invalidGlobal := &Config{}
	invalidGlobal.ProxyURL = "not-a-url"
	if err := invalidGlobal.ValidateProxyURLs(); err == nil {
		t.Fatal("expected invalid global proxy-url error")
	}

	invalidKey := &Config{GeminiKey: []GeminiKey{{APIKey: "g", ProxyURL: "ftp://proxy.local"}}}
	if err := invalidKey.ValidateProxyURLs(); err == nil {
		t.Fatal("expected invalid gemini proxy-url error")
	}

	direct := &Config{}
	direct.ProxyURL = "direct"
	if err := direct.ValidateProxyURLs(); err != nil {
		t.Fatalf("direct proxy-url rejected: %v", err)
	}
}

func TestCheckMonotonicRevisionRejectsOlder(t *testing.T) {
	t.Parallel()

	current := &Config{Revision: 4}
	if err := CheckMonotonicRevision(current, &Config{Revision: 3}); err == nil {
		t.Fatal("expected stale revision error")
	}
	if err := CheckMonotonicRevision(current, &Config{Revision: 4}); err != nil {
		t.Fatalf("equal revision rejected: %v", err)
	}
	if err := CheckMonotonicRevision(current, &Config{Revision: 5}); err != nil {
		t.Fatalf("newer revision rejected: %v", err)
	}
	if err := CheckMonotonicRevision(current, &Config{Revision: 0}); err != nil {
		t.Fatalf("legacy zero revision rejected: %v", err)
	}
	if err := CheckMonotonicRevision(&Config{Revision: 0}, &Config{Revision: 1}); err != nil {
		t.Fatalf("first revision rejected: %v", err)
	}
}

func TestValidateConfigCommitAndProbeMatrix(t *testing.T) {
	current := &Config{Revision: 4}
	current.ProxyURL = "socks5://egress:1080"

	if err := ValidateConfigCommit(current, &Config{Revision: 3, SDKConfig: SDKConfig{ProxyURL: "socks5://egress:1080"}}); err == nil {
		t.Fatal("expected stale revision reject")
	}
	if err := ValidateConfigCommit(current, &Config{Revision: 5, SDKConfig: SDKConfig{ProxyURL: "not-a-url"}}); err == nil {
		t.Fatal("expected invalid proxy-url reject")
	}
	next := &Config{Revision: 5}
	next.ProxyURL = "socks5://egress:1080"
	if err := ValidateConfigCommit(current, next); err != nil {
		t.Fatalf("valid commit rejected: %v", err)
	}
	if err := ProbeConfigProxies(context.Background(), next, NopProxyProber{}); err != nil {
		t.Fatalf("nop probe failed: %v", err)
	}
	errProbe := ProbeConfigProxies(context.Background(), next, FailProxyProber{Cause: ProbeCauseFailed})
	if errProbe == nil {
		t.Fatal("expected probe failure")
	}
	var typed *ProbeError
	if !errors.As(errProbe, &typed) || typed.Cause != ProbeCauseFailed {
		t.Fatalf("probe error = %v, want typed cause %s", errProbe, ProbeCauseFailed)
	}
}

func TestParseConfigBytesRejectsEmptyPartialAndInvalidProxy(t *testing.T) {
	if _, err := ParseConfigBytes(nil); err == nil {
		t.Fatal("expected empty payload error")
	}
	if _, err := ParseConfigBytes([]byte("proxy-url: \"http://proxy")); err == nil {
		t.Fatal("expected partial payload error")
	}
	if _, err := ParseConfigBytes([]byte("proxy-url: not-a-url\n")); err == nil {
		t.Fatal("expected invalid proxy-url schema error")
	}
	cfg, err := ParseConfigBytes([]byte("revision: 9\nproxy-url: socks5://egress:1080\n"))
	if err != nil {
		t.Fatalf("valid payload rejected: %v", err)
	}
	if cfg.Revision != 9 {
		t.Fatalf("revision = %d, want 9", cfg.Revision)
	}
	if cfg.ProxyURL != "socks5://egress:1080" {
		t.Fatalf("proxy-url = %q", cfg.ProxyURL)
	}
}
