package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

type testLegacyPinBinding struct {
	Prefix   string  `json:"prefix"`
	TS       float64 `json:"ts"`
	ProxyURL *string `json:"proxy_url,omitempty"`
}

type testLegacyPinFile struct {
	Version  int                             `json:"v"`
	Bindings map[string]testLegacyPinBinding `json:"bindings"`
}

func TestStrictSessionAffinityKeepsUnavailableLegacyBinding(t *testing.T) {
	pinPath := filepath.Join(t.TempDir(), "session-pin.json")
	sessionID := "strict-session-unavailable"
	writeTestLegacyPinFile(t, pinPath, map[string]testLegacyPinBinding{
		testLegacyFingerprint(sessionID): {Prefix: "s-auth-a", TS: float64(time.Now().Unix())},
	})
	selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
		Fallback:  &RoundRobinSelector{},
		TTL:       time.Hour,
		Strict:    true,
		StorePath: pinPath,
	})
	defer selector.Stop()

	nextRetry := time.Now().Add(45 * time.Second)
	authA := &Auth{ID: "auth-a", Prefix: "s-auth-a", Status: StatusActive, Unavailable: true, NextRetryAfter: nextRetry}
	authB := &Auth{ID: "auth-b", Prefix: "s-auth-b", Status: StatusActive}
	_, err := selector.Pick(context.Background(), "codex", "gpt-5.6-sol", strictAffinityOptions(sessionID), []*Auth{authA, authB})
	if err == nil {
		t.Fatal("strict affinity selected a different credential while the binding was unavailable")
	}
	var strictErr *StrictAffinityUnavailableError
	if !errors.As(err, &strictErr) {
		t.Fatalf("error = %T %v, want *StrictAffinityUnavailableError", err, err)
	}
	if got := strictErr.StatusCode(); got != http.StatusServiceUnavailable {
		t.Fatalf("StatusCode() = %d, want %d", got, http.StatusServiceUnavailable)
	}
	retryAfter := SafeResponseHeaders(err).Get("Retry-After")
	retrySeconds, parseErr := strconv.Atoi(retryAfter)
	if parseErr != nil || retrySeconds < 1 || retrySeconds > 45 {
		t.Fatalf("Retry-After = %q, want 1..45 seconds", retryAfter)
	}
	assertTestLegacyPinPrefix(t, pinPath, sessionID, "s-auth-a")

	selector.InvalidateAuth(authA.ID)
	authA.Unavailable = false
	authA.NextRetryAfter = time.Time{}
	selected, err := selector.Pick(context.Background(), "codex", "gpt-5.6-sol", strictAffinityOptions(sessionID), []*Auth{authA, authB})
	if err != nil {
		t.Fatalf("Pick() after recovery error = %v", err)
	}
	if selected.ID != authA.ID {
		t.Fatalf("Pick() after recovery = %q, want original %q", selected.ID, authA.ID)
	}
}

func TestStrictSessionAffinityMissingOrDisabledBindingDoesNotRebind(t *testing.T) {
	for _, testCase := range []struct {
		name  string
		auths []*Auth
	}{
		{name: "missing", auths: []*Auth{{ID: "auth-b", Prefix: "s-auth-b", Status: StatusActive}}},
		{name: "disabled", auths: []*Auth{{ID: "auth-a", Prefix: "s-auth-a", Status: StatusDisabled, Disabled: true}, {ID: "auth-b", Prefix: "s-auth-b", Status: StatusActive}}},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			pinPath := filepath.Join(t.TempDir(), "session-pin.json")
			sessionID := "strict-session-" + testCase.name
			writeTestLegacyPinFile(t, pinPath, map[string]testLegacyPinBinding{
				testLegacyFingerprint(sessionID): {Prefix: "s-auth-a", TS: float64(time.Now().Unix())},
			})
			selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
				Fallback:  &RoundRobinSelector{},
				TTL:       time.Hour,
				Strict:    true,
				StorePath: pinPath,
			})
			defer selector.Stop()

			selected, err := selector.Pick(context.Background(), "codex", "gpt-5.6-sol", strictAffinityOptions(sessionID), testCase.auths)
			if selected != nil || err == nil {
				t.Fatalf("Pick() = %#v, %v; want nil strict-unavailable error", selected, err)
			}
			var strictErr *StrictAffinityUnavailableError
			if !errors.As(err, &strictErr) {
				t.Fatalf("error = %T %v, want *StrictAffinityUnavailableError", err, err)
			}
			assertTestLegacyPinPrefix(t, pinPath, sessionID, "s-auth-a")
		})
	}
}

func TestStrictSessionAffinityWritesRollbackCompatibleRoundRobinBindings(t *testing.T) {
	pinPath := filepath.Join(t.TempDir(), "session-pin.json")
	selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
		Fallback:  &RoundRobinSelector{},
		TTL:       time.Hour,
		Strict:    true,
		StorePath: pinPath,
	})
	defer selector.Stop()
	auths := []*Auth{
		{ID: "auth-a", Prefix: "s-auth-a", Status: StatusActive},
		{ID: "auth-b", Prefix: "s-auth-b", Status: StatusActive},
	}

	for index, sessionID := range []string{"new-session-a", "new-session-b", "new-session-c", "new-session-d"} {
		selected, err := selector.Pick(context.Background(), "codex", "gpt-5.6-sol", strictAffinityOptions(sessionID), auths)
		if err != nil {
			t.Fatalf("Pick(%q) error = %v", sessionID, err)
		}
		want := auths[index%len(auths)]
		if selected.ID != want.ID {
			t.Fatalf("Pick(%q) = %q, want round-robin %q", sessionID, selected.ID, want.ID)
		}
		assertTestLegacyPinPrefix(t, pinPath, sessionID, want.Prefix)
	}

	selected, err := selector.Pick(context.Background(), "codex", "gpt-5.6-luna", strictAffinityOptions("new-session-a"), auths)
	if err != nil {
		t.Fatalf("same-session second-model Pick() error = %v", err)
	}
	if selected.ID != auths[0].ID {
		t.Fatalf("same-session second-model Pick() = %q, want %q", selected.ID, auths[0].ID)
	}

	raw, err := os.ReadFile(pinPath)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", pinPath, err)
	}
	var stored testLegacyPinFile
	if err := json.Unmarshal(raw, &stored); err != nil {
		t.Fatalf("Unmarshal pin store error = %v", err)
	}
	if stored.Version != 1 || len(stored.Bindings) != 4 {
		t.Fatalf("pin store = %#v, want version 1 with four legacy bindings", stored)
	}
}

func TestStrictSessionAffinityPinsAndValidatesEffectiveProxy(t *testing.T) {
	pinPath := filepath.Join(t.TempDir(), "session-pin.json")
	selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
		Fallback:        &RoundRobinSelector{},
		TTL:             time.Hour,
		Strict:          true,
		StorePath:       pinPath,
		DefaultProxyURL: "socks5://global-egress:1080",
	})
	defer selector.Stop()
	authA := &Auth{ID: "auth-a", Prefix: "s-auth-a", Status: StatusActive, ProxyURL: "socks5://fixed-egress-a:1080"}
	authB := &Auth{ID: "auth-b", Prefix: "s-auth-b", Status: StatusActive, ProxyURL: "socks5://fixed-egress-b:1080"}
	options := strictAffinityOptions("fixed-egress-session")

	selected, err := selector.Pick(context.Background(), "codex", "gpt-5.6-sol", options, []*Auth{authA, authB})
	if err != nil || selected == nil || selected.ID != authA.ID {
		t.Fatalf("initial Pick() = %#v, %v; want auth-a", selected, err)
	}
	assertTestLegacyPinProxy(t, pinPath, "fixed-egress-session", "socks5://fixed-egress-a:1080")

	authA.ProxyURL = "socks5://changed-egress:1080"
	selected, err = selector.Pick(context.Background(), "codex", "gpt-5.6-sol", options, []*Auth{authA, authB})
	if selected != nil || err == nil {
		t.Fatalf("Pick() after proxy change = %#v, %v; want strict unavailable without account migration", selected, err)
	}
	var strictErr *StrictAffinityUnavailableError
	if !errors.As(err, &strictErr) {
		t.Fatalf("error after proxy change = %T %v, want *StrictAffinityUnavailableError", err, err)
	}
	assertTestLegacyPinPrefix(t, pinPath, "fixed-egress-session", authA.Prefix)
	assertTestLegacyPinProxy(t, pinPath, "fixed-egress-session", "socks5://fixed-egress-a:1080")
}

func TestStrictSessionAffinityPinsDefaultProxyAndUpgradesLegacyBinding(t *testing.T) {
	pinPath := filepath.Join(t.TempDir(), "session-pin.json")
	sessionID := "legacy-default-egress-session"
	writeTestLegacyPinFile(t, pinPath, map[string]testLegacyPinBinding{
		testLegacyFingerprint(sessionID): {Prefix: "s-auth-a", TS: float64(time.Now().Unix())},
	})
	selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
		Fallback:        &RoundRobinSelector{},
		TTL:             time.Hour,
		Strict:          true,
		StorePath:       pinPath,
		DefaultProxyURL: "http://default-egress:8080",
	})
	defer selector.Stop()
	authA := &Auth{ID: "auth-a", Prefix: "s-auth-a", Status: StatusActive}
	authB := &Auth{ID: "auth-b", Prefix: "s-auth-b", Status: StatusActive}
	options := strictAffinityOptions(sessionID)

	selected, err := selector.Pick(context.Background(), "codex", "gpt-5.6-sol", options, []*Auth{authA, authB})
	if err != nil || selected == nil || selected.ID != authA.ID {
		t.Fatalf("legacy Pick() = %#v, %v; want auth-a", selected, err)
	}
	assertTestLegacyPinProxy(t, pinPath, sessionID, "http://default-egress:8080")

	selector.strictDefaultProxyURL = "http://changed-default-egress:8080"
	selected, err = selector.Pick(context.Background(), "codex", "gpt-5.6-sol", options, []*Auth{authA, authB})
	if selected != nil || err == nil {
		t.Fatalf("Pick() after default proxy change = %#v, %v; want strict unavailable", selected, err)
	}
	assertTestLegacyPinProxy(t, pinPath, sessionID, "http://default-egress:8080")
}

func TestStrictSessionAffinityExcludesConfiguredPrefixFromNewBinding(t *testing.T) {
	directory := t.TempDir()
	pinPath := filepath.Join(directory, "session-pin.json")
	excludePath := filepath.Join(directory, "pin-exclude.json")
	sessionID := "excluded-session"
	writeTestLegacyPinFile(t, pinPath, map[string]testLegacyPinBinding{
		testLegacyFingerprint(sessionID): {Prefix: "s-auth-a", TS: float64(time.Now().Unix())},
	})
	if err := os.WriteFile(excludePath, []byte(`["s-auth-a"]`), 0o600); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", excludePath, err)
	}
	selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
		Fallback:    &RoundRobinSelector{},
		TTL:         time.Hour,
		Strict:      true,
		StorePath:   pinPath,
		ExcludePath: excludePath,
	})
	defer selector.Stop()
	authA := &Auth{ID: "auth-a", Prefix: "s-auth-a", Status: StatusActive}
	authB := &Auth{ID: "auth-b", Prefix: "s-auth-b", Status: StatusActive}

	selected, err := selector.Pick(context.Background(), "codex", "gpt-5.6-sol", strictAffinityOptions(sessionID), []*Auth{authA, authB})
	if selected != nil || err == nil {
		t.Fatalf("Pick() = %#v, %v; want strict unavailable without account migration", selected, err)
	}
	var strictErr *StrictAffinityUnavailableError
	if !errors.As(err, &strictErr) {
		t.Fatalf("error = %T %v, want *StrictAffinityUnavailableError", err, err)
	}
	assertTestLegacyPinPrefix(t, pinPath, sessionID, authA.Prefix)
}

func TestStrictSessionAffinityConcurrentFirstRequestsCreateOneBinding(t *testing.T) {
	pinPath := filepath.Join(t.TempDir(), "session-pin.json")
	selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
		Fallback:  &RoundRobinSelector{},
		TTL:       time.Hour,
		Strict:    true,
		StorePath: pinPath,
	})
	defer selector.Stop()
	auths := []*Auth{
		{ID: "auth-a", Prefix: "s-auth-a", Status: StatusActive},
		{ID: "auth-b", Prefix: "s-auth-b", Status: StatusActive},
	}

	const workers = 32
	start := make(chan struct{})
	results := make(chan string, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			selected, err := selector.Pick(context.Background(), "codex", "gpt-5.6-sol", strictAffinityOptions("one-concurrent-session"), auths)
			if err != nil {
				errs <- err
				return
			}
			results <- selected.ID
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent Pick() error = %v", err)
	}
	first := ""
	for selectedID := range results {
		if first == "" {
			first = selectedID
		}
		if selectedID != first {
			t.Fatalf("one new session selected both %q and %q", first, selectedID)
		}
	}
	if first == "" {
		t.Fatal("concurrent Pick() returned no credential")
	}
	wantPrefix := map[string]string{"auth-a": "s-auth-a", "auth-b": "s-auth-b"}[first]
	assertTestLegacyPinPrefix(t, pinPath, "one-concurrent-session", wantPrefix)
}

func TestStrictSessionAffinityDoesNotPinExplicitNonCodexModel(t *testing.T) {
	pinPath := filepath.Join(t.TempDir(), "session-pin.json")
	selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
		Fallback:  &RoundRobinSelector{},
		TTL:       time.Hour,
		Strict:    true,
		StorePath: pinPath,
	})
	defer selector.Stop()
	auths := []*Auth{
		{ID: "auth-a", Prefix: "s-auth-a", Status: StatusActive},
		{ID: "auth-b", Prefix: "s-auth-b", Status: StatusActive},
	}

	selected, err := selector.Pick(context.Background(), "xai", "grok-4.3", strictAffinityOptions("grok-session"), auths)
	if err != nil {
		t.Fatalf("Pick() error = %v", err)
	}
	if selected == nil {
		t.Fatal("Pick() returned nil auth")
	}
	if _, err := os.Stat(pinPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("non-Codex request unexpectedly created strict pin store: %v", err)
	}
}

func TestNonStrictSessionAffinityPreservesAutomaticFailover(t *testing.T) {
	selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
		Fallback: &RoundRobinSelector{},
		TTL:      time.Hour,
	})
	defer selector.Stop()
	authA := &Auth{ID: "auth-a", Prefix: "s-auth-a", Status: StatusActive}
	authB := &Auth{ID: "auth-b", Prefix: "s-auth-b", Status: StatusActive}
	opts := strictAffinityOptions("normal-affinity-session")

	selected, err := selector.Pick(context.Background(), "codex", "gpt-5.6-sol", opts, []*Auth{authA, authB})
	if err != nil || selected.ID != authA.ID {
		t.Fatalf("initial Pick() = %#v, %v; want auth-a", selected, err)
	}
	authA.Unavailable = true
	authA.NextRetryAfter = time.Now().Add(time.Minute)
	selected, err = selector.Pick(context.Background(), "codex", "gpt-5.6-sol", opts, []*Auth{authA, authB})
	if err != nil || selected.ID != authB.ID {
		t.Fatalf("failover Pick() = %#v, %v; want auth-b", selected, err)
	}
}

func strictAffinityOptions(sessionID string) cliproxyexecutor.Options {
	return cliproxyexecutor.Options{
		Headers:         http.Header{"X-Session-Id": []string{sessionID}},
		OriginalRequest: []byte(`{"model":"gpt-5.6-sol","input":"hi"}`),
	}
}

func testLegacyFingerprint(sessionID string) string {
	digest := sha256.Sum256([]byte(sessionID))
	return hex.EncodeToString(digest[:])[:10]
}

func writeTestLegacyPinFile(t *testing.T, path string, bindings map[string]testLegacyPinBinding) {
	t.Helper()
	raw, err := json.Marshal(testLegacyPinFile{Version: 1, Bindings: bindings})
	if err != nil {
		t.Fatalf("Marshal pin store error = %v", err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", path, err)
	}
}

func assertTestLegacyPinPrefix(t *testing.T, path, sessionID, wantPrefix string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	var stored testLegacyPinFile
	if err := json.Unmarshal(raw, &stored); err != nil {
		t.Fatalf("Unmarshal pin store error = %v", err)
	}
	row, ok := stored.Bindings[testLegacyFingerprint(sessionID)]
	if !ok || row.Prefix != wantPrefix || row.TS <= 0 {
		t.Fatalf("binding for %q = %#v, %v; want prefix %q with timestamp", sessionID, row, ok, wantPrefix)
	}
}

func assertTestLegacyPinProxy(t *testing.T, path, sessionID, wantProxyURL string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%q) error = %v", path, err)
	}
	var stored testLegacyPinFile
	if err := json.Unmarshal(raw, &stored); err != nil {
		t.Fatalf("Unmarshal pin store error = %v", err)
	}
	row, ok := stored.Bindings[testLegacyFingerprint(sessionID)]
	if !ok || row.ProxyURL == nil || *row.ProxyURL != wantProxyURL {
		t.Fatalf("binding proxy for %q = %#v, %v; want %q", sessionID, row.ProxyURL, ok, wantProxyURL)
	}
}

func TestStrictSessionAffinityAcceptsFloatTimestamps(t *testing.T) {
	pinPath := filepath.Join(t.TempDir(), "session-pin.json")
	sessionID := "test-float-ts-session"
	fingerprint := testLegacyFingerprint(sessionID)
	// Write raw JSON with float timestamp like Python time.time()
	rawJSON := fmt.Sprintf(`{"v":1,"bindings":{"%s":{"prefix":"s-auth-float","ts":1787305655.4010465}}}`, fingerprint)
	if err := os.WriteFile(pinPath, []byte(rawJSON), 0o600); err != nil {
		t.Fatalf("WriteFile error = %v", err)
	}

	selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
		Fallback:  &RoundRobinSelector{},
		TTL:       100 * 365 * 24 * time.Hour, // large TTL so 1787305655 is valid
		Strict:    true,
		StorePath: pinPath,
	})
	defer selector.Stop()

	authA := &Auth{ID: "auth-float", Prefix: "s-auth-float", Status: StatusActive}
	authB := &Auth{ID: "auth-other", Prefix: "s-auth-other", Status: StatusActive}

	selected, err := selector.Pick(context.Background(), "codex", "gpt-5.6-sol", strictAffinityOptions(sessionID), []*Auth{authA, authB})
	if err != nil {
		t.Fatalf("Pick() with float timestamp failed: %v", err)
	}
	if selected == nil || selected.ID != authA.ID {
		t.Fatalf("Pick() = %#v, want %q", selected, authA.ID)
	}
}

