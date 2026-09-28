package auth

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/tidwall/gjson"
)

const (
	legacyPinVersion        = 1
	legacyPinRefreshEvery   = time.Second
	strictAffinityRetrySoon = 5 * time.Second
)

type legacyPinBinding struct {
	Prefix   string  `json:"prefix"`
	TS       float64 `json:"ts"`
	ProxyURL *string `json:"proxy_url,omitempty"`
}

type legacyPinFile struct {
	Version  int                         `json:"v"`
	Bindings map[string]legacyPinBinding `json:"bindings"`
}

type legacyPinFileStamp struct {
	exists  bool
	size    int64
	modTime int64
}

// legacyPinStore reads and writes the rollback-compatible Python queue format.
// The complete file is small operational state; request bodies and credentials
// are never stored here.
type legacyPinStore struct {
	mu           sync.Mutex
	path         string
	excludePath  string
	ttl          time.Duration
	bindings     map[string]legacyPinBinding
	excluded     map[string]struct{}
	pinStamp     legacyPinFileStamp
	excludeStamp legacyPinFileStamp
	nextRefresh  time.Time
}

func newLegacyPinStore(path, excludePath string, ttl time.Duration) *legacyPinStore {
	return &legacyPinStore{
		path:        strings.TrimSpace(path),
		excludePath: strings.TrimSpace(excludePath),
		ttl:         ttl,
		bindings:    make(map[string]legacyPinBinding),
		excluded:    make(map[string]struct{}),
	}
}

func (s *legacyPinStore) lookup(sessionID string) (legacyPinBinding, bool, error) {
	if s == nil || sessionID == "" {
		return legacyPinBinding{}, false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.refreshLocked(false); err != nil {
		return legacyPinBinding{}, false, err
	}
	binding, ok := s.bindings[legacySessionFingerprint(sessionID)]
	if !ok || strings.TrimSpace(binding.Prefix) == "" || s.expired(binding, time.Now()) {
		return legacyPinBinding{}, false, nil
	}
	return binding, true, nil
}

func (s *legacyPinStore) isExcluded(prefix string) (bool, error) {
	if s == nil || strings.TrimSpace(prefix) == "" {
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.refreshLocked(false); err != nil {
		return false, err
	}
	_, excluded := s.excluded[strings.TrimSpace(prefix)]
	return excluded, nil
}

func (s *legacyPinStore) eligible(auths []*Auth) ([]*Auth, error) {
	if s == nil {
		return auths, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.refreshLocked(false); err != nil {
		return nil, err
	}
	eligible := make([]*Auth, 0, len(auths))
	for _, auth := range auths {
		if auth == nil || strings.TrimSpace(auth.Prefix) == "" {
			continue
		}
		if _, excluded := s.excluded[auth.Prefix]; excluded {
			continue
		}
		eligible = append(eligible, auth)
	}
	return eligible, nil
}

func (s *legacyPinStore) set(sessionID, prefix, proxyURL string) error {
	if s == nil || sessionID == "" {
		return errors.New("strict affinity pin store is unavailable")
	}
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		return errors.New("selected credential has no stable routing prefix")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.refreshLocked(true); err != nil {
		return err
	}
	if _, excluded := s.excluded[prefix]; excluded {
		return fmt.Errorf("selected credential prefix %q is excluded from strict affinity", prefix)
	}
	now := time.Now()
	s.pruneExpiredLocked(now)
	proxyURL = strings.TrimSpace(proxyURL)
	s.bindings[legacySessionFingerprint(sessionID)] = legacyPinBinding{Prefix: prefix, TS: float64(now.UnixNano()) / 1e9, ProxyURL: &proxyURL}
	if s.path == "" {
		return nil
	}
	return s.persistLocked(now)
}

func (s *legacyPinStore) refreshLocked(force bool) error {
	now := time.Now()
	if !force && now.Before(s.nextRefresh) {
		return nil
	}
	if s.path != "" {
		stamp, err := statLegacyPinFile(s.path)
		if err != nil {
			return fmt.Errorf("stat strict affinity pin store: %w", err)
		}
		if force || stamp != s.pinStamp {
			bindings, errRead := readLegacyPinBindings(s.path)
			if errRead != nil {
				return fmt.Errorf("read strict affinity pin store: %w", errRead)
			}
			s.bindings = bindings
			s.pinStamp = stamp
		}
	}
	if s.excludePath != "" {
		stamp, err := statLegacyPinFile(s.excludePath)
		if err != nil {
			return fmt.Errorf("stat strict affinity exclude file: %w", err)
		}
		if force || stamp != s.excludeStamp {
			excluded, errRead := readLegacyPinExcludes(s.excludePath)
			if errRead != nil {
				return fmt.Errorf("read strict affinity exclude file: %w", errRead)
			}
			s.excluded = excluded
			s.excludeStamp = stamp
		}
	}
	s.nextRefresh = now.Add(legacyPinRefreshEvery)
	return nil
}

func statLegacyPinFile(path string) (legacyPinFileStamp, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return legacyPinFileStamp{}, nil
	}
	if err != nil {
		return legacyPinFileStamp{}, err
	}
	return legacyPinFileStamp{exists: true, size: info.Size(), modTime: info.ModTime().UnixNano()}, nil
}

func readLegacyPinBindings(path string) (map[string]legacyPinBinding, error) {
	bindings := make(map[string]legacyPinBinding)
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return bindings, nil
	}
	if err != nil {
		return nil, err
	}
	var file legacyPinFile
	if err := json.Unmarshal(raw, &file); err != nil {
		return nil, err
	}
	if file.Version != 0 && file.Version != legacyPinVersion {
		return nil, fmt.Errorf("unsupported pin store version %d", file.Version)
	}
	for fingerprint, binding := range file.Bindings {
		fingerprint = strings.TrimSpace(fingerprint)
		binding.Prefix = strings.TrimSpace(binding.Prefix)
		if fingerprint == "" || binding.Prefix == "" {
			continue
		}
		bindings[fingerprint] = binding
	}
	return bindings, nil
}

func readLegacyPinExcludes(path string) (map[string]struct{}, error) {
	excluded := make(map[string]struct{})
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return excluded, nil
	}
	if err != nil {
		return nil, err
	}
	var prefixes []string
	if err := json.Unmarshal(raw, &prefixes); err != nil {
		return nil, err
	}
	for _, prefix := range prefixes {
		if prefix = strings.TrimSpace(prefix); prefix != "" {
			excluded[prefix] = struct{}{}
		}
	}
	return excluded, nil
}

func (s *legacyPinStore) expired(binding legacyPinBinding, now time.Time) bool {
	if s.ttl <= 0 || binding.TS <= 0 {
		return false
	}
	sec := int64(binding.TS)
	nsec := int64((binding.TS - float64(sec)) * 1e9)
	return now.Sub(time.Unix(sec, nsec)) > s.ttl
}

func (s *legacyPinStore) pruneExpiredLocked(now time.Time) {
	for fingerprint, binding := range s.bindings {
		if s.expired(binding, now) {
			delete(s.bindings, fingerprint)
		}
	}
}

func (s *legacyPinStore) persistLocked(now time.Time) error {
	directory := filepath.Dir(s.path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create strict affinity store directory: %w", err)
	}
	raw, err := json.Marshal(legacyPinFile{Version: legacyPinVersion, Bindings: s.bindings})
	if err != nil {
		return fmt.Errorf("encode strict affinity pin store: %w", err)
	}
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(s.path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("create strict affinity temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryPath)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("set strict affinity temporary file mode: %w", err)
	}
	if _, err := temporary.Write(raw); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write strict affinity temporary file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync strict affinity temporary file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close strict affinity temporary file: %w", err)
	}
	if err := os.Rename(temporaryPath, s.path); err != nil {
		return fmt.Errorf("replace strict affinity pin store: %w", err)
	}
	removeTemporary = false
	if directoryHandle, errOpen := os.Open(directory); errOpen == nil {
		_ = directoryHandle.Sync()
		_ = directoryHandle.Close()
	}
	stamp, err := statLegacyPinFile(s.path)
	if err != nil {
		return fmt.Errorf("stat persisted strict affinity pin store: %w", err)
	}
	s.pinStamp = stamp
	s.nextRefresh = now.Add(legacyPinRefreshEvery)
	return nil
}

func legacySessionFingerprint(sessionID string) string {
	digest := sha256.Sum256([]byte(sessionID))
	return hex.EncodeToString(digest[:])[:10]
}

func legacyStrictSessionID(headers http.Header, body []byte) string {
	for _, name := range []string{"X-Session-ID", "Session-Id", "Session_id", "X-Session-Affinity"} {
		if value := validLegacySessionID(headers.Get(name)); value != "" {
			return value
		}
	}
	if len(body) == 0 || !gjson.ValidBytes(body) {
		return ""
	}
	for _, path := range []string{"previous_response_id", "session_id", "sessionId", "conversation_id", "prompt_cache_key"} {
		if value := validLegacySessionID(gjson.GetBytes(body, path).String()); value != "" {
			return value
		}
	}
	conversation := gjson.GetBytes(body, "conversation")
	if conversation.Type == gjson.String {
		return validLegacySessionID(conversation.String())
	}
	return validLegacySessionID(conversation.Get("id").String())
}

func validLegacySessionID(value string) string {
	value = strings.TrimSpace(value)
	if len(value) < 4 || len(value) > 200 || strings.ContainsAny(value, "\r\n") {
		return ""
	}
	return value
}

func isStrictCodexTextModel(model string) bool {
	model = strings.ToLower(strings.TrimSpace(model))
	if slash := strings.LastIndexByte(model, '/'); slash >= 0 {
		model = model[slash+1:]
	}
	return strings.HasPrefix(model, "gpt-") || strings.HasPrefix(model, "codex") || strings.HasPrefix(model, "chatgpt") ||
		(len(model) >= 2 && model[0] == 'o' && model[1] >= '1' && model[1] <= '9')
}

func strictAffinityEffectiveProxyURL(auth *Auth, defaultProxyURL string) string {
	if auth != nil {
		if proxyURL := strings.TrimSpace(auth.ProxyURL); proxyURL != "" {
			return proxyURL
		}
	}
	return strings.TrimSpace(defaultProxyURL)
}

// StrictAffinityUnavailableError prevents a pinned session from silently
// moving to another credential and carries a retry hint to downstream clients.
type StrictAffinityUnavailableError struct {
	cause      *Error
	retryAfter time.Duration
}

func newStrictAffinityUnavailableError(prefix, reason string, retryAfter time.Duration) *StrictAffinityUnavailableError {
	if retryAfter <= 0 {
		retryAfter = strictAffinityRetrySoon
	}
	prefix = strings.TrimSpace(prefix)
	if prefix == "" {
		prefix = "unbound"
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "credential is not currently available"
	}
	return &StrictAffinityUnavailableError{
		cause: &Error{
			Code:       "strict_affinity_unavailable",
			Message:    fmt.Sprintf("strict session affinity credential %s is unavailable: %s", prefix, reason),
			Retryable:  true,
			HTTPStatus: http.StatusServiceUnavailable,
		},
		retryAfter: retryAfter,
	}
}

func (e *StrictAffinityUnavailableError) Error() string {
	if e == nil || e.cause == nil {
		return ""
	}
	return e.cause.Error()
}

func (e *StrictAffinityUnavailableError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

func (e *StrictAffinityUnavailableError) StatusCode() int {
	if e == nil || e.cause == nil {
		return 0
	}
	return e.cause.StatusCode()
}

func (e *StrictAffinityUnavailableError) RetryAfter() *time.Duration {
	if e == nil || e.retryAfter <= 0 {
		return nil
	}
	value := e.retryAfter
	return &value
}

func (e *StrictAffinityUnavailableError) SafeResponseHeaders() http.Header {
	if e == nil {
		return nil
	}
	return safeRetryAfterHeader(e.retryAfter)
}

func strictAffinityRetryFor(auth *Auth, model string, now time.Time) time.Duration {
	if auth == nil {
		return strictAffinityRetrySoon
	}
	_, _, next := isAuthBlockedForModel(auth, model, now)
	if next.After(now) {
		return next.Sub(now)
	}
	return strictAffinityRetrySoon
}

func strictAffinityBlockReason(auth *Auth, model string, now time.Time) string {
	if auth == nil {
		return "bound credential is missing"
	}
	blocked, reason, _ := isAuthBlockedForModel(auth, model, now)
	if !blocked {
		return "bound credential is not selectable"
	}
	switch reason {
	case blockReasonDisabled:
		return "bound credential is disabled"
	case blockReasonCooldown:
		return "bound credential is cooling down"
	default:
		return "bound credential is unavailable"
	}
}
