package auth

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

type bodyHoldStream struct {
	sessionID string
	chunks    chan cliproxyexecutor.StreamChunk
	cancel    chan struct{}
}

type bodyHoldExecutor struct {
	id string

	started   chan struct{}
	firstByte <-chan struct{}
	bodyHold  <-chan struct{}

	mu      sync.Mutex
	streams map[string]*bodyHoldStream
	closed  []string
	active  atomic.Int64
}

func newBodyHoldExecutor(id string, firstByte, bodyHold <-chan struct{}) *bodyHoldExecutor {
	return &bodyHoldExecutor{
		id:        id,
		started:   make(chan struct{}, 128),
		firstByte: firstByte,
		bodyHold:  bodyHold,
		streams:   make(map[string]*bodyHoldStream),
	}
}

func (e *bodyHoldExecutor) Identifier() string { return e.id }

func (e *bodyHoldExecutor) Execute(ctx context.Context, auth *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	result, err := e.ExecuteStream(ctx, auth, req, opts)
	if err != nil {
		return cliproxyexecutor.Response{}, err
	}
	var payload []byte
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			return cliproxyexecutor.Response{}, chunk.Err
		}
		payload = append(payload, chunk.Payload...)
	}
	return cliproxyexecutor.Response{Payload: payload}, nil
}

func (e *bodyHoldExecutor) ExecuteStream(ctx context.Context, _ *Auth, _ cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	sessionID := streamSessionID(opts)
	chunks := make(chan cliproxyexecutor.StreamChunk, 2)
	cancel := make(chan struct{})
	e.mu.Lock()
	e.streams[sessionID] = &bodyHoldStream{sessionID: sessionID, chunks: chunks, cancel: cancel}
	e.mu.Unlock()
	e.active.Add(1)
	select {
	case e.started <- struct{}{}:
	default:
	}
	go func() {
		defer func() {
			e.active.Add(-1)
			close(chunks)
			e.mu.Lock()
			delete(e.streams, sessionID)
			e.mu.Unlock()
		}()
		sendErr := func(err error) {
			select {
			case chunks <- cliproxyexecutor.StreamChunk{Err: err}:
			default:
			}
		}
		select {
		case <-ctx.Done():
			sendErr(ctx.Err())
			return
		case <-cancel:
			sendErr(&net.OpError{Op: "read", Err: errors.New("connection reset")})
			return
		case <-e.firstByte:
		}
		select {
		case chunks <- cliproxyexecutor.StreamChunk{Payload: []byte("TOKENS")}:
		case <-ctx.Done():
			sendErr(ctx.Err())
			return
		case <-cancel:
			sendErr(&net.OpError{Op: "read", Err: errors.New("connection reset")})
			return
		}
		select {
		case <-ctx.Done():
			sendErr(ctx.Err())
			return
		case <-cancel:
			sendErr(&net.OpError{Op: "read", Err: errors.New("connection reset")})
			return
		case <-e.bodyHold:
		}
		large := make([]byte, 64*1024)
		for i := range large {
			large[i] = 'A'
		}
		select {
		case chunks <- cliproxyexecutor.StreamChunk{Payload: large}:
		case <-ctx.Done():
			sendErr(ctx.Err())
		case <-cancel:
			sendErr(&net.OpError{Op: "read", Err: errors.New("connection reset")})
		}
	}()
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func (e *bodyHoldExecutor) Refresh(_ context.Context, auth *Auth) (*Auth, error) { return auth, nil }

func (e *bodyHoldExecutor) CountTokens(ctx context.Context, auth *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return e.Execute(ctx, auth, req, opts)
}

func (e *bodyHoldExecutor) HttpRequest(context.Context, *Auth, *http.Request) (*http.Response, error) {
	return nil, nil
}

func (e *bodyHoldExecutor) CloseExecutionSession(sessionID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.closed = append(e.closed, sessionID)
	if sessionID == CloseAllExecutionSessionsID {
		for _, stream := range e.streams {
			select {
			case <-stream.cancel:
			default:
				close(stream.cancel)
			}
		}
		return
	}
	if stream, ok := e.streams[sessionID]; ok {
		select {
		case <-stream.cancel:
		default:
			close(stream.cancel)
		}
	}
}

func (e *bodyHoldExecutor) ClosedSessionIDs() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, len(e.closed))
	copy(out, e.closed)
	return out
}

func streamSessionID(opts cliproxyexecutor.Options) string {
	if opts.Metadata == nil {
		return ""
	}
	raw, _ := opts.Metadata[cliproxyexecutor.ExecutionSessionMetadataKey].(string)
	return raw
}

func TestHotUpdateHundredInFlightLeasesContract(t *testing.T) {
	goroutinesBefore := runtime.NumGoroutine()
	fdsBefore := openFDCount(t)

	manager := NewManager(nil, nil, nil)
	ctx := context.Background()
	const model = "gpt-test"
	good := &Auth{ID: "auth-good", Provider: "codex", Status: StatusActive}
	bad := &Auth{ID: "auth-bad", Provider: "codex", Status: StatusActive}
	other := &Auth{ID: "auth-other", Provider: "gemini", Status: StatusActive}
	reg := registry.GetGlobalRegistry()
	for _, auth := range []*Auth{good, bad, other} {
		reg.RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: model}})
		t.Cleanup(func() { reg.UnregisterClient(auth.ID) })
		if _, err := manager.Register(ctx, auth); err != nil {
			t.Fatalf("register %s: %v", auth.ID, err)
		}
	}

	firstByte := make(chan struct{})
	bodyHold := make(chan struct{})
	first := newBodyHoldExecutor("codex", firstByte, bodyHold)
	manager.RegisterExecutor(first)
	manager.RegisterExecutor(&replaceAwareExecutor{id: "gemini"})
	originalGen := manager.ExecutorGenerationID("codex")
	if originalGen == "" {
		t.Fatal("expected original generation id")
	}

	const inflight = 100
	type streamOutcome struct {
		sessionID    string
		authID       string
		generationID string
		err          error
		bytes        int
	}
	outcomes := make([]streamOutcome, inflight)
	var wg sync.WaitGroup
	wg.Add(inflight)
	for i := 0; i < inflight; i++ {
		i := i
		sessionID := "sess-good-" + strconv.Itoa(i)
		authID := "auth-good"
		if i == 0 {
			sessionID = "sess-revoked"
			authID = "auth-bad"
		}
		opts := cliproxyexecutor.Options{Metadata: map[string]any{
			cliproxyexecutor.ExecutionSessionMetadataKey: sessionID,
			cliproxyexecutor.PinnedAuthMetadataKey:       authID,
		}}
		go func() {
			defer wg.Done()
			outcomes[i].sessionID = sessionID
			outcomes[i].authID = authID
			outcomes[i].generationID = manager.ExecutorGenerationID("codex")
			result, err := manager.ExecuteStream(ctx, []string{"codex"}, cliproxyexecutor.Request{Model: model}, opts)
			if err != nil {
				outcomes[i].err = err
				return
			}
			for chunk := range result.Chunks {
				if chunk.Err != nil {
					outcomes[i].err = chunk.Err
					return
				}
				outcomes[i].bytes += len(chunk.Payload)
			}
		}()
	}

	waitStarted(t, first, inflight)
	if got := manager.ExecutorGenerationLeaseTotal("codex"); got != inflight {
		t.Fatalf("in-flight leases = %d, want %d", got, inflight)
	}

	manager.homeRuntimeAuths["sess-revoked"] = map[string]*Auth{"auth-bad": {ID: "auth-bad", Provider: "codex"}}
	manager.homeRuntimeAuths["sess-good-1"] = map[string]*Auth{"auth-good": {ID: "auth-good", Provider: "codex"}}

	for n := 0; n < 3; n++ {
		next := newBodyHoldExecutor("codex", firstByte, bodyHold)
		manager.RegisterExecutor(next)
		if _, err := manager.Update(ctx, &Auth{ID: "auth-good", Provider: "codex", Status: StatusActive, ProxyURL: "socks5://egress:108" + strconv.Itoa(n)}); err != nil {
			t.Fatalf("same-account proxy update %d: %v", n, err)
		}
	}
	if _, err := manager.Update(ctx, &Auth{ID: "auth-other", Provider: "gemini", Status: StatusActive, Label: "rotated"}); err != nil {
		t.Fatalf("unrelated-account update: %v", err)
	}
	if _, err := manager.Update(ctx, &Auth{ID: "auth-good", Provider: "codex", Status: StatusActive, Disabled: true}); err != nil {
		t.Fatalf("disable: %v", err)
	}
	closed := manager.RevokeAuth("auth-bad", "credential compromised")
	if closed != 1 {
		t.Fatalf("revoke closed = %d, want 1", closed)
	}
	if cause := manager.revokedCauses["auth-bad"]; cause != "credential compromised" {
		t.Fatalf("revoke cause = %q", cause)
	}

	if got := first.ClosedSessionIDs(); !containsSession(got, "sess-revoked") {
		t.Fatalf("revoke must close sess-revoked, got %v", got)
	}
	if containsSession(first.ClosedSessionIDs(), CloseAllExecutionSessionsID) {
		t.Fatalf("generation must not close all sessions while leases are held: %v", first.ClosedSessionIDs())
	}
	published := manager.ExecutorGenerationID("codex")
	if published == "" || published == originalGen {
		t.Fatalf("new generation id = %q, original = %q", published, originalGen)
	}

	close(firstByte)
	close(bodyHold)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight streams did not drain within 5s")
	}

	var unexpectedEOF, unexpectedReset, unexpected503, revokedErrors, okStreams int
	for _, out := range outcomes {
		if out.authID == "auth-bad" {
			if out.err == nil {
				t.Fatal("revoked stream completed without error")
			}
			revokedErrors++
			continue
		}
		if out.err != nil {
			if errors.Is(out.err, io.ErrUnexpectedEOF) || errors.Is(out.err, io.EOF) {
				unexpectedEOF++
			}
			var op *net.OpError
			if errors.As(out.err, &op) {
				unexpectedReset++
			}
			var authErr *Error
			if errors.As(out.err, &authErr) && authErr.HTTPStatus == 503 {
				unexpected503++
			}
			t.Errorf("non-revoked session %s failed: %v", out.sessionID, out.err)
			continue
		}
		if out.bytes < 6 {
			t.Errorf("session %s short body: %d", out.sessionID, out.bytes)
		}
		if out.generationID != originalGen {
			t.Errorf("in-flight generation = %s, want original %s", out.generationID, originalGen)
		}
		okStreams++
	}
	if unexpectedEOF != 0 || unexpectedReset != 0 || unexpected503 != 0 {
		t.Fatalf("unexpected EOF=%d reset=%d 503=%d", unexpectedEOF, unexpectedReset, unexpected503)
	}
	if revokedErrors != 1 {
		t.Fatalf("revoked errors = %d, want 1", revokedErrors)
	}
	if okStreams != inflight-1 {
		t.Fatalf("successful streams = %d, want %d", okStreams, inflight-1)
	}

	spare := &Auth{ID: "auth-spare", Provider: "codex", Status: StatusActive}
	reg.RegisterClient(spare.ID, spare.Provider, []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { reg.UnregisterClient(spare.ID) })
	if _, err := manager.Register(ctx, spare); err != nil {
		t.Fatalf("register spare: %v", err)
	}
	released := alreadyClosed()
	probe := newBodyHoldExecutor("codex", released, released)
	manager.RegisterExecutor(probe)
	newGen := manager.ExecutorGenerationID("codex")
	if newGen == originalGen {
		t.Fatal("post-update selection stayed on original generation")
	}
	result, err := manager.ExecuteStream(ctx, []string{"codex"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{
		Metadata: map[string]any{
			cliproxyexecutor.PinnedAuthMetadataKey:       "auth-spare",
			cliproxyexecutor.ExecutionSessionMetadataKey: "sess-new",
		},
	})
	if err != nil {
		t.Fatalf("new selection after hot-update: %v", err)
	}
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("new selection stream error: %v", chunk.Err)
		}
	}
	if manager.ExecutorGenerationID("codex") != newGen {
		t.Fatalf("published generation moved during new selection")
	}

	waitLeasesZero(t, manager, "codex")
	if first.active.Load() != 0 {
		t.Fatalf("first generation still active = %d", first.active.Load())
	}

	goroutinesAfter := runtime.NumGoroutine()
	if delta := goroutinesAfter - goroutinesBefore; delta > 20 {
		t.Fatalf("goroutine delta %d exceeds bound 20 (before=%d after=%d)", delta, goroutinesBefore, goroutinesAfter)
	}
	fdsAfter := openFDCount(t)
	if delta := fdsAfter - fdsBefore; delta > 16 {
		t.Fatalf("fd delta %d exceeds bound 16 (before=%d after=%d)", delta, fdsBefore, fdsAfter)
	}

	t.Logf("hot-update-100 leases=%d original_gen=%s published_gen=%s new_gen=%s revoked=%d ok=%d goroutine_delta=%d fd_delta=%d close_causes=%v",
		inflight, originalGen, published, newGen, revokedErrors, okStreams, goroutinesAfter-goroutinesBefore, fdsAfter-fdsBefore, first.ClosedSessionIDs())
}

func waitStarted(t *testing.T, exec *bodyHoldExecutor, n int) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case <-exec.started:
		case <-deadline:
			t.Fatalf("started %d/%d streams before timeout", i, n)
		}
	}
}

func waitLeasesZero(t *testing.T, manager *Manager, provider string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		if manager.ExecutorGenerationLeaseTotal(provider) == 0 {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("leases did not drain, remaining=%d", manager.ExecutorGenerationLeaseTotal(provider))
		case <-ticker.C:
		}
	}
}

func alreadyClosed() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func containsSession(ids []string, want string) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func openFDCount(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0
	}
	return len(entries)
}

func TestInvalidUpdateKeepsPreviousGeneration(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	first := &replaceAwareExecutor{id: "codex"}
	manager.RegisterExecutor(first)
	lease := manager.acquireExecutorGeneration("codex")
	original := manager.ExecutorGenerationID("codex")

	second := &replaceAwareExecutor{id: "codex"}
	manager.RegisterExecutor(second)
	if manager.ExecutorGenerationID("codex") == original {
		t.Fatal("valid replace must publish a new generation")
	}
	if closed := first.ClosedSessionIDs(); len(closed) != 0 {
		t.Fatalf("in-flight lease closed during replace: %v", closed)
	}
	lease.release()
	if closed := first.ClosedSessionIDs(); len(closed) != 1 || closed[0] != CloseAllExecutionSessionsID {
		t.Fatalf("want close after last lease, got %v", closed)
	}
}

func TestGenerationLeaseTotalsAcrossReplace(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	first := &replaceAwareExecutor{id: "codex"}
	manager.RegisterExecutor(first)
	leases := make([]*executorGeneration, 8)
	for i := range leases {
		leases[i] = manager.acquireExecutorGeneration("codex")
	}
	if got := manager.ExecutorGenerationLeaseTotal("codex"); got != 8 {
		t.Fatalf("lease total = %d, want 8", got)
	}
	manager.RegisterExecutor(&replaceAwareExecutor{id: "codex"})
	if got := manager.ExecutorGenerationLeaseTotal("codex"); got != 8 {
		t.Fatalf("lease total after replace = %d, want 8", got)
	}
	if manager.ExecutorGenerationActive("codex") != 0 {
		t.Fatalf("current generation should have zero new leases")
	}
	for _, lease := range leases {
		lease.release()
	}
	if got := manager.ExecutorGenerationLeaseTotal("codex"); got != 0 {
		t.Fatalf("lease total after release = %d, want 0", got)
	}
}
