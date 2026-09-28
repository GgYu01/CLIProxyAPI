package auth

import (
	"context"
	"testing"
)

func TestRegisterExecutorDoesNotCloseActiveGeneration(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	first := &replaceAwareExecutor{id: "codex"}
	second := &replaceAwareExecutor{id: "codex"}

	manager.RegisterExecutor(first)
	lease := manager.acquireExecutorGeneration("codex")
	if lease == nil {
		t.Fatal("expected generation lease")
	}
	if got := lease.active.Load(); got != 1 {
		t.Fatalf("active=%d want 1", got)
	}

	manager.RegisterExecutor(second)
	if closed := first.ClosedSessionIDs(); len(closed) != 0 {
		t.Fatalf("in-flight generation closed on replace: %v", closed)
	}
	if !lease.draining.Load() {
		t.Fatal("previous generation should be draining")
	}

	lease.release()
	closed := first.ClosedSessionIDs()
	if len(closed) != 1 || closed[0] != CloseAllExecutionSessionsID {
		t.Fatalf("want close after last lease, got %v", closed)
	}
	if len(second.ClosedSessionIDs()) != 0 {
		t.Fatal("replacement executor must stay open")
	}
}

func TestRegisterExecutorKeepsHundredActiveLeasesAcrossThreeReplaces(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	current := &replaceAwareExecutor{id: "codex"}
	manager.RegisterExecutor(current)

	leases := make([]*executorGeneration, 100)
	for i := 0; i < 100; i++ {
		leases[i] = manager.acquireExecutorGeneration("codex")
		if leases[i] == nil {
			t.Fatal("expected lease")
		}
	}
	first := current
	for n := 0; n < 3; n++ {
		next := &replaceAwareExecutor{id: "codex"}
		manager.RegisterExecutor(next)
		current = next
	}
	if closed := first.ClosedSessionIDs(); len(closed) != 0 {
		t.Fatalf("active leases closed during replace: %v", closed)
	}
	for _, lease := range leases {
		if lease.executor != first {
			t.Fatal("in-flight leases must stay on the original generation")
		}
		lease.release()
	}
	if closed := first.ClosedSessionIDs(); len(closed) != 1 || closed[0] != CloseAllExecutionSessionsID {
		t.Fatalf("want close after last lease, got %v", closed)
	}
}

func TestRegisterExecutorClosesIdlePreviousGeneration(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	first := &replaceAwareExecutor{id: "codex"}
	second := &replaceAwareExecutor{id: "codex"}
	manager.RegisterExecutor(first)
	manager.RegisterExecutor(second)
	closed := first.ClosedSessionIDs()
	if len(closed) != 1 || closed[0] != CloseAllExecutionSessionsID {
		t.Fatalf("idle previous generation should close immediately, got %v", closed)
	}
}

func TestUnregisterExecutorDrainsActiveGeneration(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	first := &replaceAwareExecutor{id: "codex"}
	manager.RegisterExecutor(first)
	lease := manager.acquireExecutorGeneration("codex")
	if lease == nil {
		t.Fatal("expected lease")
	}

	manager.UnregisterExecutor("codex")
	if closed := first.ClosedSessionIDs(); len(closed) != 0 {
		t.Fatalf("active lease closed on unregister: %v", closed)
	}
	if _, ok := manager.Executor("codex"); ok {
		t.Fatal("unregistered executor must stop serving new lookups")
	}
	lease.release()
	if closed := first.ClosedSessionIDs(); len(closed) != 1 || closed[0] != CloseAllExecutionSessionsID {
		t.Fatalf("want close after last lease, got %v", closed)
	}
}

func TestUnregisterExecutorClosesIdleGeneration(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	first := &replaceAwareExecutor{id: "codex"}
	manager.RegisterExecutor(first)
	manager.UnregisterExecutor("codex")
	if closed := first.ClosedSessionIDs(); len(closed) != 1 || closed[0] != CloseAllExecutionSessionsID {
		t.Fatalf("idle generation should close on unregister, got %v", closed)
	}
}

func TestDisableAuthPreservesInflightGeneration(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	ctx := context.Background()
	if _, errRegister := manager.Register(ctx, &Auth{ID: "auth-1", Provider: "codex"}); errRegister != nil {
		t.Fatalf("register auth: %v", errRegister)
	}
	first := &replaceAwareExecutor{id: "codex"}
	manager.RegisterExecutor(first)
	lease := manager.acquireExecutorGeneration("codex")
	if lease == nil {
		t.Fatal("expected lease")
	}

	disabled := &Auth{ID: "auth-1", Provider: "codex", Disabled: true}
	if _, errUpdate := manager.Update(ctx, disabled); errUpdate != nil {
		t.Fatalf("disable auth: %v", errUpdate)
	}
	if closed := first.ClosedSessionIDs(); len(closed) != 0 {
		t.Fatalf("disable must drain in-flight sessions, got close: %v", closed)
	}
	stored, ok := manager.auths["auth-1"]
	if !ok || stored == nil || !stored.Disabled {
		t.Fatal("disabled flag must persist so selection stops picking the auth")
	}
	lease.release()
	if closed := first.ClosedSessionIDs(); len(closed) != 0 {
		t.Fatalf("natural drain must not close sessions either, got: %v", closed)
	}
}

func TestUnrelatedAuthUpdatePreservesOtherGeneration(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	ctx := context.Background()
	if _, errRegister := manager.Register(ctx, &Auth{ID: "auth-1", Provider: "codex"}); errRegister != nil {
		t.Fatalf("register auth-1: %v", errRegister)
	}
	if _, errRegister := manager.Register(ctx, &Auth{ID: "auth-2", Provider: "gemini"}); errRegister != nil {
		t.Fatalf("register auth-2: %v", errRegister)
	}
	first := &replaceAwareExecutor{id: "codex"}
	manager.RegisterExecutor(first)
	lease := manager.acquireExecutorGeneration("codex")
	if lease == nil {
		t.Fatal("expected lease")
	}

	if _, errUpdate := manager.Update(ctx, &Auth{ID: "auth-2", Provider: "gemini", Label: "rotated"}); errUpdate != nil {
		t.Fatalf("update unrelated auth: %v", errUpdate)
	}
	if closed := first.ClosedSessionIDs(); len(closed) != 0 {
		t.Fatalf("unrelated update closed foreign sessions: %v", closed)
	}
	if _, ok := manager.Executor("codex"); !ok {
		t.Fatal("unrelated update must not drop the live executor")
	}
	stored, ok := manager.auths["auth-1"]
	if !ok || stored == nil || stored.Disabled {
		t.Fatal("unrelated update must not touch the other auth")
	}
	lease.release()
}

func TestRevokeAuthCancelsOnlyTargetSessions(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	ctx := context.Background()
	if _, errRegister := manager.Register(ctx, &Auth{ID: "auth-bad", Provider: "codex"}); errRegister != nil {
		t.Fatalf("register revoked auth: %v", errRegister)
	}
	if _, errRegister := manager.Register(ctx, &Auth{ID: "auth-good", Provider: "codex"}); errRegister != nil {
		t.Fatalf("register good auth: %v", errRegister)
	}
	badExec := &replaceAwareExecutor{id: "codex"}
	manager.RegisterExecutor(badExec)
	manager.homeRuntimeAuths["sess-bad"] = map[string]*Auth{"auth-bad": {ID: "auth-bad", Provider: "codex"}}
	manager.homeRuntimeAuths["sess-good"] = map[string]*Auth{"auth-good": {ID: "auth-good", Provider: "codex"}}

	closed := manager.RevokeAuth("auth-bad", "credential compromised")
	if closed != 1 {
		t.Fatalf("want exactly 1 closed session, got %d", closed)
	}
	if got := badExec.ClosedSessionIDs(); len(got) != 1 || got[0] != "sess-bad" {
		t.Fatalf("only the revoked session must close, got %v", got)
	}
	if _, ok := manager.homeRuntimeAuths["sess-good"]["auth-good"]; !ok {
		t.Fatal("unrelated session mapping must survive revoke")
	}
	stored, ok := manager.auths["auth-bad"]
	if !ok || stored == nil || !stored.Disabled {
		t.Fatal("revoked auth must stop new selection")
	}
	if cause := manager.revokedCauses["auth-bad"]; cause != "credential compromised" {
		t.Fatalf("revoke cause must be audited, got %q", cause)
	}
	good, ok := manager.auths["auth-good"]
	if !ok || good == nil || good.Disabled {
		t.Fatal("unrelated auth must stay enabled")
	}
}
