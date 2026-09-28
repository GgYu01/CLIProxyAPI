package usage

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestStreamFromContextDefaultsMissingToFalse(t *testing.T) {
	if StreamFromContext(context.Background()) {
		t.Fatalf("StreamFromContext(background) = true, want false")
	}
}

func TestStreamFromContextHonorsExplicitTrue(t *testing.T) {
	ctx := WithStream(context.Background(), true)
	if !StreamFromContext(ctx) {
		t.Fatalf("StreamFromContext(true) = false, want true")
	}
}

func TestRecordStreamField(t *testing.T) {
	record := Record{
		Provider: "openai",
		Model:    "gpt-5.4",
		Stream:   true,
	}
	if !record.Stream {
		t.Fatalf("Record.Stream = false, want true")
	}
}

func TestRecordBaseURLField(t *testing.T) {
	record := Record{
		Provider: "openai",
		Model:    "gpt-5.4",
		BaseURL:  "https://custom-gateway.example.com/v1",
	}
	if record.BaseURL != "https://custom-gateway.example.com/v1" {
		t.Fatalf("Record.BaseURL = %q, want %q", record.BaseURL, "https://custom-gateway.example.com/v1")
	}
}

func TestGenerateEnabledDefaultsNilToTrue(t *testing.T) {
	if !GenerateEnabled(nil) {
		t.Fatalf("GenerateEnabled(nil) = false, want true")
	}
}

func TestGenerateEnabledHonorsExplicitFalse(t *testing.T) {
	if GenerateEnabled(GenerateFlag(false)) {
		t.Fatalf("GenerateEnabled(false) = true, want false")
	}
}

func TestGenerateEnabledHonorsExplicitTrue(t *testing.T) {
	if !GenerateEnabled(GenerateFlag(true)) {
		t.Fatalf("GenerateEnabled(true) = false, want true")
	}
}

func TestGenerateFromContextDefaultsMissingToTrue(t *testing.T) {
	if !GenerateFromContext(context.Background()) {
		t.Fatalf("GenerateFromContext(background) = false, want true")
	}
}

func TestGenerateFromContextHonorsExplicitFalse(t *testing.T) {
	ctx := WithGenerate(context.Background(), false)
	if GenerateFromContext(ctx) {
		t.Fatalf("GenerateFromContext(false) = true, want false")
	}
}

func TestRecordOmittedGenerateIsEnabled(t *testing.T) {
	// Existing callers construct Record without setting Generate.
	// Omission must remain distinguishable from explicit false and default to true.
	record := Record{
		Provider: "openai",
		Model:    "gpt-5.4",
	}
	if record.Generate != nil {
		t.Fatalf("Record.Generate = %v, want nil for omitted field", record.Generate)
	}
	if !GenerateEnabled(record.Generate) {
		t.Fatalf("GenerateEnabled(omitted) = false, want true")
	}
}

func TestManagerAppliesRecordBackpressureAtConfiguredCapacity(t *testing.T) {
	manager := NewManagerWithLimits(1, 1<<20)
	defer manager.Stop()

	started := make(chan struct{})
	release := make(chan struct{})
	var first sync.Once
	manager.Register(pluginFunc(func(context.Context, Record) {
		first.Do(func() {
			close(started)
			<-release
		})
	}))

	manager.Publish(context.Background(), Record{Model: "first"})
	<-started
	manager.Publish(context.Background(), Record{Model: "second"})

	publishStarted := make(chan struct{})
	publishDone := make(chan struct{})
	go func() {
		close(publishStarted)
		manager.Publish(context.Background(), Record{Model: "third"})
		close(publishDone)
	}()
	<-publishStarted

	stats := waitForManagerStats(t, manager, func(stats ManagerStats) bool {
		return stats.QueuedRecords == 1 && stats.BlockedPublishers == 1
	})
	if stats.MaxRecords != 1 || stats.PeakRecords != 1 || stats.BackpressureTotal != 1 {
		t.Fatalf("manager stats = %+v, want one-record bounded backpressure", stats)
	}
	select {
	case <-publishDone:
		t.Fatal("third Publish returned before queue capacity was released")
	default:
	}

	close(release)
	select {
	case <-publishDone:
	case <-time.After(time.Second):
		t.Fatal("third Publish did not resume after queue capacity was released")
	}
}

func TestManagerAppliesByteBackpressureWithoutDroppingRecord(t *testing.T) {
	manager := NewManagerWithLimits(8, 5)
	defer manager.Stop()

	started := make(chan struct{})
	release := make(chan struct{})
	received := make(chan string, 3)
	var first sync.Once
	manager.Register(pluginFunc(func(_ context.Context, record Record) {
		first.Do(func() {
			close(started)
			<-release
		})
		received <- record.Model
	}))

	manager.Publish(context.Background(), Record{Model: "first"})
	<-started
	manager.Publish(context.Background(), Record{Model: "12345"})

	publishDone := make(chan struct{})
	go func() {
		manager.Publish(context.Background(), Record{Model: "abcde"})
		close(publishDone)
	}()

	stats := waitForManagerStats(t, manager, func(stats ManagerStats) bool {
		return stats.QueuedBytes == 5 && stats.BlockedPublishers == 1
	})
	if stats.MaxBytes != 5 || stats.PeakBytes != 5 {
		t.Fatalf("manager stats = %+v, want five-byte queue peak", stats)
	}

	close(release)
	select {
	case <-publishDone:
	case <-time.After(time.Second):
		t.Fatal("byte-limited Publish did not resume")
	}

	manager.Stop()
	got := map[string]bool{}
	for len(got) < 3 {
		select {
		case model := <-received:
			got[model] = true
		case <-time.After(time.Second):
			t.Fatalf("received models = %v, want all three records", got)
		}
	}
}

func TestManagerReusesBoundedQueueStorageAndClearsDeliveredRecords(t *testing.T) {
	manager := NewManagerWithLimits(8, 1<<20)
	defer manager.Stop()

	delivered := make(chan struct{}, 1)
	manager.Register(pluginFunc(func(context.Context, Record) {
		delivered <- struct{}{}
	}))
	for i := 0; i < 8; i++ {
		manager.Publish(context.Background(), Record{Model: "sensitive-model-value"})
		select {
		case <-delivered:
		case <-time.After(time.Second):
			t.Fatal("usage record was not delivered")
		}
	}

	manager.mu.Lock()
	defer manager.mu.Unlock()
	if len(manager.queue) != 0 {
		t.Fatalf("queue length = %d, want zero after delivery", len(manager.queue))
	}
	if cap(manager.queue) == 0 {
		t.Fatal("queue discarded its bounded backing storage instead of reusing it")
	}
	backing := manager.queue[:cap(manager.queue)]
	for i := range backing {
		if backing[i].record.Model != "" {
			t.Fatalf("delivered queue slot %d still retains record data", i)
		}
	}
}

type pluginFunc func(context.Context, Record)

func (fn pluginFunc) HandleUsage(ctx context.Context, record Record) {
	fn(ctx, record)
}

func waitForManagerStats(t *testing.T, manager *Manager, ready func(ManagerStats) bool) ManagerStats {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		stats := manager.Stats()
		if ready(stats) {
			return stats
		}
		select {
		case <-deadline.C:
			t.Fatalf("manager stats did not reach expected state: %+v", stats)
		case <-ticker.C:
		}
	}
}
