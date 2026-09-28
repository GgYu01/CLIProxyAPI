package redisqueue

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEnqueueBroadcastsToUsageSubscribersAndSkipsQueue(t *testing.T) {
	withEnabledQueue(t, func() {
		first, unsubscribeFirst := SubscribeUsage()
		defer unsubscribeFirst()
		second, unsubscribeSecond := SubscribeUsage()
		defer unsubscribeSecond()

		requireUsageSubscriberPayload(t, first, usageSupportRefreshPayload)
		requireUsageSubscriberPayload(t, second, usageSupportRefreshPayload)

		Enqueue([]byte("usage-record"))

		requireUsageSubscriberPayload(t, first, "usage-record")
		requireUsageSubscriberPayload(t, second, "usage-record")

		if items := PopOldest(1); len(items) != 0 {
			t.Fatalf("PopOldest() items = %q, want empty after subscriber broadcast", items)
		}

		unsubscribeFirst()
		unsubscribeSecond()

		Enqueue([]byte("queued-record"))
		items := PopOldest(1)
		if len(items) != 1 || string(items[0]) != "queued-record" {
			t.Fatalf("PopOldest() items = %q, want queued record after unsubscribe", items)
		}
	})
}

func TestSetEnabledFalseClosesUsageSubscribers(t *testing.T) {
	withEnabledQueue(t, func() {
		subscriber, unsubscribe := SubscribeUsage()
		defer unsubscribe()
		errorSubscriber, unsubscribeErrors := SubscribeErrors()
		defer unsubscribeErrors()

		requireUsageSubscriberPayload(t, subscriber, usageSupportRefreshPayload)

		SetEnabled(false)

		select {
		case _, ok := <-subscriber:
			if ok {
				t.Fatalf("subscriber channel remained open after SetEnabled(false)")
			}
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for subscriber close")
		}

		select {
		case _, ok := <-errorSubscriber:
			if ok {
				t.Fatalf("error subscriber channel remained open after SetEnabled(false)")
			}
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for error subscriber close")
		}
	})
}

func TestEnqueueErrorBroadcastsToErrorSubscribersAndDiscardsWithoutSubscribers(t *testing.T) {
	withEnabledQueue(t, func() {
		subscriber, unsubscribe := SubscribeErrors()
		defer unsubscribe()

		EnqueueError([]byte("error-record"))
		requireUsageSubscriberPayload(t, subscriber, "error-record")

		unsubscribe()

		EnqueueError([]byte("discarded-error"))
		requireErrorQueueEmpty(t)
	})
}

func TestNotifyUsageRefreshBroadcastsOnlyToUsageSubscribers(t *testing.T) {
	withEnabledQueue(t, func() {
		subscriber, unsubscribe := SubscribeUsage()
		defer unsubscribe()
		errorSubscriber, unsubscribeErrors := SubscribeErrors()
		defer unsubscribeErrors()

		requireUsageSubscriberPayload(t, subscriber, usageSupportRefreshPayload)

		NotifyUsageRefresh()
		requireUsageSubscriberPayload(t, subscriber, usageRefreshPayload)

		select {
		case got := <-errorSubscriber:
			t.Fatalf("error subscriber received usage refresh payload %q", string(got))
		default:
		}

		unsubscribe()
		NotifyUsageRefresh()
		if items := PopOldest(1); len(items) != 0 {
			t.Fatalf("PopOldest() items = %q, want empty after refresh notification without subscribers", items)
		}
	})
}

func TestEnqueueFallsBackWhenUsageSubscriberBufferIsFull(t *testing.T) {
	withEnabledQueue(t, func() {
		subscriber, unsubscribe := SubscribeUsage()
		defer unsubscribe()
		requireUsageSubscriberPayload(t, subscriber, usageSupportRefreshPayload)

		for i := 0; i < usageSubscriberBuffer; i++ {
			Enqueue([]byte(fmt.Sprintf("buffered-%03d", i)))
		}
		Enqueue([]byte("overflow"))

		items := PopOldest(1)
		if len(items) != 1 || string(items[0]) != "overflow" {
			t.Fatalf("PopOldest() items = %q, want overflow payload preserved", items)
		}
	})
}

func TestUsageDiskSpoolPersistsAndReplaysInOrder(t *testing.T) {
	withEnabledQueue(t, func() {
		spoolDir := t.TempDir()
		if err := SetSpoolDirectory(spoolDir); err != nil {
			t.Fatalf("SetSpoolDirectory() error = %v", err)
		}
		defer func() { _ = SetSpoolDirectory("") }()
		SetSpoolMaxBytes(1024)

		Enqueue([]byte("first"))
		Enqueue([]byte("second"))

		stats := Stats()
		if stats.DiskRecords != 2 || stats.DiskBytes != 11 {
			t.Fatalf("Stats() = %+v, want two disk records and 11 bytes", stats)
		}

		if err := SetSpoolDirectory(""); err != nil {
			t.Fatalf("disable spool: %v", err)
		}
		if err := SetSpoolDirectory(spoolDir); err != nil {
			t.Fatalf("reload spool: %v", err)
		}

		items := PopOldest(2)
		if len(items) != 2 || string(items[0]) != "first" || string(items[1]) != "second" {
			t.Fatalf("PopOldest() items = %q, want persisted insertion order", items)
		}
		stats = Stats()
		if stats.DiskRecords != 0 || stats.DiskBytes != 0 {
			t.Fatalf("Stats() after replay = %+v, want empty disk spool", stats)
		}
	})
}

func TestUsageDiskSpoolBackpressuresAtByteLimitWithoutDropping(t *testing.T) {
	withEnabledQueue(t, func() {
		if err := SetSpoolDirectory(t.TempDir()); err != nil {
			t.Fatalf("SetSpoolDirectory() error = %v", err)
		}
		defer func() { _ = SetSpoolDirectory("") }()
		SetSpoolMaxBytes(5)

		Enqueue([]byte("first"))
		done := make(chan struct{})
		go func() {
			Enqueue([]byte("next"))
			close(done)
		}()

		waitForQueueStats(t, func(stats QueueStats) bool {
			return stats.BlockedPublishers == 1
		})
		select {
		case <-done:
			t.Fatal("Enqueue returned before disk capacity was released")
		default:
		}

		items := PopOldest(1)
		if len(items) != 1 || string(items[0]) != "first" {
			t.Fatalf("first PopOldest() = %q, want first", items)
		}
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Enqueue did not resume after disk capacity was released")
		}
		items = PopOldest(1)
		if len(items) != 1 || string(items[0]) != "next" {
			t.Fatalf("second PopOldest() = %q, want next", items)
		}
	})
}

func TestUsageDiskSpoolReplaysOlderDiskRecordBeforeEmergencyMemoryFallback(t *testing.T) {
	withEnabledQueue(t, func() {
		root := t.TempDir()
		spoolDir := filepath.Join(root, "usage-spool")
		if err := SetSpoolDirectory(spoolDir); err != nil {
			t.Fatalf("SetSpoolDirectory() error = %v", err)
		}
		defer func() { _ = SetSpoolDirectory("") }()
		SetSpoolMaxBytes(1024)

		Enqueue([]byte("older-on-disk"))
		movedDir := filepath.Join(root, "usage-spool-moved")
		if err := os.Rename(spoolDir, movedDir); err != nil {
			t.Fatalf("move spool out of the write path: %v", err)
		}
		if err := os.WriteFile(spoolDir, []byte("not-a-directory"), 0o600); err != nil {
			t.Fatalf("replace spool path with a file: %v", err)
		}

		Enqueue([]byte("newer-memory-fallback"))
		if err := os.Remove(spoolDir); err != nil {
			t.Fatalf("remove failed spool path: %v", err)
		}
		if err := os.Rename(movedDir, spoolDir); err != nil {
			t.Fatalf("restore spool directory: %v", err)
		}

		items := PopOldest(2)
		if len(items) != 2 || string(items[0]) != "older-on-disk" || string(items[1]) != "newer-memory-fallback" {
			t.Fatalf("PopOldest() items = %q, want disk record before emergency memory fallback", items)
		}
	})
}

func TestUsageDiskSpoolKeepsEmergencyFallbackOrderedAndUnexpired(t *testing.T) {
	withEnabledQueue(t, func() {
		root := t.TempDir()
		spoolDir := filepath.Join(root, "usage-spool")
		if err := SetSpoolDirectory(spoolDir); err != nil {
			t.Fatalf("SetSpoolDirectory() error = %v", err)
		}
		defer func() { _ = SetSpoolDirectory("") }()
		SetSpoolMaxBytes(1024)

		Enqueue([]byte("first-disk"))
		movedDir := filepath.Join(root, "usage-spool-moved")
		if err := os.Rename(spoolDir, movedDir); err != nil {
			t.Fatalf("move spool out of the write path: %v", err)
		}
		if err := os.WriteFile(spoolDir, []byte("not-a-directory"), 0o600); err != nil {
			t.Fatalf("replace spool path with a file: %v", err)
		}
		Enqueue([]byte("second-memory-fallback"))
		if stats := Stats(); stats.DiskWriteErrors != 1 {
			t.Fatalf("Stats().DiskWriteErrors = %d, want 1", stats.DiskWriteErrors)
		}
		if err := os.Remove(spoolDir); err != nil {
			t.Fatalf("remove failed spool path: %v", err)
		}
		if err := os.Rename(movedDir, spoolDir); err != nil {
			t.Fatalf("restore spool directory: %v", err)
		}

		global.mu.Lock()
		if global.head >= len(global.items) {
			global.mu.Unlock()
			t.Fatal("emergency memory fallback was not retained")
		}
		global.items[global.head].enqueuedAt = time.Now().Add(-2 * time.Hour)
		global.mu.Unlock()
		Enqueue([]byte("third-after-recovery"))

		items := PopOldest(3)
		if len(items) != 3 ||
			string(items[0]) != "first-disk" ||
			string(items[1]) != "second-memory-fallback" ||
			string(items[2]) != "third-after-recovery" {
			t.Fatalf("PopOldest() items = %q, want complete insertion order across disk recovery", items)
		}
	})
}

func requireUsageSubscriberPayload(t *testing.T, subscriber <-chan []byte, want string) {
	t.Helper()

	select {
	case got, ok := <-subscriber:
		if !ok {
			t.Fatalf("subscriber closed before receiving %q", want)
		}
		if string(got) != want {
			t.Fatalf("subscriber payload = %q, want %q", string(got), want)
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for subscriber payload %q", want)
	}
}

func requireErrorQueueEmpty(t *testing.T) {
	t.Helper()

	errorGlobal.mu.Lock()
	defer errorGlobal.mu.Unlock()

	if len(errorGlobal.items)-errorGlobal.head != 0 {
		t.Fatalf("error queue retained %d item(s), want none", len(errorGlobal.items)-errorGlobal.head)
	}
}

func waitForQueueStats(t *testing.T, ready func(QueueStats) bool) QueueStats {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		stats := Stats()
		if ready(stats) {
			return stats
		}
		select {
		case <-deadline.C:
			t.Fatalf("queue stats did not reach expected state: %+v", stats)
		case <-ticker.C:
		}
	}
}
