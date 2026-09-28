package config

import "testing"

func TestParseConfigBytesUsageQueueSpoolDefaults(t *testing.T) {
	cfg, err := ParseConfigBytes([]byte(`{}`))
	if err != nil {
		t.Fatalf("ParseConfigBytes() error = %v", err)
	}
	if cfg.RedisUsageQueueSpoolDir != "" {
		t.Fatalf("RedisUsageQueueSpoolDir = %q, want disabled by default", cfg.RedisUsageQueueSpoolDir)
	}
	if cfg.RedisUsageQueueSpoolMaxBytes != 512<<20 {
		t.Fatalf("RedisUsageQueueSpoolMaxBytes = %d, want %d", cfg.RedisUsageQueueSpoolMaxBytes, 512<<20)
	}
}

func TestParseConfigBytesUsageQueueSpoolSettings(t *testing.T) {
	cfg, err := ParseConfigBytes([]byte(`
redis-usage-queue-spool-dir: " /var/lib/cliproxy-api/usage "
redis-usage-queue-spool-max-bytes: 1048576
`))
	if err != nil {
		t.Fatalf("ParseConfigBytes() error = %v", err)
	}
	if cfg.RedisUsageQueueSpoolDir != "/var/lib/cliproxy-api/usage" {
		t.Fatalf("RedisUsageQueueSpoolDir = %q", cfg.RedisUsageQueueSpoolDir)
	}
	if cfg.RedisUsageQueueSpoolMaxBytes != 1048576 {
		t.Fatalf("RedisUsageQueueSpoolMaxBytes = %d, want 1048576", cfg.RedisUsageQueueSpoolMaxBytes)
	}
}
