package redisqueue

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const usageSpoolSuffix = ".usage"

type diskSpoolStats struct {
	records           int
	bytes             int64
	peakRecords       int
	peakBytes         int64
	maxBytes          int64
	blockedPublishers int
	backpressureTotal uint64
	writeErrors       uint64
}

type diskSpool struct {
	mu                sync.Mutex
	space             *sync.Cond
	dir               string
	maxBytes          int64
	records           int
	bytes             int64
	peakRecords       int
	peakBytes         int64
	sequence          uint64
	blockedPublishers int
	backpressureTotal uint64
	writeErrors       uint64
}

func (s *diskSpool) init(maxBytes int64) {
	s.maxBytes = maxBytes
	s.space = sync.NewCond(&s.mu)
}

func (s *diskSpool) setDirectory(path string) error {
	path = strings.TrimSpace(path)
	if path == "" {
		s.mu.Lock()
		s.dir = ""
		s.records = 0
		s.bytes = 0
		s.peakRecords = 0
		s.peakBytes = 0
		s.backpressureTotal = 0
		s.writeErrors = 0
		s.mu.Unlock()
		s.space.Broadcast()
		return nil
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve usage spool directory: %w", err)
	}
	if err = os.MkdirAll(absPath, 0o700); err != nil {
		return fmt.Errorf("create usage spool directory: %w", err)
	}
	if err = os.Chmod(absPath, 0o700); err != nil {
		return fmt.Errorf("set usage spool directory permissions: %w", err)
	}

	records, bytes, err := scanUsageSpool(absPath)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.dir = absPath
	s.records = records
	s.bytes = bytes
	s.peakRecords = records
	s.peakBytes = bytes
	s.blockedPublishers = 0
	s.backpressureTotal = 0
	s.writeErrors = 0
	s.mu.Unlock()
	s.space.Broadcast()
	return nil
}

func (s *diskSpool) setMaxBytes(maxBytes int64) {
	s.mu.Lock()
	s.maxBytes = maxBytes
	s.mu.Unlock()
	s.space.Broadcast()
}

func (s *diskSpool) enabled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dir != ""
}

func (s *diskSpool) hasRecords() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dir != "" && s.records > 0
}

func (s *diskSpool) enqueue(payload []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	blocked := false
	for Enabled() && s.dir != "" && !s.canEnqueueLocked(len(payload)) {
		if !blocked {
			blocked = true
			s.blockedPublishers++
			s.backpressureTotal++
		}
		s.space.Wait()
	}
	if blocked {
		s.blockedPublishers--
	}
	if !Enabled() || s.dir == "" {
		return false
	}

	temporary, err := os.CreateTemp(s.dir, ".usage-*.tmp")
	if err != nil {
		s.writeErrors++
		return false
	}
	temporaryPath := temporary.Name()
	committed := false
	defer func() {
		if !committed {
			_ = temporary.Close()
			_ = os.Remove(temporaryPath)
		}
	}()

	if err = temporary.Chmod(0o600); err != nil {
		s.writeErrors++
		return false
	}
	if _, err = temporary.Write(payload); err != nil {
		s.writeErrors++
		return false
	}
	if err = temporary.Sync(); err != nil {
		s.writeErrors++
		return false
	}
	if err = temporary.Close(); err != nil {
		s.writeErrors++
		return false
	}

	s.sequence++
	name := fmt.Sprintf("%020d-%020d%s", time.Now().UnixNano(), s.sequence, usageSpoolSuffix)
	finalPath := filepath.Join(s.dir, name)
	if err = os.Rename(temporaryPath, finalPath); err != nil {
		s.writeErrors++
		return false
	}
	committed = true
	s.records++
	s.bytes += int64(len(payload))
	if s.records > s.peakRecords {
		s.peakRecords = s.records
	}
	if s.bytes > s.peakBytes {
		s.peakBytes = s.bytes
	}
	return true
}

func (s *diskSpool) canEnqueueLocked(payloadBytes int) bool {
	if s.bytes+int64(payloadBytes) <= s.maxBytes {
		return true
	}
	return s.records == 0
}

func (s *diskSpool) popOldest(count int) [][]byte {
	if count <= 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dir == "" || s.records == 0 {
		return nil
	}

	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.Type().IsRegular() && strings.HasSuffix(entry.Name(), usageSpoolSuffix) {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	if len(names) > count {
		names = names[:count]
	}

	items := make([][]byte, 0, len(names))
	for _, name := range names {
		path := filepath.Join(s.dir, name)
		payload, errRead := os.ReadFile(path)
		if errRead != nil {
			break
		}
		if errRemove := os.Remove(path); errRemove != nil {
			break
		}
		items = append(items, payload)
		s.records--
		s.bytes -= int64(len(payload))
	}
	if s.records < 0 {
		s.records = 0
	}
	if s.bytes < 0 {
		s.bytes = 0
	}
	s.space.Broadcast()
	return items
}

func (s *diskSpool) stats() diskSpoolStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return diskSpoolStats{
		records:           s.records,
		bytes:             s.bytes,
		peakRecords:       s.peakRecords,
		peakBytes:         s.peakBytes,
		maxBytes:          s.maxBytes,
		blockedPublishers: s.blockedPublishers,
		backpressureTotal: s.backpressureTotal,
		writeErrors:       s.writeErrors,
	}
}

func (s *diskSpool) wake() {
	s.space.Broadcast()
}

func scanUsageSpool(dir string) (int, int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0, 0, fmt.Errorf("read usage spool directory: %w", err)
	}
	records := 0
	var bytes int64
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), usageSpoolSuffix) {
			continue
		}
		info, errInfo := entry.Info()
		if errInfo != nil {
			return 0, 0, fmt.Errorf("inspect usage spool record: %w", errInfo)
		}
		records++
		bytes += info.Size()
	}
	return records, bytes, nil
}
