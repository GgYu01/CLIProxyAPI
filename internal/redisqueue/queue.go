package redisqueue

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultRetentionSeconds int64 = 60
	maxRetentionSeconds     int64 = 3600
	defaultMemoryMaxRecords       = 512
	defaultMemoryMaxBytes   int64 = 16 << 20
	defaultSpoolMaxBytes    int64 = 512 << 20
	usageSubscriberBuffer         = 256
	errorSubscriberBuffer         = 256

	usageSupportRefreshPayload = `{"support_refresh":true}`
	usageRefreshPayload        = `{"refresh":true}`
)

type queueItem struct {
	enqueuedAt time.Time
	payload    []byte
	expires    bool
}

type queue struct {
	mu                sync.Mutex
	space             *sync.Cond
	items             []queueItem
	head              int
	bytes             int64
	peakRecords       int
	peakBytes         int64
	maxRecords        int
	maxBytes          int64
	blockedPublishers int
	backpressureTotal uint64
	subscribers       map[uint64]chan []byte
	nextSubscriberID  uint64
}

// QueueStats reports usage queue memory, disk, and subscriber state without
// consuming records.
type QueueStats struct {
	MemoryRecords     int    `json:"memory_records"`
	MemoryBytes       int64  `json:"memory_bytes"`
	MemoryPeakRecords int    `json:"memory_peak_records"`
	MemoryPeakBytes   int64  `json:"memory_peak_bytes"`
	MemoryMaxRecords  int    `json:"memory_max_records"`
	MemoryMaxBytes    int64  `json:"memory_max_bytes"`
	DiskRecords       int    `json:"disk_records"`
	DiskBytes         int64  `json:"disk_bytes"`
	DiskPeakRecords   int    `json:"disk_peak_records"`
	DiskPeakBytes     int64  `json:"disk_peak_bytes"`
	DiskMaxBytes      int64  `json:"disk_max_bytes"`
	Subscribers       int    `json:"subscribers"`
	BlockedPublishers int    `json:"blocked_publishers"`
	BackpressureTotal uint64 `json:"backpressure_total"`
	DiskWriteErrors   uint64 `json:"disk_write_errors"`
}

var (
	enabled          atomic.Bool
	retentionSeconds atomic.Int64
	global           queue
	errorGlobal      queue
	usageSpool       diskSpool
)

func init() {
	retentionSeconds.Store(defaultRetentionSeconds)
	global.init(defaultMemoryMaxRecords, defaultMemoryMaxBytes)
	errorGlobal.init(defaultMemoryMaxRecords, defaultMemoryMaxBytes)
	usageSpool.init(defaultSpoolMaxBytes)
}

func (q *queue) init(maxRecords int, maxBytes int64) {
	q.maxRecords = maxRecords
	q.maxBytes = maxBytes
	q.space = sync.NewCond(&q.mu)
}

func SetEnabled(value bool) {
	enabled.Store(value)
	if !value {
		global.clear()
		errorGlobal.clear()
		usageSpool.wake()
	}
}

func Enabled() bool {
	return enabled.Load()
}

func SetRetentionSeconds(value int) {
	normalized := int64(value)
	if normalized <= 0 {
		normalized = defaultRetentionSeconds
	} else if normalized > maxRetentionSeconds {
		normalized = maxRetentionSeconds
	}
	retentionSeconds.Store(normalized)
}

func Enqueue(payload []byte) {
	if !Enabled() {
		return
	}
	if len(payload) == 0 {
		return
	}
	spoolEnabled := usageSpool.enabled()
	if spoolEnabled && global.hasRecords() {
		// A previous disk write already fell back to memory. Keep every newer
		// record behind it until the disk backlog and fallback are drained.
		global.enqueueRetained(payload)
		return
	}
	if usageSpool.hasRecords() {
		if usageSpool.enqueue(payload) {
			return
		}
		global.enqueueRetained(payload)
		return
	}
	if global.publishToSubscribers(payload) {
		return
	}
	if spoolEnabled {
		if usageSpool.enqueue(payload) {
			return
		}
		global.enqueueRetained(payload)
		return
	}
	global.enqueue(payload)
}

func EnqueueError(payload []byte) {
	if !Enabled() {
		return
	}
	if len(payload) == 0 {
		return
	}
	errorGlobal.publishToSubscribers(payload)
}

func PopOldest(count int) [][]byte {
	if !Enabled() {
		return nil
	}
	if count <= 0 {
		return nil
	}
	// Disk records are older than the emergency memory fallback: once the
	// durable spool contains data, new records keep targeting that spool and
	// reach memory only when a disk write fails.
	items := usageSpool.popOldest(count)
	if len(items) < count {
		items = append(items, global.popOldest(count-len(items))...)
	}
	return items
}

// SetSpoolDirectory enables a durable usage spool. An empty path disables new
// disk writes without deleting already persisted records.
func SetSpoolDirectory(path string) error {
	return usageSpool.setDirectory(path)
}

// SetSpoolMaxBytes sets the durable usage spool payload-byte budget.
func SetSpoolMaxBytes(value int64) {
	if value <= 0 {
		value = defaultSpoolMaxBytes
	}
	usageSpool.setMaxBytes(value)
}

// Stats returns a read-only queue snapshot.
func Stats() QueueStats {
	global.mu.Lock()
	memoryRecords := len(global.items) - global.head
	stats := QueueStats{
		MemoryRecords:     memoryRecords,
		MemoryBytes:       global.bytes,
		MemoryPeakRecords: global.peakRecords,
		MemoryPeakBytes:   global.peakBytes,
		MemoryMaxRecords:  global.maxRecords,
		MemoryMaxBytes:    global.maxBytes,
		Subscribers:       len(global.subscribers),
		BlockedPublishers: global.blockedPublishers,
		BackpressureTotal: global.backpressureTotal,
	}
	global.mu.Unlock()

	disk := usageSpool.stats()
	stats.DiskRecords = disk.records
	stats.DiskBytes = disk.bytes
	stats.DiskPeakRecords = disk.peakRecords
	stats.DiskPeakBytes = disk.peakBytes
	stats.DiskMaxBytes = disk.maxBytes
	stats.BlockedPublishers += disk.blockedPublishers
	stats.BackpressureTotal += disk.backpressureTotal
	stats.DiskWriteErrors = disk.writeErrors
	return stats
}

func SubscribeUsage() (<-chan []byte, func()) {
	return global.subscribe(usageSubscriberBuffer, []byte(usageSupportRefreshPayload))
}

func SubscribeErrors() (<-chan []byte, func()) {
	return errorGlobal.subscribe(errorSubscriberBuffer, nil)
}

func NotifyUsageRefresh() {
	global.publishToSubscribers([]byte(usageRefreshPayload))
}

func (q *queue) clear() {
	q.mu.Lock()

	subscribers := make([]chan []byte, 0, len(q.subscribers))
	for _, subscriber := range q.subscribers {
		subscribers = append(subscribers, subscriber)
	}
	q.items = nil
	q.head = 0
	q.bytes = 0
	q.peakRecords = 0
	q.peakBytes = 0
	q.backpressureTotal = 0
	q.subscribers = nil
	q.mu.Unlock()
	q.space.Broadcast()

	for _, subscriber := range subscribers {
		close(subscriber)
	}
}

func (q *queue) enqueue(payload []byte) {
	q.enqueueWithExpiry(payload, true)
}

func (q *queue) enqueueRetained(payload []byte) {
	q.enqueueWithExpiry(payload, false)
}

func (q *queue) enqueueWithExpiry(payload []byte, expires bool) {
	now := time.Now()

	q.mu.Lock()
	defer q.mu.Unlock()

	q.pruneLocked(now)
	blocked := false
	for Enabled() && !q.canEnqueueLocked(len(payload)) {
		if !blocked {
			blocked = true
			q.blockedPublishers++
			q.backpressureTotal++
		}
		q.space.Wait()
		q.pruneLocked(time.Now())
	}
	if blocked {
		q.blockedPublishers--
	}
	if !Enabled() {
		return
	}
	q.items = append(q.items, queueItem{
		enqueuedAt: time.Now(),
		payload:    append([]byte(nil), payload...),
		expires:    expires,
	})
	q.bytes += int64(len(payload))
	records := len(q.items) - q.head
	if records > q.peakRecords {
		q.peakRecords = records
	}
	if q.bytes > q.peakBytes {
		q.peakBytes = q.bytes
	}
	q.maybeCompactLocked()
}

func (q *queue) hasRecords() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.head < len(q.items)
}

func (q *queue) canEnqueueLocked(payloadBytes int) bool {
	records := len(q.items) - q.head
	if records >= q.maxRecords {
		return false
	}
	if q.bytes+int64(payloadBytes) <= q.maxBytes {
		return true
	}
	return records == 0
}

func (q *queue) publishToSubscribers(payload []byte) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.subscribers) == 0 {
		return false
	}

	delivered := false
	for id, subscriber := range q.subscribers {
		cloned := append([]byte(nil), payload...)
		select {
		case subscriber <- cloned:
			delivered = true
		default:
			delete(q.subscribers, id)
			close(subscriber)
		}
	}

	return delivered
}

func (q *queue) subscribe(buffer int, initialPayload []byte) (<-chan []byte, func()) {
	subscriber := make(chan []byte, buffer)
	if len(initialPayload) > 0 {
		subscriber <- append([]byte(nil), initialPayload...)
	}

	q.mu.Lock()
	if q.subscribers == nil {
		q.subscribers = make(map[uint64]chan []byte)
	}
	q.nextSubscriberID++
	id := q.nextSubscriberID
	q.subscribers[id] = subscriber
	q.mu.Unlock()

	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			q.unsubscribe(id)
		})
	}
	return subscriber, unsubscribe
}

func (q *queue) unsubscribe(id uint64) {
	q.mu.Lock()
	subscriber, ok := q.subscribers[id]
	if ok {
		delete(q.subscribers, id)
	}
	q.mu.Unlock()

	if ok {
		close(subscriber)
	}
}

func (q *queue) popOldest(count int) [][]byte {
	now := time.Now()

	q.mu.Lock()
	defer q.mu.Unlock()

	q.pruneLocked(now)
	available := len(q.items) - q.head
	if available <= 0 {
		q.items = nil
		q.head = 0
		return nil
	}
	if count > available {
		count = available
	}

	out := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		item := q.items[q.head+i]
		out = append(out, item.payload)
		q.bytes -= int64(len(item.payload))
	}
	q.head += count
	q.maybeCompactLocked()
	q.space.Broadcast()
	return out
}

func (q *queue) pruneLocked(now time.Time) {
	if q.head >= len(q.items) {
		q.items = nil
		q.head = 0
		return
	}

	windowSeconds := retentionSeconds.Load()
	if windowSeconds <= 0 {
		windowSeconds = defaultRetentionSeconds
	}
	cutoff := now.Add(-time.Duration(windowSeconds) * time.Second)
	for q.head < len(q.items) && q.items[q.head].expires && q.items[q.head].enqueuedAt.Before(cutoff) {
		q.bytes -= int64(len(q.items[q.head].payload))
		q.head++
	}
	if q.bytes < 0 {
		q.bytes = 0
	}
}

func (q *queue) maybeCompactLocked() {
	if q.head == 0 {
		return
	}
	if q.head >= len(q.items) {
		q.items = nil
		q.head = 0
		return
	}
	if q.head < 1024 && q.head*2 < len(q.items) {
		return
	}
	q.items = append([]queueItem(nil), q.items[q.head:]...)
	q.head = 0
}
