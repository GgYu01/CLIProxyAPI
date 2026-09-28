package usage

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	internallogging "github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	log "github.com/sirupsen/logrus"
)

// DefaultServiceTier is retained for direct SDK and non-OpenAI usage callers.
const DefaultServiceTier = "default"

// AutoServiceTier is the OpenAI request semantics when service_tier is omitted.
// OpenAI HTTP handlers set it explicitly, without changing other providers'
// historical direct-SDK default.
const AutoServiceTier = "auto"

// Record contains the usage statistics captured for a single provider request.
type Record struct {
	// RequestID uniquely identifies this specific model execution instance (UUID v4).
	RequestID string
	// TraceID identifies the parent inbound HTTP request when available (8-character hex).
	TraceID  string
	Provider string
	// BaseURL stores the configured upstream base URL when available.
	BaseURL string
	// ExecutorType stores the concrete executor type that handled the request.
	ExecutorType    string
	Model           string
	Alias           string
	APIKey          string
	SessionID       string
	ParentSessionID string
	AuthID          string
	AuthIndex       string
	// AccessTokenSHA256 identifies the OAuth token version without exposing the token.
	AccessTokenSHA256 string
	AuthType          string
	Source            string
	// ReasoningEffort stores the translated upstream thinking level for request event logs.
	ReasoningEffort string
	// ServiceTier stores the client-requested service tier.
	ServiceTier string
	// RequestServiceTier is a deprecated input-only alias retained for existing
	// plugin callers. It is normalized into ServiceTier and never emitted.
	RequestServiceTier string
	// ResponseServiceTier stores the final tier reported by the upstream response.
	ResponseServiceTier string
	// ResponseModel stores the model name reported by the upstream response, empty when unknown.
	ResponseModel string
	// Generate reports whether the client requested actual generation.
	// nil or true means generation is enabled; only an explicit false disables generation.
	// Use GenerateFlag to set the value and GenerateEnabled to read it with the default.
	Generate *bool
	// Stream reports whether the request was executed in streaming mode.
	Stream      bool
	RequestedAt time.Time
	Latency     time.Duration
	TTFT        time.Duration
	Failed      bool
	Fail        Failure
	Detail      Detail
	// ResponseHeaders stores a snapshot of upstream response headers for usage sinks.
	ResponseHeaders http.Header
}

// Failure holds HTTP failure metadata for an upstream request attempt.
type Failure struct {
	StatusCode int
	Body       string
}

// Detail holds the token usage breakdown.
type Detail struct {
	InputTokens         int64
	OutputTokens        int64
	ReasoningTokens     int64
	CachedTokens        int64
	CacheReadTokens     int64
	CacheCreationTokens int64
	TotalTokens         int64
	TokenBreakdown      TokenBreakdown
	ResponseServiceTier string
}

type requestedModelAliasContextKey struct{}
type reasoningEffortContextKey struct{}
type serviceTierContextKey struct{}
type generateContextKey struct{}
type streamContextKey struct{}
type executionRequestIDContextKey struct{}
type executionTraceIDContextKey struct{}

// WithExecutionRequestID attaches a specific execution instance request ID to the context.
func WithExecutionRequestID(ctx context.Context, requestID string) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return context.WithValue(ctx, executionRequestIDContextKey{}, strings.TrimSpace(requestID))
}

// ExecutionRequestIDFromContext retrieves the execution instance request ID from the context.
func ExecutionRequestIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(executionRequestIDContextKey{}).(string); ok {
		return v
	}
	return ""
}

// WithTraceID attaches the parent inbound HTTP request ID to the context.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return context.WithValue(ctx, executionTraceIDContextKey{}, strings.TrimSpace(traceID))
}

// TraceIDFromContext retrieves the parent inbound HTTP request ID from the context.
func TraceIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(executionTraceIDContextKey{}).(string); ok {
		return v
	}
	return ""
}

// WithRequestedModelAlias stores the client-requested model name for usage sinks.
func WithRequestedModelAlias(ctx context.Context, alias string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	alias = strings.TrimSpace(alias)
	if alias == "" {
		return ctx
	}
	return context.WithValue(ctx, requestedModelAliasContextKey{}, alias)
}

// RequestedModelAliasFromContext returns the client-requested model name stored in ctx.
func RequestedModelAliasFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	raw := ctx.Value(requestedModelAliasContextKey{})
	switch value := raw.(type) {
	case string:
		return strings.TrimSpace(value)
	case []byte:
		return strings.TrimSpace(string(value))
	default:
		return ""
	}
}

// WithReasoningEffort stores the client-requested reasoning effort for usage sinks.
func WithReasoningEffort(ctx context.Context, effort string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	effort = strings.TrimSpace(effort)
	if effort == "" {
		return ctx
	}
	return context.WithValue(ctx, reasoningEffortContextKey{}, effort)
}

// ReasoningEffortFromContext returns the client-requested reasoning effort stored in ctx.
func ReasoningEffortFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	raw := ctx.Value(reasoningEffortContextKey{})
	switch value := raw.(type) {
	case string:
		return strings.TrimSpace(value)
	case []byte:
		return strings.TrimSpace(string(value))
	default:
		return ""
	}
}

// WithServiceTier stores the client-requested service tier for usage sinks.
func WithServiceTier(ctx context.Context, tier string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	tier = strings.TrimSpace(tier)
	if tier == "" {
		tier = DefaultServiceTier
	}
	return context.WithValue(ctx, serviceTierContextKey{}, tier)
}

// ServiceTierFromContext returns the client-requested service tier stored in ctx.
func ServiceTierFromContext(ctx context.Context) string {
	if ctx == nil {
		return DefaultServiceTier
	}
	raw := ctx.Value(serviceTierContextKey{})
	switch value := raw.(type) {
	case string:
		tier := strings.TrimSpace(value)
		if tier == "" {
			return DefaultServiceTier
		}
		return tier
	case []byte:
		tier := strings.TrimSpace(string(value))
		if tier == "" {
			return DefaultServiceTier
		}
		return tier
	default:
		return DefaultServiceTier
	}
}

// WithGenerate stores whether the client requested actual generation for usage sinks.
// Missing context values default to true; only an explicit false disables generation.
func WithGenerate(ctx context.Context, generate bool) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, generateContextKey{}, generate)
}

// GenerateFromContext returns whether the client requested actual generation.
// Missing values default to true.
func GenerateFromContext(ctx context.Context) bool {
	if ctx == nil {
		return true
	}
	raw := ctx.Value(generateContextKey{})
	switch value := raw.(type) {
	case bool:
		return value
	default:
		return true
	}
}

// WithStream stores whether the request was executed in streaming mode for usage sinks.
func WithStream(ctx context.Context, stream bool) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, streamContextKey{}, stream)
}

// StreamFromContext returns whether the request was executed in streaming mode.
// Missing values default to false.
func StreamFromContext(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	raw := ctx.Value(streamContextKey{})
	switch value := raw.(type) {
	case bool:
		return value
	default:
		return false
	}
}

// GenerateFlag returns a pointer suitable for Record.Generate.
func GenerateFlag(generate bool) *bool {
	return &generate
}

// GenerateEnabled reports whether generation is enabled for the record field.
// A nil value defaults to true so legacy callers that omit Generate keep the historical behavior.
func GenerateEnabled(generate *bool) bool {
	if generate == nil {
		return true
	}
	return *generate
}

// Plugin consumes usage records emitted by the proxy runtime.
type Plugin interface {
	HandleUsage(ctx context.Context, record Record)
}

type queueItem struct {
	ctx    context.Context
	record Record
	bytes  int
}

const defaultQueueMaxBytes = 16 << 20

// ManagerStats reports the bounded in-memory usage dispatch queue state.
type ManagerStats struct {
	QueuedRecords     int    `json:"queued_records"`
	QueuedBytes       int    `json:"queued_bytes"`
	MaxRecords        int    `json:"max_records"`
	MaxBytes          int    `json:"max_bytes"`
	PeakRecords       int    `json:"peak_records"`
	PeakBytes         int    `json:"peak_bytes"`
	BlockedPublishers int    `json:"blocked_publishers"`
	BackpressureTotal uint64 `json:"backpressure_total"`
}

// Manager maintains a queue of usage records and delivers them to registered plugins.
type Manager struct {
	once     sync.Once
	stopOnce sync.Once
	cancel   context.CancelFunc

	mu     sync.Mutex
	cond   *sync.Cond
	space  *sync.Cond
	queue  []queueItem
	head   int
	closed bool

	maxRecords        int
	maxBytes          int
	queuedBytes       int
	peakRecords       int
	peakBytes         int
	blockedPublishers int
	backpressureTotal uint64

	pluginsMu sync.RWMutex
	plugins   []Plugin
	named     map[string]int
}

// NewManager constructs a manager with a buffered queue.
func NewManager(buffer int) *Manager {
	return NewManagerWithLimits(buffer, defaultQueueMaxBytes)
}

// NewManagerWithLimits constructs a manager with record and byte bounds.
// A non-positive record limit is normalized to one so direct SDK callers keep
// asynchronous delivery without creating an unbounded queue.
func NewManagerWithLimits(maxRecords, maxBytes int) *Manager {
	if maxRecords <= 0 {
		maxRecords = 1
	}
	if maxBytes <= 0 {
		maxBytes = defaultQueueMaxBytes
	}
	m := &Manager{
		maxRecords: maxRecords,
		maxBytes:   maxBytes,
	}
	m.cond = sync.NewCond(&m.mu)
	m.space = sync.NewCond(&m.mu)
	return m
}

// Start launches the background dispatcher. Calling Start multiple times is safe.
func (m *Manager) Start(ctx context.Context) {
	if m == nil {
		return
	}
	m.once.Do(func() {
		if ctx == nil {
			ctx = context.Background()
		}
		var workerCtx context.Context
		workerCtx, m.cancel = context.WithCancel(ctx)
		go m.run(workerCtx)
	})
}

// Stop stops the dispatcher and drains the queue.
func (m *Manager) Stop() {
	if m == nil {
		return
	}
	m.stopOnce.Do(func() {
		if m.cancel != nil {
			m.cancel()
		}
		m.mu.Lock()
		m.closed = true
		m.mu.Unlock()
		m.cond.Broadcast()
		m.space.Broadcast()
	})
}

// Register appends a plugin to the delivery list.
func (m *Manager) Register(plugin Plugin) {
	if m == nil || plugin == nil {
		return
	}
	m.pluginsMu.Lock()
	m.plugins = append(m.plugins, plugin)
	m.pluginsMu.Unlock()
}

// RegisterNamed registers or replaces a plugin by name.
func (m *Manager) RegisterNamed(name string, plugin Plugin) {
	if m == nil || plugin == nil {
		return
	}
	name = strings.TrimSpace(name)
	if name == "" {
		return
	}

	m.pluginsMu.Lock()
	if m.named == nil {
		m.named = make(map[string]int)
	}
	if index, exists := m.named[name]; exists && index >= 0 && index < len(m.plugins) {
		m.plugins[index] = plugin
		m.pluginsMu.Unlock()
		return
	}
	m.named[name] = len(m.plugins)
	m.plugins = append(m.plugins, plugin)
	m.pluginsMu.Unlock()
}

// Publish enqueues a usage record for processing. If no plugin is registered
// the record will be discarded downstream.
func (m *Manager) Publish(ctx context.Context, record Record) {
	if m == nil {
		return
	}
	if strings.TrimSpace(record.RequestID) == "" {
		if reqID := ExecutionRequestIDFromContext(ctx); reqID != "" {
			record.RequestID = reqID
		} else {
			record.RequestID = uuid.NewString()
		}
	}
	if strings.TrimSpace(record.TraceID) == "" {
		if trID := TraceIDFromContext(ctx); trID != "" {
			record.TraceID = trID
		} else if trID := internallogging.GetRequestID(ctx); trID != "" {
			record.TraceID = trID
		}
	}
	// ensure worker is running even if Start was not called explicitly
	m.Start(context.Background())
	itemBytes := estimateRecordBytes(record)
	m.mu.Lock()
	blocked := false
	for !m.closed && !m.canEnqueueLocked(itemBytes) {
		if !blocked {
			blocked = true
			m.blockedPublishers++
			m.backpressureTotal++
		}
		m.space.Wait()
	}
	if blocked {
		m.blockedPublishers--
	}
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.compactQueueForAppendLocked()
	m.queue = append(m.queue, queueItem{ctx: ctx, record: record, bytes: itemBytes})
	m.queuedBytes += itemBytes
	queuedRecords := len(m.queue) - m.head
	if queuedRecords > m.peakRecords {
		m.peakRecords = queuedRecords
	}
	if m.queuedBytes > m.peakBytes {
		m.peakBytes = m.queuedBytes
	}
	m.mu.Unlock()
	m.cond.Signal()
}

func (m *Manager) canEnqueueLocked(itemBytes int) bool {
	queuedRecords := len(m.queue) - m.head
	if queuedRecords >= m.maxRecords {
		return false
	}
	if m.queuedBytes+itemBytes <= m.maxBytes {
		return true
	}
	// Preserve one unusually large complete record instead of dropping or
	// truncating it. All additional publishers still receive backpressure.
	return queuedRecords == 0
}

// Stats returns a consistent snapshot without consuming queued records.
func (m *Manager) Stats() ManagerStats {
	if m == nil {
		return ManagerStats{}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return ManagerStats{
		QueuedRecords:     len(m.queue) - m.head,
		QueuedBytes:       m.queuedBytes,
		MaxRecords:        m.maxRecords,
		MaxBytes:          m.maxBytes,
		PeakRecords:       m.peakRecords,
		PeakBytes:         m.peakBytes,
		BlockedPublishers: m.blockedPublishers,
		BackpressureTotal: m.backpressureTotal,
	}
}

func (m *Manager) run(ctx context.Context) {
	for {
		m.mu.Lock()
		for !m.closed && m.head >= len(m.queue) {
			m.cond.Wait()
		}
		if m.head >= len(m.queue) && m.closed {
			m.mu.Unlock()
			return
		}
		item := m.queue[m.head]
		m.queue[m.head] = queueItem{}
		m.head++
		if m.head >= len(m.queue) {
			m.queue = m.queue[:0]
			m.head = 0
		}
		m.queuedBytes -= item.bytes
		if m.queuedBytes < 0 {
			m.queuedBytes = 0
		}
		m.mu.Unlock()
		m.space.Broadcast()
		m.dispatch(item)
	}
}

func (m *Manager) compactQueueForAppendLocked() {
	if m.head == 0 || len(m.queue) < cap(m.queue) {
		return
	}
	active := copy(m.queue, m.queue[m.head:])
	clear(m.queue[active:])
	m.queue = m.queue[:active]
	m.head = 0
}

func estimateRecordBytes(record Record) int {
	size := len(record.Provider) + len(record.ExecutorType) + len(record.Model) +
		len(record.Alias) + len(record.APIKey) + len(record.AuthID) +
		len(record.AuthIndex) + len(record.AccessTokenSHA256) + len(record.AuthType) +
		len(record.Source) + len(record.ReasoningEffort) + len(record.ServiceTier) +
		len(record.RequestServiceTier) + len(record.ResponseServiceTier) +
		len(record.Fail.Body)
	for name, values := range record.ResponseHeaders {
		size += len(name)
		for _, value := range values {
			size += len(value)
		}
	}
	if size <= 0 {
		return 1
	}
	return size
}

func (m *Manager) dispatch(item queueItem) {
	m.pluginsMu.RLock()
	plugins := make([]Plugin, len(m.plugins))
	copy(plugins, m.plugins)
	m.pluginsMu.RUnlock()
	if len(plugins) == 0 {
		return
	}
	for _, plugin := range plugins {
		if plugin == nil {
			continue
		}
		safeInvoke(plugin, item.ctx, item.record)
	}
}

func safeInvoke(plugin Plugin, ctx context.Context, record Record) {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("usage: plugin panic recovered: %v", r)
		}
	}()
	plugin.HandleUsage(ctx, record)
}

var defaultManager = NewManager(512)

// DefaultManager returns the global usage manager instance.
func DefaultManager() *Manager { return defaultManager }

// RegisterPlugin registers a plugin on the default manager.
func RegisterPlugin(plugin Plugin) { DefaultManager().Register(plugin) }

// RegisterNamedPlugin registers or replaces a named plugin on the default manager.
func RegisterNamedPlugin(name string, plugin Plugin) { DefaultManager().RegisterNamed(name, plugin) }

// PublishRecord publishes a record using the default manager.
func PublishRecord(ctx context.Context, record Record) { DefaultManager().Publish(ctx, record) }

// StartDefault starts the default manager's dispatcher.
func StartDefault(ctx context.Context) { DefaultManager().Start(ctx) }

// StopDefault stops the default manager's dispatcher.
func StopDefault() { DefaultManager().Stop() }
