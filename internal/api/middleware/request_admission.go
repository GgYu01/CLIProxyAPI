package middleware

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"bufio"
	"io"
	"math"
	"net/http"
	"net"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
)

const requestAdmissionModelPeekBytes = 64 << 10

var (
	errAdmissionFull            = errors.New("request admission queue is full")
	errAdmissionTimeout         = errors.New("request admission wait timed out")
	errRequestBodyTooLarge      = errors.New("request body exceeds configured limit")
	errUpstreamFirstByteTimeout = errors.New("upstream produced no response bytes before the configured deadline")
	requestModelPattern         = regexp.MustCompile(`(?i)"model"\s*:\s*"([^"\\]{1,200})"`)
	requestImageToolPattern     = regexp.MustCompile(`(?i)"(?:type|name)"\s*:\s*"image_generation"`)
)

// RequestAdmissionConfig configures the in-process replacement for the Python
// cpa-queue service. Wait limits apply only before acceptance; active 600/900
// second requests are governed by their client context, not by these timers.
type RequestAdmissionConfig struct {
	Enabled                 bool
	MaxInFlight             int
	MaxWaiting              int
	WaitTimeout             time.Duration
	ImageMaxInFlight        int
	ImageMaxWaiting         int
	ImageWaitTimeout        time.Duration
	MaxBodyBytes            int64
	BodyBudgetBytes         int64
	BodyBudgetWaitTimeout   time.Duration
	LargeBodyThresholdBytes int64
	LargeBodyMaxInFlight    int
	LargeBodyWaitTimeout    time.Duration
	ModelRPM                map[string]int
	ModelRPMWindow          time.Duration
	ModelRPMMaxWaiting      int
	FirstByteTimeout        time.Duration
	FirstByteRetryAfter     time.Duration
}

// DefaultRequestAdmissionConfig returns the production compatibility values.
func DefaultRequestAdmissionConfig() RequestAdmissionConfig {
	return RequestAdmissionConfig{
		Enabled:                 false,
		MaxInFlight:             -1,
		MaxWaiting:              0,
		WaitTimeout:             180 * time.Second,
		ImageMaxInFlight:        30,
		ImageMaxWaiting:         10,
		ImageWaitTimeout:        180 * time.Second,
		MaxBodyBytes:            256 << 20,
		BodyBudgetBytes:         512 << 20,
		BodyBudgetWaitTimeout:   30 * time.Second,
		LargeBodyThresholdBytes: 8 << 20,
		LargeBodyMaxInFlight:    16,
		LargeBodyWaitTimeout:    30 * time.Second,
		ModelRPM:                map[string]int{},
		ModelRPMWindow:          time.Minute,
		ModelRPMMaxWaiting:      8,
		FirstByteTimeout:        600 * time.Second,
		FirstByteRetryAfter:     5 * time.Second,
	}
}

func normalizeRequestAdmissionConfig(cfg RequestAdmissionConfig) RequestAdmissionConfig {
	defaults := DefaultRequestAdmissionConfig()
	if cfg.MaxInFlight == 0 {
		cfg.MaxInFlight = defaults.MaxInFlight
	}
	if cfg.MaxInFlight < 0 {
		cfg.MaxInFlight = -1
	}
	if cfg.MaxWaiting == 0 {
		cfg.MaxWaiting = defaults.MaxWaiting
	}
	if cfg.MaxWaiting < 0 {
		cfg.MaxWaiting = 0
	}
	if cfg.WaitTimeout <= 0 {
		cfg.WaitTimeout = defaults.WaitTimeout
	}
	if cfg.ImageMaxInFlight == 0 {
		cfg.ImageMaxInFlight = defaults.ImageMaxInFlight
	}
	if cfg.ImageMaxInFlight < 0 {
		cfg.ImageMaxInFlight = -1
	}
	if cfg.ImageMaxWaiting == 0 {
		cfg.ImageMaxWaiting = defaults.ImageMaxWaiting
	}
	if cfg.ImageMaxWaiting < 0 {
		cfg.ImageMaxWaiting = 0
	}
	if cfg.ImageWaitTimeout <= 0 {
		cfg.ImageWaitTimeout = defaults.ImageWaitTimeout
	}
	if cfg.MaxBodyBytes <= 0 {
		cfg.MaxBodyBytes = defaults.MaxBodyBytes
	}
	if cfg.BodyBudgetBytes <= 0 {
		cfg.BodyBudgetBytes = defaults.BodyBudgetBytes
	}
	if cfg.BodyBudgetWaitTimeout <= 0 {
		cfg.BodyBudgetWaitTimeout = defaults.BodyBudgetWaitTimeout
	}
	if cfg.LargeBodyThresholdBytes <= 0 {
		cfg.LargeBodyThresholdBytes = defaults.LargeBodyThresholdBytes
	}
	if cfg.LargeBodyMaxInFlight == 0 {
		cfg.LargeBodyMaxInFlight = defaults.LargeBodyMaxInFlight
	}
	if cfg.LargeBodyMaxInFlight < 0 {
		cfg.LargeBodyMaxInFlight = -1
	}
	if cfg.LargeBodyWaitTimeout <= 0 {
		cfg.LargeBodyWaitTimeout = defaults.LargeBodyWaitTimeout
	}
	if cfg.ModelRPM == nil {
		cfg.ModelRPM = defaults.ModelRPM
	}
	if cfg.ModelRPMWindow <= 0 {
		cfg.ModelRPMWindow = defaults.ModelRPMWindow
	}
	if cfg.ModelRPMMaxWaiting <= 0 {
		cfg.ModelRPMMaxWaiting = defaults.ModelRPMMaxWaiting
	}
	if cfg.FirstByteTimeout <= 0 {
		cfg.FirstByteTimeout = defaults.FirstByteTimeout
	}
	if cfg.FirstByteRetryAfter <= 0 {
		cfg.FirstByteRetryAfter = defaults.FirstByteRetryAfter
	}
	return cfg
}

// RequestAdmission owns bounded semaphores and counters inside CPA.
type RequestAdmission struct {
	cfg        RequestAdmissionConfig
	textLane   *requestAdmissionLane
	imageLane  *requestAdmissionLane
	largeLane  *requestAdmissionLane
	bodyBudget *requestBodyBudget
	rpm        map[string]*requestRPMLimiter

	accepted         atomic.Int64
	rejected         atomic.Int64
	canceled         atomic.Int64
	bodyTooLarge     atomic.Int64
	rpmRejected      atomic.Int64
	firstByteTimeout atomic.Int64
}

type requestAdmissionLane struct {
	tokens     chan struct{}
	maxWaiting int64
	wait       time.Duration
	waiting    atomic.Int64
	inFlight   atomic.Int64
	peak       atomic.Int64
}

type requestRPMLimiter struct {
	mu      sync.Mutex
	hits    []time.Time
	waiting atomic.Int64
	limit   int
}

type requestBodyBudget struct {
	limit      int64
	maxWaiting int64
	wait       time.Duration
	waiting    atomic.Int64

	mu     sync.Mutex
	used   int64
	peak   int64
	notify chan struct{}
}

// AdmissionStats is a read-only snapshot compatible with the old queue
// monitoring surface while exposing native large-body and RPM state.
type AdmissionStats struct {
	Enabled                  bool             `json:"enabled"`
	Accepted                 int64            `json:"accepted"`
	Rejected                 int64            `json:"rejected"`
	Canceled                 int64            `json:"canceled"`
	BodyTooLarge             int64            `json:"body_too_large"`
	RPMRejected              int64            `json:"rpm_rejected"`
	InFlight                 int64            `json:"inflight"`
	Waiting                  int64            `json:"waiting"`
	InFlightPeak             int64            `json:"inflight_peak"`
	ImageInFlight            int64            `json:"image_inflight"`
	ImageWaiting             int64            `json:"image_waiting"`
	ImageInFlightPeak        int64            `json:"image_inflight_peak"`
	LargeBodyInFlight        int64            `json:"large_body_inflight"`
	LargeBodyWaiting         int64            `json:"large_body_waiting"`
	LargeBodyInFlightPeak    int64            `json:"large_body_inflight_peak"`
	BodyBytesInFlight        int64            `json:"body_bytes_inflight"`
	BodyBytesWaiting         int64            `json:"body_bytes_waiting"`
	BodyBytesPeak            int64            `json:"body_bytes_peak"`
	ModelRPMWaiting          map[string]int64 `json:"model_rpm_waiting"`
	UpstreamFirstByteTimeout int64            `json:"upstream_first_byte_timeout"`
}

func NewRequestAdmission(cfg RequestAdmissionConfig) *RequestAdmission {
	enabled := cfg.Enabled
	if !enabled {
		return &RequestAdmission{
			cfg: RequestAdmissionConfig{Enabled: false},
		}
	}
	cfg = normalizeRequestAdmissionConfig(cfg)
	cfg.Enabled = enabled
	a := &RequestAdmission{
		cfg:        cfg,
		textLane:   newRequestAdmissionLane(cfg.MaxInFlight, cfg.MaxWaiting, cfg.WaitTimeout),
		imageLane:  newRequestAdmissionLane(cfg.ImageMaxInFlight, cfg.ImageMaxWaiting, cfg.ImageWaitTimeout),
		largeLane:  newRequestAdmissionLane(cfg.LargeBodyMaxInFlight, cfg.MaxWaiting, cfg.LargeBodyWaitTimeout),
		bodyBudget: newRequestBodyBudget(cfg.BodyBudgetBytes, cfg.MaxWaiting, cfg.BodyBudgetWaitTimeout),
		rpm:        make(map[string]*requestRPMLimiter),
	}
	for model, limit := range cfg.ModelRPM {
		model = normalizeAdmissionModel(model)
		if model != "" && limit > 0 {
			a.rpm[model] = &requestRPMLimiter{limit: limit}
		}
	}
	return a
}

func newRequestAdmissionLane(maxInFlight, maxWaiting int, wait time.Duration) *requestAdmissionLane {
	if maxInFlight < 0 {
		return nil
	}
	return &requestAdmissionLane{
		tokens:     func() chan struct{} { if maxInFlight < 0 { return nil }; return make(chan struct{}, maxInFlight) }(),
		maxWaiting: int64(maxWaiting),
		wait:       wait,
	}
}

func newRequestBodyBudget(limit int64, maxWaiting int, wait time.Duration) *requestBodyBudget {
	return &requestBodyBudget{
		limit:      limit,
		maxWaiting: int64(maxWaiting),
		wait:       wait,
		notify:     make(chan struct{}),
	}
}

func (b *requestBodyBudget) tryReserve(amount int64) (bool, <-chan struct{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.used+amount <= b.limit {
		b.used += amount
		if b.used > b.peak {
			b.peak = b.used
		}
		return true, nil
	}
	return false, b.notify
}

func (b *requestBodyBudget) acquire(ctx context.Context, amount int64) (func(), error) {
	if b == nil || amount <= 0 {
		return func() {}, nil
	}
	if amount > b.limit {
		return nil, errAdmissionFull
	}
	reserved, notify := b.tryReserve(amount)
	if reserved {
		return b.releaseFunc(amount), nil
	}
	if !reserveBoundedCounter(&b.waiting, b.maxWaiting) {
		return nil, errAdmissionFull
	}
	defer b.waiting.Add(-1)
	timer := time.NewTimer(b.wait)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			return nil, errAdmissionTimeout
		case <-notify:
		}
		reserved, notify = b.tryReserve(amount)
		if reserved {
			return b.releaseFunc(amount), nil
		}
	}
}

func (b *requestBodyBudget) releaseFunc(amount int64) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			b.mu.Lock()
			b.used -= amount
			if b.used < 0 {
				b.used = 0
			}
			close(b.notify)
			b.notify = make(chan struct{})
			b.mu.Unlock()
		})
	}
}

func (b *requestBodyBudget) snapshot() (used, waiting, peak int64) {
	if b == nil {
		return 0, 0, 0
	}
	b.mu.Lock()
	used, peak = b.used, b.peak
	b.mu.Unlock()
	return used, b.waiting.Load(), peak
}

func (a *RequestAdmission) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if a == nil || !a.cfg.Enabled || !isRequestAdmissionPath(c.Request) {
			c.Next()
			return
		}
		ctx := c.Request.Context()
		if c.Request.ContentLength > a.cfg.MaxBodyBytes {
			a.bodyTooLarge.Add(1)
			a.reject(c, http.StatusRequestEntityTooLarge, "body_too_large", fmt.Sprintf("request body exceeds %d bytes", a.cfg.MaxBodyBytes), 0)
			return
		}

		body := &requestAdmissionBody{
			ReadCloser: c.Request.Body,
			ctx:        ctx,
			maxBytes:   a.cfg.MaxBodyBytes,
			threshold:  a.cfg.LargeBodyThresholdBytes,
			largeLane:  a.largeLane,
			onTooLarge: func() { a.bodyTooLarge.Add(1) },
		}
		if c.Request.Body != nil {
			c.Request.Body = body
		}
		defer body.releaseLarge()
		defer body.releaseBudget()
		if c.Request.ContentLength > a.cfg.LargeBodyThresholdBytes {
			releaseLarge, err := a.largeLane.acquire(ctx)
			if err != nil {
				a.handleLaneError(c, err, "large_body_queue", a.cfg.LargeBodyWaitTimeout)
				return
			}
			body.setLargeRelease(releaseLarge)
		}

		imageRequest := isAdmissionImagePath(c.Request.URL.Path)
		model := ""
		if !imageRequest && c.Request.Body != nil && requestCanHaveBody(c.Request.Method) {
			prefix, peekErr := peekAdmissionRequestBody(c.Request)
			if peekErr != nil {
				if errors.Is(peekErr, errRequestBodyTooLarge) {
					a.reject(c, http.StatusRequestEntityTooLarge, "body_too_large", fmt.Sprintf("request body exceeds %d bytes", a.cfg.MaxBodyBytes), 0)
					return
				}
				if errors.Is(peekErr, errAdmissionFull) || errors.Is(peekErr, errAdmissionTimeout) || errors.Is(peekErr, context.Canceled) || errors.Is(peekErr, context.DeadlineExceeded) {
					a.handleLaneError(c, peekErr, "large_body_queue", a.cfg.LargeBodyWaitTimeout)
					return
				}
				a.reject(c, http.StatusBadRequest, "invalid_request_body", peekErr.Error(), 0)
				return
			}
			model = admissionModelFromPrefix(prefix)
			imageRequest = requestImageToolPattern.Match(prefix)
		}
		if limiter, key := a.modelRPMLimiter(model); limiter != nil && !imageRequest {
			if err := limiter.acquire(ctx, a.cfg.ModelRPMWindow, a.cfg.ModelRPMMaxWaiting); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					a.canceled.Add(1)
					c.Abort()
					return
				}
				a.rpmRejected.Add(1)
				a.reject(c, http.StatusTooManyRequests, "model_rpm_full", fmt.Sprintf("%s is limited to %d requests per %s", key, limiter.limit, a.cfg.ModelRPMWindow), a.cfg.ModelRPMWindow)
				return
			}
		}

	if c.Request.ContentLength > 0 || c.Request.ContentLength < 0 {
			reserveBytes := c.Request.ContentLength
			if reserveBytes < 0 {
				reserveBytes = a.cfg.MaxBodyBytes
			}
			releaseBudget, err := a.bodyBudget.acquire(ctx, reserveBytes)
			if err != nil {
				a.handleLaneError(c, err, "body_budget", a.cfg.BodyBudgetWaitTimeout)
				return
			}
			body.setBudgetRelease(releaseBudget)
		}

		lane := a.textLane
		code := "queue"
		wait := a.cfg.WaitTimeout
		if imageRequest {
			lane = a.imageLane
			code = "image_queue"
			wait = a.cfg.ImageWaitTimeout
		}
		release, err := lane.acquire(ctx)
		if err != nil {
			a.handleLaneError(c, err, code, wait)
			return
		}
		defer release()
		a.accepted.Add(1)
		a.serveAccepted(c)
	}
}

const (
	firstBytePending int32 = iota
	firstByteObserved
	firstByteTimedOut
)

type firstBytePolicyWriter struct {
	gin.ResponseWriter
	state  atomic.Int32
	timer  *time.Timer
	cancel context.CancelCauseFunc
}

func (w *firstBytePolicyWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if !w.markFirstByte() {
		return nil, nil, errUpstreamFirstByteTimeout
	}
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("response writer does not support hijacking")
	}
	return hijacker.Hijack()
}

func (w *firstBytePolicyWriter) markFirstByte() bool {
	if w == nil {
		return false
	}
	for {
		switch state := w.state.Load(); state {
		case firstByteObserved:
			return true
		case firstByteTimedOut:
			return false
		default:
			if w.state.CompareAndSwap(firstBytePending, firstByteObserved) {
				if w.timer != nil {
					w.timer.Stop()
				}
				return true
			}
		}
	}
}

func (w *firstBytePolicyWriter) triggerTimeout() {
	if w == nil || !w.state.CompareAndSwap(firstBytePending, firstByteTimedOut) {
		return
	}
	if w.cancel != nil {
		w.cancel(errUpstreamFirstByteTimeout)
	}
}

func (w *firstBytePolicyWriter) WriteHeader(code int) {
	if w == nil || w.state.Load() == firstByteTimedOut {
		return
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *firstBytePolicyWriter) WriteHeaderNow() {
	if w.markFirstByte() {
		w.ResponseWriter.WriteHeaderNow()
	}
}

func (w *firstBytePolicyWriter) Write(payload []byte) (int, error) {
	if !w.markFirstByte() {
		return len(payload), nil
	}
	return w.ResponseWriter.Write(payload)
}

func (w *firstBytePolicyWriter) WriteString(payload string) (int, error) {
	if !w.markFirstByte() {
		return len(payload), nil
	}
	return w.ResponseWriter.WriteString(payload)
}

func (w *firstBytePolicyWriter) Flush() {
	if w.markFirstByte() {
		w.ResponseWriter.Flush()
	}
}

func (a *RequestAdmission) serveAccepted(c *gin.Context) {
	if a == nil || c == nil || c.Request == nil || a.cfg.FirstByteTimeout <= 0 {
		c.Next()
		return
	}
	originalWriter := c.Writer
	requestContext, cancel := context.WithCancelCause(c.Request.Context())
	defer cancel(nil)
	policyWriter := &firstBytePolicyWriter{ResponseWriter: originalWriter, cancel: cancel}
	policyWriter.timer = time.AfterFunc(a.cfg.FirstByteTimeout, policyWriter.triggerTimeout)
	c.Writer = policyWriter
	c.Request = c.Request.WithContext(requestContext)
	c.Next()
	policyWriter.timer.Stop()
	c.Writer = originalWriter

	if policyWriter.state.Load() != firstByteTimedOut || originalWriter.Written() {
		return
	}
	a.firstByteTimeout.Add(1)
	a.rejected.Add(1)
	header := c.Writer.Header()
	header.Del("Content-Length")
	header.Del("Transfer-Encoding")
	header.Set("Content-Type", "application/json; charset=utf-8")
	a.reject(c, http.StatusGatewayTimeout, "upstream_first_byte_timeout", fmt.Sprintf("upstream produced no response bytes for %.0fs", a.cfg.FirstByteTimeout.Seconds()), a.cfg.FirstByteRetryAfter)
}

func (a *RequestAdmission) handleLaneError(c *gin.Context, err error, code string, retry time.Duration) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		a.canceled.Add(1)
		c.Abort()
		return
	}
	a.rejected.Add(1)
	suffix := "full"
	if errors.Is(err, errAdmissionTimeout) {
		suffix = "timeout"
	}
	a.reject(c, http.StatusServiceUnavailable, code+"_"+suffix, "CPA request admission capacity is temporarily unavailable", retry)
}

func (a *RequestAdmission) reject(c *gin.Context, status int, code string, message string, retry time.Duration) {
	if retry > 0 {
		seconds := int(math.Ceil(retry.Seconds()))
		if seconds < 1 {
			seconds = 1
		}
		c.Header("Retry-After", strconv.Itoa(seconds))
	}
	c.AbortWithStatusJSON(status, gin.H{"error": gin.H{"type": code, "message": message}})
}

func (a *RequestAdmission) Stats() AdmissionStats {
	stats := AdmissionStats{Enabled: a != nil && a.cfg.Enabled, ModelRPMWaiting: make(map[string]int64)}
	if a == nil {
		return stats
	}
	stats.Accepted = a.accepted.Load()
	stats.Rejected = a.rejected.Load()
	stats.Canceled = a.canceled.Load()
	stats.BodyTooLarge = a.bodyTooLarge.Load()
	stats.RPMRejected = a.rpmRejected.Load()
	stats.UpstreamFirstByteTimeout = a.firstByteTimeout.Load()
	stats.InFlight, stats.Waiting, stats.InFlightPeak = a.textLane.snapshot()
	stats.ImageInFlight, stats.ImageWaiting, stats.ImageInFlightPeak = a.imageLane.snapshot()
	stats.LargeBodyInFlight, stats.LargeBodyWaiting, stats.LargeBodyInFlightPeak = a.largeLane.snapshot()
	stats.BodyBytesInFlight, stats.BodyBytesWaiting, stats.BodyBytesPeak = a.bodyBudget.snapshot()
	keys := make([]string, 0, len(a.rpm))
	for key := range a.rpm {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		stats.ModelRPMWaiting[key] = a.rpm[key].waiting.Load()
	}
	return stats
}

func (l *requestAdmissionLane) acquire(ctx context.Context) (func(), error) {
	if l == nil {
		return func() {}, nil
	}
	if l.tokens == nil {
		l.onAcquire()
		return func() { l.inFlight.Add(-1) }, nil
	}
	select {
	case l.tokens <- struct{}{}:
		l.onAcquire()
		return l.releaseFunc(), nil
	default:
	}
	if !reserveBoundedCounter(&l.waiting, l.maxWaiting) {
		return nil, errAdmissionFull
	}
	defer l.waiting.Add(-1)
	timer := time.NewTimer(l.wait)
	defer timer.Stop()
	select {
	case l.tokens <- struct{}{}:
		l.onAcquire()
		return l.releaseFunc(), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, errAdmissionTimeout
	}
}

func (l *requestAdmissionLane) onAcquire() {
	current := l.inFlight.Add(1)
	for {
		peak := l.peak.Load()
		if current <= peak || l.peak.CompareAndSwap(peak, current) {
			return
		}
	}
}

func (l *requestAdmissionLane) releaseFunc() func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			<-l.tokens
			l.inFlight.Add(-1)
		})
	}
}

func (l *requestAdmissionLane) snapshot() (inFlight, waiting, peak int64) {
	if l == nil {
		return 0, 0, 0
	}
	return l.inFlight.Load(), l.waiting.Load(), l.peak.Load()
}

func reserveBoundedCounter(counter *atomic.Int64, maximum int64) bool {
	for {
		current := counter.Load()
		if current >= maximum {
			return false
		}
		if counter.CompareAndSwap(current, current+1) {
			return true
		}
	}
}

func (a *RequestAdmission) modelRPMLimiter(model string) (*requestRPMLimiter, string) {
	model = normalizeAdmissionModel(model)
	if model == "" {
		return nil, ""
	}
	if limiter := a.rpm[model]; limiter != nil {
		return limiter, model
	}
	for key, limiter := range a.rpm {
		if strings.HasSuffix(model, key) || strings.HasSuffix(key, model) {
			return limiter, key
		}
	}
	return nil, ""
}

func (r *requestRPMLimiter) acquire(ctx context.Context, window time.Duration, maxWaiting int) error {
	reserved := false
	defer func() {
		if reserved {
			r.waiting.Add(-1)
		}
	}()
	for {
		now := time.Now()
		r.mu.Lock()
		cutoff := now.Add(-window)
		first := 0
		for first < len(r.hits) && !r.hits[first].After(cutoff) {
			first++
		}
		if first > 0 {
			copy(r.hits, r.hits[first:])
			r.hits = r.hits[:len(r.hits)-first]
		}
		if len(r.hits) < r.limit {
			r.hits = append(r.hits, now)
			r.mu.Unlock()
			return nil
		}
		wait := time.Until(r.hits[0].Add(window))
		r.mu.Unlock()
		if !reserved {
			if !reserveBoundedCounter(&r.waiting, int64(maxWaiting)) {
				return errAdmissionFull
			}
			reserved = true
		}
		if wait < time.Millisecond {
			wait = time.Millisecond
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}

type replayAdmissionBody struct {
	io.Reader
	io.Closer
}

func peekAdmissionRequestBody(request *http.Request) ([]byte, error) {
	if request == nil || request.Body == nil {
		return nil, nil
	}
	original := request.Body
	prefix, err := io.ReadAll(io.LimitReader(original, requestAdmissionModelPeekBytes))
	if err != nil {
		_ = original.Close()
		return nil, fmt.Errorf("read request prefix: %w", err)
	}
	request.Body = &replayAdmissionBody{Reader: io.MultiReader(bytes.NewReader(prefix), original), Closer: original}
	return prefix, nil
}

func admissionModelFromPrefix(prefix []byte) string {
	match := requestModelPattern.FindSubmatch(prefix)
	if len(match) != 2 {
		return ""
	}
	return normalizeAdmissionModel(string(match[1]))
}

func normalizeAdmissionModel(model string) string {
	model = strings.ToLower(strings.TrimSpace(model))
	if slash := strings.LastIndexByte(model, '/'); slash >= 0 {
		model = model[slash+1:]
	}
	return model
}

func isRequestAdmissionPath(request *http.Request) bool {
	if request == nil || request.URL == nil {
		return false
	}
	path := strings.ToLower(request.URL.Path)
	if !strings.HasPrefix(path, "/v1/") && !strings.HasPrefix(path, "/v1beta/") &&
		!strings.HasPrefix(path, "/backend-api/codex/") && !strings.HasPrefix(path, "/openai/v1/") {
		return false
	}
	if request.Method == http.MethodGet && !strings.EqualFold(request.Header.Get("Upgrade"), "websocket") {
		return false
	}
	return request.Method != http.MethodHead && request.Method != http.MethodOptions
}

func requestCanHaveBody(method string) bool {
	return method != http.MethodGet && method != http.MethodHead
}

func isAdmissionImagePath(path string) bool {
	path = strings.ToLower(strings.SplitN(path, "?", 2)[0])
	return strings.Contains(path, "/images/") || strings.HasSuffix(path, "/images")
}

type requestAdmissionBody struct {
	io.ReadCloser
	ctx        context.Context
	maxBytes   int64
	threshold  int64
	largeLane  *requestAdmissionLane
	readBytes  int64
	onTooLarge func()
	tooLarge   sync.Once

	mu            sync.Mutex
	largeRelease  func()
	budgetRelease func()
}

func (b *requestAdmissionBody) Read(p []byte) (int, error) {
	if b == nil || b.ReadCloser == nil {
		return 0, io.EOF
	}
	n, err := b.ReadCloser.Read(p)
	if n <= 0 {
		return n, err
	}
	next := b.readBytes + int64(n)
	if b.maxBytes > 0 && next > b.maxBytes {
		b.readBytes = next
		b.tooLarge.Do(func() {
			if b.onTooLarge != nil {
				b.onTooLarge()
			}
		})
		return 0, errRequestBodyTooLarge
	}
	if b.largeLane != nil && b.threshold > 0 && b.readBytes <= b.threshold && next > b.threshold {
		b.mu.Lock()
		alreadyAcquired := b.largeRelease != nil
		b.mu.Unlock()
		if !alreadyAcquired {
			release, acquireErr := b.largeLane.acquire(b.ctx)
			if acquireErr != nil {
				return 0, acquireErr
			}
			b.setLargeRelease(release)
		}
	}
	b.readBytes = next
	return n, err
}

func (b *requestAdmissionBody) Close() error {
	if b == nil {
		return nil
	}
	b.releaseLarge()
	b.releaseBudget()
	if b.ReadCloser == nil {
		return nil
	}
	return b.ReadCloser.Close()
}

func (b *requestAdmissionBody) setBudgetRelease(release func()) {
	if b == nil || release == nil {
		return
	}
	b.mu.Lock()
	if b.budgetRelease == nil {
		b.budgetRelease = release
		release = nil
	}
	b.mu.Unlock()
	if release != nil {
		release()
	}
}

func (b *requestAdmissionBody) releaseBudget() {
	if b == nil {
		return
	}
	b.mu.Lock()
	release := b.budgetRelease
	b.budgetRelease = nil
	b.mu.Unlock()
	if release != nil {
		release()
	}
}

func (b *requestAdmissionBody) setLargeRelease(release func()) {
	if b == nil || release == nil {
		return
	}
	b.mu.Lock()
	if b.largeRelease == nil {
		b.largeRelease = release
		release = nil
	}
	b.mu.Unlock()
	if release != nil {
		release()
	}
}

func (b *requestAdmissionBody) releaseLarge() {
	if b == nil {
		return
	}
	b.mu.Lock()
	release := b.largeRelease
	b.largeRelease = nil
	b.mu.Unlock()
	if release != nil {
		release()
	}
}
