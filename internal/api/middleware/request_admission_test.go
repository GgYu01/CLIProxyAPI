package middleware

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	apihandlers "github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)

func TestRequestAdmissionProductionDefaults(t *testing.T) {
	cfg := DefaultRequestAdmissionConfig()
	if cfg.MaxInFlight != 42 || cfg.MaxWaiting != 8 {
		t.Fatalf("text admission = %d in flight / %d waiting, want 42 / 8", cfg.MaxInFlight, cfg.MaxWaiting)
	}
	if cfg.ImageMaxInFlight != 30 || cfg.ImageMaxWaiting != 10 {
		t.Fatalf("image admission = %d in flight / %d waiting, want 30 / 10", cfg.ImageMaxInFlight, cfg.ImageMaxWaiting)
	}
	if cfg.LargeBodyThresholdBytes != 8<<20 || cfg.LargeBodyMaxInFlight != 16 || cfg.MaxBodyBytes != 256<<20 {
		t.Fatalf("large body admission = threshold %d, in flight %d, max %d", cfg.LargeBodyThresholdBytes, cfg.LargeBodyMaxInFlight, cfg.MaxBodyBytes)
	}
	if cfg.BodyBudgetBytes != 512<<20 || cfg.BodyBudgetWaitTimeout != 30*time.Second {
		t.Fatalf("aggregate body budget = %d / %s, want 512 MiB / 30s", cfg.BodyBudgetBytes, cfg.BodyBudgetWaitTimeout)
	}
	if len(cfg.ModelRPM) != 0 || cfg.ModelRPMWindow != time.Minute {
		t.Fatalf("model RPM = %#v / %s, want disabled / 1m", cfg.ModelRPM, cfg.ModelRPMWindow)
	}
	if cfg.FirstByteTimeout != 600*time.Second || cfg.FirstByteRetryAfter != 5*time.Second {
		t.Fatalf("first-byte policy = %s / %s, want 600s / 5s", cfg.FirstByteTimeout, cfg.FirstByteRetryAfter)
	}
}

func TestRequestAdmissionAggregateBodyBudgetIsBoundedAndReleased(t *testing.T) {
	gin.SetMode(gin.TestMode)
	admission := NewRequestAdmission(RequestAdmissionConfig{
		Enabled:               true,
		MaxInFlight:           3,
		MaxWaiting:            1,
		WaitTimeout:           time.Second,
		MaxBodyBytes:          64,
		BodyBudgetBytes:       16,
		BodyBudgetWaitTimeout: 5 * time.Second,
	})
	router := gin.New()
	router.Use(admission.Middleware())
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	router.POST("/v1/responses", func(c *gin.Context) {
		started <- struct{}{}
		<-release
		c.Status(http.StatusNoContent)
	})

	done := make(chan int, 2)
	serve := func() {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader("0123456789"))
		router.ServeHTTP(response, request)
		done <- response.Code
	}
	go serve()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first body-budget request did not start")
	}
	go serve()
	eventuallyAdmission(t, time.Second, func() bool { return admission.Stats().BodyBytesWaiting == 1 })

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader("0123456789"))
	router.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("request beyond body budget waiting room status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
	if got := response.Header().Get("Retry-After"); got != "5" {
		t.Fatalf("body-budget Retry-After = %q, want 5", got)
	}
	stats := admission.Stats()
	if stats.BodyBytesInFlight != 10 || stats.BodyBytesPeak != 10 || stats.BodyBytesWaiting != 1 {
		t.Fatalf("body budget at saturation = %#v, want 10 bytes reserved and one waiter", stats)
	}

	close(release)
	for range 2 {
		select {
		case code := <-done:
			if code != http.StatusNoContent {
				t.Fatalf("body-budget admitted request status = %d, want %d", code, http.StatusNoContent)
			}
		case <-time.After(time.Second):
			t.Fatal("body-budget admitted request did not finish")
		}
	}
	eventuallyAdmission(t, time.Second, func() bool {
		stats := admission.Stats()
		return stats.BodyBytesInFlight == 0 && stats.BodyBytesWaiting == 0 && stats.InFlight == 0
	})
}

func TestRequestAdmissionUnknownLengthBodyReservesConservativeBudget(t *testing.T) {
	gin.SetMode(gin.TestMode)
	admission := NewRequestAdmission(RequestAdmissionConfig{
		Enabled:               true,
		MaxInFlight:           3,
		MaxWaiting:            1,
		WaitTimeout:           time.Second,
		MaxBodyBytes:          10,
		BodyBudgetBytes:       15,
		BodyBudgetWaitTimeout: 5 * time.Second,
	})
	router := gin.New()
	router.Use(admission.Middleware())
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	router.POST("/v1/responses", func(c *gin.Context) {
		started <- struct{}{}
		<-release
		c.Status(http.StatusNoContent)
	})

	serve := func(done chan<- int) {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/v1/responses", io.NopCloser(strings.NewReader("{}")))
		request.ContentLength = -1
		router.ServeHTTP(response, request)
		done <- response.Code
	}
	done := make(chan int, 2)
	go serve(done)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first unknown-length request did not start")
	}
	go serve(done)
	eventuallyAdmission(t, time.Second, func() bool { return admission.Stats().BodyBytesWaiting == 1 })

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", io.NopCloser(strings.NewReader("{}")))
	request.ContentLength = -1
	router.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("unknown-length request beyond body waiting room status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
	stats := admission.Stats()
	if stats.BodyBytesInFlight != 10 || stats.BodyBytesWaiting != 1 || stats.BodyBytesPeak != 10 {
		t.Fatalf("unknown-length body budget at saturation = %#v, want conservative 10-byte reservation and one waiter", stats)
	}

	close(release)
	for range 2 {
		select {
		case code := <-done:
			if code != http.StatusNoContent {
				t.Fatalf("unknown-length admitted request status = %d, want %d", code, http.StatusNoContent)
			}
		case <-time.After(time.Second):
			t.Fatal("unknown-length admitted request did not finish")
		}
	}
	eventuallyAdmission(t, time.Second, func() bool {
		stats := admission.Stats()
		return stats.BodyBytesInFlight == 0 && stats.BodyBytesWaiting == 0 && stats.InFlight == 0
	})
}

func TestRequestAdmissionFirstByteTimeoutCancelsAndReturnsGatewayTimeout(t *testing.T) {
	gin.SetMode(gin.TestMode)
	admission := NewRequestAdmission(RequestAdmissionConfig{
		Enabled:             true,
		MaxInFlight:         1,
		MaxWaiting:          1,
		WaitTimeout:         time.Second,
		MaxBodyBytes:        1 << 20,
		FirstByteTimeout:    20 * time.Millisecond,
		FirstByteRetryAfter: 7 * time.Second,
	})
	router := gin.New()
	router.Use(admission.Middleware())
	cause := make(chan error, 1)
	router.POST("/v1/responses", func(c *gin.Context) {
		<-c.Request.Context().Done()
		cause <- context.Cause(c.Request.Context())
		c.JSON(http.StatusInternalServerError, gin.H{"legacy": "must be suppressed"})
	})

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.6-sol"}`))
	router.ServeHTTP(response, request)
	if response.Code != http.StatusGatewayTimeout {
		t.Fatalf("first-byte timeout status = %d, want %d; body=%s", response.Code, http.StatusGatewayTimeout, response.Body.String())
	}
	if got := response.Header().Get("Retry-After"); got != "7" {
		t.Fatalf("first-byte Retry-After = %q, want 7", got)
	}
	select {
	case got := <-cause:
		if !errors.Is(got, errUpstreamFirstByteTimeout) {
			t.Fatalf("request cancellation cause = %v, want first-byte timeout", got)
		}
	default:
		t.Fatal("handler did not observe first-byte cancellation")
	}
	stats := admission.Stats()
	if stats.UpstreamFirstByteTimeout != 1 || stats.InFlight != 0 || stats.Waiting != 0 {
		t.Fatalf("stats after first-byte timeout = %#v, want timeout counted and admission released", stats)
	}
}

func TestRequestAdmissionFirstByteDisarmsTimeoutForActiveStream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	admission := NewRequestAdmission(RequestAdmissionConfig{
		Enabled:             true,
		MaxInFlight:         1,
		MaxWaiting:          1,
		WaitTimeout:         time.Second,
		MaxBodyBytes:        1 << 20,
		FirstByteTimeout:    15 * time.Millisecond,
		FirstByteRetryAfter: time.Second,
	})
	router := gin.New()
	router.Use(admission.Middleware())
	router.POST("/v1/responses", func(c *gin.Context) {
		_, _ = c.Writer.Write([]byte("a"))
		c.Writer.Flush()
		time.Sleep(40 * time.Millisecond)
		_, _ = c.Writer.Write([]byte("b"))
	})

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.6-sol","stream":true}`))
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK || response.Body.String() != "ab" {
		t.Fatalf("active stream = status %d body %q, want 200 and ab", response.Code, response.Body.String())
	}
	if stats := admission.Stats(); stats.UpstreamFirstByteTimeout != 0 || stats.InFlight != 0 {
		t.Fatalf("active stream stats = %#v, want no timeout and released admission", stats)
	}
}

func TestRequestAdmissionSuccessfulHijackDisarmsFirstByteTimeout(t *testing.T) {
	gin.SetMode(gin.TestMode)
	admission := NewRequestAdmission(RequestAdmissionConfig{
		Enabled:             true,
		MaxInFlight:         1,
		MaxWaiting:          1,
		WaitTimeout:         time.Second,
		MaxBodyBytes:        1 << 20,
		FirstByteTimeout:    15 * time.Millisecond,
		FirstByteRetryAfter: time.Second,
	})
	router := gin.New()
	router.Use(admission.Middleware())
	contextCanceled := make(chan bool, 1)
	router.GET("/v1/responses", func(c *gin.Context) {
		connection, _, err := c.Writer.Hijack()
		if err != nil {
			contextCanceled <- true
			return
		}
		defer connection.Close()
		time.Sleep(40 * time.Millisecond)
		contextCanceled <- c.Request.Context().Err() != nil
	})

	response := newAdmissionHijackRecorder()
	request := httptest.NewRequest(http.MethodGet, "/v1/responses", nil)
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Upgrade", "websocket")
	router.ServeHTTP(response, request)
	if canceled := <-contextCanceled; canceled {
		t.Fatal("successful WebSocket hijack remained subject to the first-byte timeout")
	}
	if stats := admission.Stats(); stats.UpstreamFirstByteTimeout != 0 || stats.InFlight != 0 {
		t.Fatalf("hijacked request stats = %#v, want no timeout and released admission", stats)
	}
}

type admissionHijackRecorder struct {
	*httptest.ResponseRecorder
	server net.Conn
	client net.Conn
}

func newAdmissionHijackRecorder() *admissionHijackRecorder {
	server, client := net.Pipe()
	return &admissionHijackRecorder{ResponseRecorder: httptest.NewRecorder(), server: server, client: client}
}

func (r *admissionHijackRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return r.server, bufio.NewReadWriter(bufio.NewReader(r.server), bufio.NewWriter(r.server)), nil
}

func TestRequestAdmissionQueueAndWaitingRoomAreBounded(t *testing.T) {
	gin.SetMode(gin.TestMode)
	admission := NewRequestAdmission(RequestAdmissionConfig{
		Enabled:      true,
		MaxInFlight:  1,
		MaxWaiting:   1,
		WaitTimeout:  5 * time.Second,
		MaxBodyBytes: 1 << 20,
	})
	router := gin.New()
	router.Use(admission.Middleware())
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	router.POST("/v1/responses", func(c *gin.Context) {
		started <- struct{}{}
		<-release
		c.Status(http.StatusNoContent)
	})

	serve := func(done chan<- int) {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.6-sol"}`))
		router.ServeHTTP(response, request)
		done <- response.Code
	}
	done := make(chan int, 2)
	go serve(done)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first admitted request did not start")
	}
	go serve(done)
	eventuallyAdmission(t, time.Second, func() bool { return admission.Stats().Waiting == 1 })

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.6-sol"}`))
	startedAt := time.Now()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("request beyond bounded waiting room status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
	if elapsed := time.Since(startedAt); elapsed > time.Second {
		t.Fatalf("full waiting room rejected after %s, want immediate rejection", elapsed)
	}
	if got := response.Header().Get("Retry-After"); got != "5" {
		t.Fatalf("Retry-After = %q, want 5", got)
	}

	close(release)
	for range 2 {
		select {
		case code := <-done:
			if code != http.StatusNoContent {
				t.Fatalf("admitted request status = %d, want %d", code, http.StatusNoContent)
			}
		case <-time.After(time.Second):
			t.Fatal("admitted request did not finish")
		}
	}
	eventuallyAdmission(t, time.Second, func() bool {
		stats := admission.Stats()
		return stats.InFlight == 0 && stats.Waiting == 0
	})
}

func TestRequestAdmissionModelRPMWaitingIsBoundedAndCancelable(t *testing.T) {
	gin.SetMode(gin.TestMode)
	admission := NewRequestAdmission(RequestAdmissionConfig{
		Enabled:            true,
		MaxInFlight:        4,
		MaxWaiting:         1,
		WaitTimeout:        time.Second,
		MaxBodyBytes:       1 << 20,
		ModelRPM:           map[string]int{"gpt-5.6-luna": 2},
		ModelRPMWindow:     5 * time.Second,
		ModelRPMMaxWaiting: 1,
	})
	router := gin.New()
	router.Use(admission.Middleware())
	router.POST("/v1/responses", func(c *gin.Context) { c.Status(http.StatusNoContent) })

	for range 2 {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.6-luna"}`))
		router.ServeHTTP(response, request)
		if response.Code != http.StatusNoContent {
			t.Fatalf("request within RPM limit status = %d, want %d", response.Code, http.StatusNoContent)
		}
	}

	waitingContext, cancelWaiting := context.WithCancel(context.Background())
	defer cancelWaiting()
	waitingDone := make(chan struct{})
	go func() {
		defer close(waitingDone)
		request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.6-luna"}`)).WithContext(waitingContext)
		router.ServeHTTP(httptest.NewRecorder(), request)
	}()
	eventuallyAdmission(t, time.Second, func() bool {
		return admission.Stats().ModelRPMWaiting["gpt-5.6-luna"] == 1
	})

	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.6-luna"}`))
	router.ServeHTTP(response, request)
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("request beyond RPM waiting room status = %d, want %d", response.Code, http.StatusTooManyRequests)
	}
	if got := response.Header().Get("Retry-After"); got != "5" {
		t.Fatalf("RPM Retry-After = %q, want 5", got)
	}

	cancelWaiting()
	select {
	case <-waitingDone:
	case <-time.After(time.Second):
		t.Fatal("canceled RPM waiter did not finish")
	}
	eventuallyAdmission(t, time.Second, func() bool {
		return admission.Stats().ModelRPMWaiting["gpt-5.6-luna"] == 0
	})
}

func TestRequestAdmissionAllowsSixteenLargeBodiesAndQueuesSeventeenth(t *testing.T) {
	gin.SetMode(gin.TestMode)
	admission := NewRequestAdmission(RequestAdmissionConfig{
		Enabled:                 true,
		MaxInFlight:             32,
		MaxWaiting:              8,
		WaitTimeout:             time.Second,
		MaxBodyBytes:            1 << 20,
		LargeBodyThresholdBytes: 8,
		LargeBodyMaxInFlight:    16,
		LargeBodyWaitTimeout:    5 * time.Second,
	})
	router := gin.New()
	router.Use(admission.Middleware())
	started := make(chan struct{}, 17)
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	router.POST("/v1/responses", func(c *gin.Context) {
		if _, err := io.Copy(io.Discard, c.Request.Body); err != nil {
			c.Status(http.StatusBadRequest)
			return
		}
		started <- struct{}{}
		<-release
		c.Status(http.StatusNoContent)
	})

	done := make(chan int, 17)
	serve := func() {
		response := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.6-sol","input":"large"}`))
		router.ServeHTTP(response, request)
		done <- response.Code
	}
	for range 16 {
		go serve()
	}
	for range 16 {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("sixteen large requests did not enter concurrently")
		}
	}
	if stats := admission.Stats(); stats.LargeBodyInFlight != 16 || stats.LargeBodyInFlightPeak != 16 {
		t.Fatalf("large lane before seventeenth = %#v, want 16 active and peak", stats)
	}

	go serve()
	eventuallyAdmission(t, time.Second, func() bool { return admission.Stats().LargeBodyWaiting == 1 })
	close(release)
	for range 17 {
		select {
		case code := <-done:
			if code != http.StatusNoContent {
				t.Fatalf("large request status = %d, want %d", code, http.StatusNoContent)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("large request did not finish")
		}
	}
	eventuallyAdmission(t, time.Second, func() bool {
		stats := admission.Stats()
		return stats.InFlight == 0 && stats.Waiting == 0 && stats.LargeBodyInFlight == 0 && stats.LargeBodyWaiting == 0
	})
}

func TestRequestAdmissionChunkedBodyStopsAtFortyEightMiBAndReleases(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := DefaultRequestAdmissionConfig()
	admission := NewRequestAdmission(cfg)
	router := gin.New()
	router.Use(admission.Middleware())
	var readBytes int64
	router.POST("/v1/responses", func(c *gin.Context) {
		buffer := make([]byte, 32<<10)
		for {
			n, err := c.Request.Body.Read(buffer)
			readBytes += int64(n)
			if errors.Is(err, errRequestBodyTooLarge) {
				c.Status(http.StatusRequestEntityTooLarge)
				return
			}
			if err != nil {
				c.Status(http.StatusBadRequest)
				return
			}
		}
	})

	body := io.NopCloser(io.LimitReader(admissionZeroReader{}, cfg.MaxBodyBytes+1))
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", body)
	request.ContentLength = -1
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("chunked body status = %d, want %d", response.Code, http.StatusRequestEntityTooLarge)
	}
	if readBytes > cfg.MaxBodyBytes {
		t.Fatalf("handler received %d bytes, want at most %d", readBytes, cfg.MaxBodyBytes)
	}
	stats := admission.Stats()
	if stats.BodyTooLarge != 1 {
		t.Fatalf("body-too-large count = %d, want 1", stats.BodyTooLarge)
	}
	if stats.LargeBodyInFlight != 0 || stats.LargeBodyWaiting != 0 || stats.LargeBodyInFlightPeak != 1 {
		t.Fatalf("large lane after chunked rejection = %#v, want released with peak 1", stats)
	}
}

type admissionZeroReader struct{}

func (admissionZeroReader) Read(p []byte) (int, error) {
	return len(p), nil
}

func TestRequestAdmissionClientDisconnectBeforeUpstreamFirstByteCancelsAndReleases(t *testing.T) {
	gin.SetMode(gin.TestMode)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for raw upstream: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	upstreamStarted := make(chan struct{})
	upstreamSocketClosed := make(chan struct{})
	upstreamErr := make(chan error, 1)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			upstreamErr <- fmt.Errorf("accept upstream connection: %w", acceptErr)
			return
		}
		defer conn.Close()
		request, readErr := http.ReadRequest(bufio.NewReader(conn))
		if readErr != nil {
			upstreamErr <- fmt.Errorf("read upstream request: %w", readErr)
			return
		}
		if _, readErr = io.Copy(io.Discard, request.Body); readErr != nil {
			upstreamErr <- fmt.Errorf("read upstream request body: %w", readErr)
			return
		}
		_ = request.Body.Close()
		close(upstreamStarted)

		var probe [1]byte
		if _, readErr = conn.Read(probe[:]); readErr == nil {
			upstreamErr <- fmt.Errorf("upstream received unexpected bytes before socket close")
			return
		}
		close(upstreamSocketClosed)
	}()

	admission := NewRequestAdmission(RequestAdmissionConfig{
		Enabled:      true,
		MaxInFlight:  1,
		MaxWaiting:   1,
		WaitTimeout:  time.Second,
		MaxBodyBytes: 1 << 20,
	})
	router := gin.New()
	router.Use(admission.Middleware())
	baseHandler := &apihandlers.BaseAPIHandler{Cfg: &sdkconfig.SDKConfig{}}
	router.POST("/v1/responses", func(c *gin.Context) {
		upstreamContext, cancelUpstream := baseHandler.GetContextWithCancel(nil, c, context.Background())
		defer cancelUpstream()
		req, err := http.NewRequestWithContext(upstreamContext, http.MethodPost, "http://"+listener.Addr().String(), strings.NewReader(`{"model":"gpt-5.6-sol"}`))
		if err != nil {
			c.Status(http.StatusInternalServerError)
			return
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return
		}
		_ = resp.Body.Close()
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.6-sol"}`)).WithContext(ctx)
	clientDone := make(chan struct{})
	go func() {
		defer close(clientDone)
		router.ServeHTTP(httptest.NewRecorder(), req)
	}()

	select {
	case <-upstreamStarted:
	case err := <-upstreamErr:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("CPA upstream request did not start")
	}
	if got := admission.Stats().InFlight; got != 1 {
		t.Fatalf("inflight before disconnect = %d, want 1", got)
	}
	cancel()
	select {
	case <-upstreamSocketClosed:
	case err := <-upstreamErr:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("downstream disconnect did not close the upstream socket")
	}
	select {
	case <-clientDone:
	case <-time.After(time.Second):
		t.Fatal("downstream client did not finish after cancellation")
	}
	eventuallyAdmission(t, time.Second, func() bool {
		stats := admission.Stats()
		return stats.InFlight == 0 && stats.Waiting == 0 && stats.LargeBodyInFlight == 0
	})
}

func TestRequestAdmissionWaitTimeoutDoesNotLimitAcceptedLongRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	admission := NewRequestAdmission(RequestAdmissionConfig{
		Enabled:      true,
		MaxInFlight:  1,
		MaxWaiting:   1,
		WaitTimeout:  10 * time.Millisecond,
		MaxBodyBytes: 1 << 20,
	})
	router := gin.New()
	router.Use(admission.Middleware())
	router.POST("/v1/responses", func(c *gin.Context) {
		time.Sleep(60 * time.Millisecond)
		c.Status(http.StatusNoContent)
	})
	response := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"gpt-5.6-sol"}`))
	router.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("accepted long request status = %d, want %d", response.Code, http.StatusNoContent)
	}
	if stats := admission.Stats(); stats.InFlight != 0 || stats.Waiting != 0 {
		t.Fatalf("stats after long request = %#v, want released", stats)
	}
}

func eventuallyAdmission(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	if !condition() {
		t.Fatal("condition was not satisfied before timeout")
	}
}
