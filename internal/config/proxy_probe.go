package config

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/proxyutil"
)

const defaultProxyProbeTimeout = 800 * time.Millisecond

// ProxyProber is a lightweight connectivity check run before a config
// generation is published. Tests inject a fake implementation so the commit
// path stays deterministic.
type ProxyProber interface {
	Probe(ctx context.Context, proxyURL string) error
}

// ProbeError is a typed pre-probe failure. Commit keeps the previous
// generation and logs Cause.
type ProbeError struct {
	Cause string
	URL   string
	Err   error
}

func (e *ProbeError) Error() string {
	if e == nil {
		return ""
	}
	if e.Cause == "" {
		return e.Err.Error()
	}
	if e.Err == nil {
		return e.Cause
	}
	return e.Cause + ": " + e.Err.Error()
}

func (e *ProbeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

const (
	ProbeCauseDialTimeout = "dial_timeout"
	ProbeCauseUnreachable = "unreachable"
	ProbeCauseHTTPFailed  = "http_failed"
	ProbeCauseInvalidURL  = "invalid_url"
	ProbeCauseFailed      = "probe_failed"
)

// DefaultProxyProber dials the proxy host with a short timeout and, for
// http/https proxies, issues a bounded HEAD. Inherit/direct settings skip.
type DefaultProxyProber struct {
	Timeout time.Duration
}

// Probe implements ProxyProber.
func (p DefaultProxyProber) Probe(ctx context.Context, proxyURL string) error {
	setting, errParse := proxyutil.Parse(proxyURL)
	if errParse != nil {
		return &ProbeError{Cause: ProbeCauseInvalidURL, URL: proxyURL, Err: errParse}
	}
	if setting.Mode != proxyutil.ModeProxy || setting.URL == nil {
		return nil
	}
	timeout := p.Timeout
	if timeout <= 0 {
		timeout = defaultProxyProbeTimeout
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	dialer := net.Dialer{Timeout: timeout}
	conn, errDial := dialer.DialContext(ctx, "tcp", proxyDialHost(setting.URL.Host, setting.URL.Scheme))
	if errDial != nil {
		cause := ProbeCauseUnreachable
		if isProbeTimeout(errDial) {
			cause = ProbeCauseDialTimeout
		}
		return &ProbeError{Cause: cause, URL: proxyURL, Err: errDial}
	}
	defer conn.Close()

	scheme := strings.ToLower(setting.URL.Scheme)
	if scheme != "http" && scheme != "https" {
		return nil
	}
	if errDeadline := conn.SetDeadline(time.Now().Add(timeout)); errDeadline != nil {
		return &ProbeError{Cause: ProbeCauseHTTPFailed, URL: proxyURL, Err: errDeadline}
	}
	host := setting.URL.Host
	req := "HEAD / HTTP/1.1\r\nHost: " + host + "\r\nConnection: close\r\n\r\n"
	if _, errWrite := io.WriteString(conn, req); errWrite != nil {
		cause := ProbeCauseHTTPFailed
		if isProbeTimeout(errWrite) {
			cause = ProbeCauseDialTimeout
		}
		return &ProbeError{Cause: cause, URL: proxyURL, Err: errWrite}
	}
	buf := make([]byte, 64)
	if _, errRead := conn.Read(buf); errRead != nil && !errors.Is(errRead, io.EOF) {
		cause := ProbeCauseHTTPFailed
		if isProbeTimeout(errRead) {
			cause = ProbeCauseDialTimeout
		}
		return &ProbeError{Cause: cause, URL: proxyURL, Err: errRead}
	}
	return nil
}

func proxyDialHost(host, scheme string) string {
	if strings.Contains(host, ":") {
		return host
	}
	switch strings.ToLower(scheme) {
	case "https":
		return net.JoinHostPort(host, "443")
	case "socks5", "socks5h":
		return net.JoinHostPort(host, "1080")
	default:
		return net.JoinHostPort(host, "80")
	}
}

func isProbeTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, osTimeoutError{}) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

type osTimeoutError struct{}

func (osTimeoutError) Error() string { return "timeout" }

// ProbeConfigProxies runs prober against every concrete proxy URL in cfg.
// A nil prober uses DefaultProxyProber. Failure must keep the previous
// generation; the returned error is typed (*ProbeError).
func ProbeConfigProxies(ctx context.Context, cfg *Config, prober ProxyProber) error {
	if cfg == nil {
		return nil
	}
	if prober == nil {
		prober = DefaultProxyProber{}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for _, raw := range cfg.ProxyURLs() {
		setting, errParse := proxyutil.Parse(raw)
		if errParse != nil {
			return &ProbeError{Cause: ProbeCauseInvalidURL, URL: raw, Err: errParse}
		}
		if setting.Mode != proxyutil.ModeProxy {
			continue
		}
		if errProbe := prober.Probe(ctx, raw); errProbe != nil {
			var typed *ProbeError
			if errors.As(errProbe, &typed) && typed != nil {
				if typed.URL == "" {
					typed.URL = raw
				}
				return typed
			}
			return &ProbeError{Cause: ProbeCauseFailed, URL: raw, Err: errProbe}
		}
	}
	return nil
}

// ValidateConfigCommit is the in-memory generation gate: weight, proxy-URL
// syntax, and monotonic revision. Connectivity probing is a separate step so
// tests can inject a prober.
func ValidateConfigCommit(current, next *Config) error {
	if next == nil {
		return fmt.Errorf("config is nil")
	}
	if errWeights := next.ValidateCredentialWeights(); errWeights != nil {
		return errWeights
	}
	if errProxy := next.ValidateProxyURLs(); errProxy != nil {
		return errProxy
	}
	return CheckMonotonicRevision(current, next)
}

// NopProxyProber always succeeds. Tests that exercise commit without a
// network use this instead of DefaultProxyProber.
type NopProxyProber struct{}

func (NopProxyProber) Probe(context.Context, string) error { return nil }

// FailProxyProber always fails with Cause.
type FailProxyProber struct {
	Cause string
}

func (p FailProxyProber) Probe(_ context.Context, proxyURL string) error {
	cause := strings.TrimSpace(p.Cause)
	if cause == "" {
		cause = ProbeCauseFailed
	}
	return &ProbeError{Cause: cause, URL: proxyURL, Err: fmt.Errorf("injected probe failure")}
}
