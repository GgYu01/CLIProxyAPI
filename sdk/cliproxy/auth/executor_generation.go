package auth

import (
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// executorGeneration is an immutable executor generation. Hot updates publish a
// new generation for future requests; in-flight attempts keep a lease on the
// old one until they finish.
type executorGeneration struct {
	id        string
	executor  ProviderExecutor
	createdAt time.Time
	active    atomic.Int64
	draining  atomic.Bool
}

func newExecutorGeneration(executor ProviderExecutor) *executorGeneration {
	return &executorGeneration{
		id:        uuidNow(),
		executor:  executor,
		createdAt: time.Now().UTC(),
	}
}

func (g *executorGeneration) ID() string {
	if g == nil {
		return ""
	}
	return g.id
}

func (g *executorGeneration) acquire() {
	if g == nil {
		return
	}
	g.active.Add(1)
}

func (g *executorGeneration) release() {
	if g == nil {
		return
	}
	if g.active.Add(-1) == 0 && g.draining.Load() {
		closeExecutorGeneration(g, "active=0")
	}
}

var generationClosers sync.Map // *executorGeneration -> struct{}

func closeExecutorGeneration(g *executorGeneration, cause string) {
	if g == nil {
		return
	}
	if _, loaded := generationClosers.LoadOrStore(g, cause); loaded {
		return
	}
	if closer, ok := g.executor.(ExecutionSessionCloser); ok && closer != nil {
		closer.CloseExecutionSession(CloseAllExecutionSessionsID)
	}
}

func uuidNow() string {
	return uuid.NewString()
}

func (m *Manager) acquireExecutorGeneration(provider string) *executorGeneration {
	if m == nil {
		return nil
	}
	provider = strings.TrimSpace(provider)
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.executorGenerations == nil {
		return nil
	}
	g := m.executorGenerations[provider]
	if g == nil {
		g = m.executorGenerations[strings.ToLower(provider)]
	}
	g.acquire()
	return g
}

func (m *Manager) acquireExecutorGenerationFor(executor ProviderExecutor) *executorGeneration {
	if m == nil || executor == nil {
		return nil
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	provider := strings.TrimSpace(executor.Identifier())
	if g := generationForExecutor(m.executorLiveGenerations[provider], executor); g != nil {
		g.acquire()
		return g
	}
	if lower := strings.ToLower(provider); lower != provider {
		if g := generationForExecutor(m.executorLiveGenerations[lower], executor); g != nil {
			g.acquire()
			return g
		}
	}
	g := m.executorGenerations[provider]
	if g == nil {
		g = m.executorGenerations[strings.ToLower(provider)]
	}
	if g != nil && g.executor == executor {
		g.acquire()
		return g
	}
	return nil
}

func generationForExecutor(gens []*executorGeneration, executor ProviderExecutor) *executorGeneration {
	for _, g := range gens {
		if g != nil && g.executor == executor {
			return g
		}
	}
	return nil
}

func compactLiveGenerations(gens []*executorGeneration) []*executorGeneration {
	if len(gens) == 0 {
		return gens
	}
	out := gens[:0]
	for _, g := range gens {
		if g == nil {
			continue
		}
		if _, closed := generationClosers.Load(g); closed && g.active.Load() == 0 {
			continue
		}
		out = append(out, g)
	}
	return out
}

// ExecutorGenerationID returns the current published generation id for provider.
func (m *Manager) ExecutorGenerationID(provider string) string {
	if m == nil {
		return ""
	}
	provider = strings.TrimSpace(provider)
	m.mu.RLock()
	defer m.mu.RUnlock()
	g := m.executorGenerations[provider]
	if g == nil {
		g = m.executorGenerations[strings.ToLower(provider)]
	}
	return g.ID()
}

// ExecutorGenerationActive returns the in-flight lease count on the current
// generation. Draining previous generations are not included.
func (m *Manager) ExecutorGenerationActive(provider string) int64 {
	if m == nil {
		return 0
	}
	provider = strings.TrimSpace(provider)
	m.mu.RLock()
	defer m.mu.RUnlock()
	g := m.executorGenerations[provider]
	if g == nil {
		g = m.executorGenerations[strings.ToLower(provider)]
	}
	if g == nil {
		return 0
	}
	return g.active.Load()
}

// ExecutorGenerationLeaseTotal returns active leases across current and
// draining generations for provider.
func (m *Manager) ExecutorGenerationLeaseTotal(provider string) int64 {
	if m == nil {
		return 0
	}
	provider = strings.TrimSpace(provider)
	m.mu.RLock()
	defer m.mu.RUnlock()
	var total int64
	for _, g := range m.executorLiveGenerations[provider] {
		if g != nil {
			total += g.active.Load()
		}
	}
	if lower := strings.ToLower(provider); lower != provider {
		for _, g := range m.executorLiveGenerations[lower] {
			if g != nil {
				total += g.active.Load()
			}
		}
	}
	return total
}

type generationLease struct {
	g *executorGeneration
}

func (m *Manager) holdExecutorGeneration(executor ProviderExecutor) *generationLease {
	if m == nil {
		return nil
	}
	return &generationLease{g: m.acquireExecutorGenerationFor(executor)}
}

func (l *generationLease) Release() {
	if l == nil || l.g == nil {
		return
	}
	l.g.release()
	l.g = nil
}

func (l *generationLease) ID() string {
	if l == nil || l.g == nil {
		return ""
	}
	return l.g.ID()
}

func (m *Manager) rememberLiveGenerationLocked(provider string, gen *executorGeneration) {
	if m == nil || gen == nil || provider == "" {
		return
	}
	if m.executorLiveGenerations == nil {
		m.executorLiveGenerations = make(map[string][]*executorGeneration)
	}
	m.executorLiveGenerations[provider] = compactLiveGenerations(append(m.executorLiveGenerations[provider], gen))
}

func (m *Manager) liveGenerationsLocked(provider string) []*executorGeneration {
	if m == nil {
		return nil
	}
	out := append([]*executorGeneration(nil), m.executorLiveGenerations[provider]...)
	if lower := strings.ToLower(provider); lower != provider {
		out = append(out, m.executorLiveGenerations[lower]...)
	}
	if g := m.executorGenerations[provider]; g != nil {
		out = append(out, g)
	}
	return out
}
