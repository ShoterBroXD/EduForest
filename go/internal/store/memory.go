package store

import (
	"context"
	"sort"
	"sync"
	"time"
)

// Memory implementa Store y Cache en memoria. Se usa en las pruebas y
// cuando la API corre sin MongoDB ni Redis (go run ./cmd/api sin variables).
type Memory struct {
	mu             sync.RWMutex
	entrenamientos map[string]Entrenamiento
	metricas       []MetricaNodo
	colegios       map[string][]Colegio
	usuarios       map[string]Usuario
	logs           []Log
	cache          map[string]cacheItem
	contadores     map[string]map[string]int64
}

type cacheItem struct {
	val    []byte
	expira time.Time
}

func NewMemory() *Memory {
	return &Memory{
		entrenamientos: map[string]Entrenamiento{},
		colegios:       map[string][]Colegio{},
		usuarios:       map[string]Usuario{},
		cache:          map[string]cacheItem{},
		contadores:     map[string]map[string]int64{},
	}
}

func (m *Memory) Ping(context.Context) error  { return nil }
func (m *Memory) Close(context.Context) error { return nil }

func (m *Memory) GuardarEntrenamiento(_ context.Context, e *Entrenamiento) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entrenamientos[e.ID] = *e
	return nil
}

func (m *Memory) Entrenamiento(_ context.Context, id string) (*Entrenamiento, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.entrenamientos[id]
	if !ok {
		return nil, ErrNoEncontrado
	}
	return &e, nil
}

func (m *Memory) Entrenamientos(_ context.Context, limite int) ([]Entrenamiento, error) {
	m.mu.RLock()
	out := make([]Entrenamiento, 0, len(m.entrenamientos))
	for _, e := range m.entrenamientos {
		out = append(out, e)
	}
	m.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Inicio.After(out[j].Inicio) })
	return out[:min(limite, len(out))], nil
}

func (m *Memory) GuardarMetricas(_ context.Context, ms []MetricaNodo) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.metricas = append(m.metricas, ms...)
	if len(m.metricas) > 10000 {
		m.metricas = m.metricas[len(m.metricas)-10000:]
	}
	return nil
}

func (m *Memory) Metricas(_ context.Context, nodo string, limite int) ([]MetricaNodo, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []MetricaNodo{}
	for i := len(m.metricas) - 1; i >= 0 && len(out) < limite; i-- {
		if nodo == "" || m.metricas[i].Nodo == nodo {
			out = append(out, m.metricas[i])
		}
	}
	return out, nil
}

func (m *Memory) GuardarColegios(_ context.Context, objetivo string, cs []Colegio) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.colegios[objetivo] = append([]Colegio(nil), cs...)
	return nil
}

func (m *Memory) Colegios(_ context.Context, objetivo string, codMod int) ([]Colegio, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []Colegio{}
	for _, c := range m.colegios[objetivo] {
		if c.CodMod == codMod {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Anexo < out[j].Anexo })
	return out, nil
}

func (m *Memory) Ranking(_ context.Context, objetivo string, limite int) ([]Colegio, error) {
	m.mu.RLock()
	out := append([]Colegio(nil), m.colegios[objetivo]...)
	m.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		if out[i].AlumnosAlta != out[j].AlumnosAlta {
			return out[i].AlumnosAlta > out[j].AlumnosAlta
		}
		return out[i].ProbClaseAlta > out[j].ProbClaseAlta
	})
	return out[:min(limite, len(out))], nil
}

func (m *Memory) Usuario(_ context.Context, nombre string) (*Usuario, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	u, ok := m.usuarios[nombre]
	if !ok {
		return nil, ErrNoEncontrado
	}
	return &u, nil
}

func (m *Memory) GuardarUsuario(_ context.Context, u *Usuario) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.usuarios[u.Nombre] = *u
	return nil
}

func (m *Memory) GuardarLog(_ context.Context, l Log) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.logs = append(m.logs, l)
	return nil
}

func (m *Memory) Logs(_ context.Context, limite int) ([]Log, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := []Log{}
	for i := len(m.logs) - 1; i >= 0 && len(out) < limite; i-- {
		out = append(out, m.logs[i])
	}
	return out, nil
}

// Métodos de Cache.

func (m *Memory) Get(_ context.Context, key string) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	it, ok := m.cache[key]
	if !ok || time.Now().After(it.expira) {
		return nil, false
	}
	return it.val, true
}

func (m *Memory) Set(_ context.Context, key string, val []byte, ttl time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cache[key] = cacheItem{val: val, expira: time.Now().Add(ttl)}
}

func (m *Memory) Incr(_ context.Context, hash, field string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.contadores[hash] == nil {
		m.contadores[hash] = map[string]int64{}
	}
	m.contadores[hash][field]++
}

func (m *Memory) Counters(_ context.Context, hash string) map[string]int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := map[string]int64{}
	for k, v := range m.contadores[hash] {
		out[k] = v
	}
	return out
}

// memoryCache adapta Memory a la interfaz Cache (Close sin contexto).
type memoryCache struct{ *Memory }

func (c memoryCache) Close() error { return nil }

func NewMemoryCache() Cache { return memoryCache{NewMemory()} }
