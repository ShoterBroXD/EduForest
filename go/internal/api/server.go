// Package api: coordinador de EduForest (REST, WebSocket, TCP a nodos, Mongo y Redis).
package api

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ShoterBroXD/EduForest/internal/cluster"
	"github.com/ShoterBroXD/EduForest/internal/dataset"
	"github.com/ShoterBroXD/EduForest/internal/forest"
	"github.com/ShoterBroXD/EduForest/internal/store"
)

type Config struct {
	ModelsDir       string
	TestYear        int
	JWTSecret       []byte
	TokenTTL        time.Duration
	AdminUser       string
	AdminPass       string
	CORSOrigin      string
	MonitorInterval time.Duration
	Workers         int
}

// Model es un bosque entrenado que la API usa para responder predicciones.
type Model struct {
	ID       string         `json:"id"`
	Objetivo string         `json:"objetivo"`
	Modo     string         `json:"modo"`
	Clases   []string       `json:"clases"`
	Nodos    []string       `json:"nodos"` // nodos que guardan sus árboles y pueden votar
	Arboles  int            `json:"arboles"`
	Creado   time.Time      `json:"creado"`
	Forest   *forest.Forest `json:"-"`
}

// targetData: split temporal y set discretizado de una variable objetivo.
type targetData struct {
	clases []string
	split  dataset.Split
	bins   *forest.Binned
}

type Server struct {
	cfg   Config
	store store.Store
	cache store.Cache
	coord *cluster.Coordinator
	ds    *dataset.Dataset
	hub   *Hub

	mu     sync.RWMutex // protege models y data
	models map[string]*Model
	data   map[string]*targetData

	training atomic.Bool // solo un entrenamiento a la vez
	started  time.Time
}

func New(cfg Config, st store.Store, cache store.Cache, coord *cluster.Coordinator, ds *dataset.Dataset) *Server {
	if cfg.Workers < 1 {
		cfg.Workers = runtime.NumCPU()
	}
	if cfg.TokenTTL == 0 {
		cfg.TokenTTL = 12 * time.Hour
	}
	if cfg.MonitorInterval == 0 {
		cfg.MonitorInterval = 5 * time.Second
	}
	if cfg.TestYear == 0 {
		cfg.TestYear = 2024
	}
	return &Server{
		cfg: cfg, store: st, cache: cache, coord: coord, ds: ds,
		hub:     NewHub(),
		models:  map[string]*Model{},
		data:    map[string]*targetData{},
		started: time.Now(),
	}
}

// Start lanza el hub y el monitor, crea el admin y carga los modelos del disco.
func (s *Server) Start(ctx context.Context) {
	go s.hub.Run(ctx)
	go s.monitor(ctx)
	if err := s.seedAdmin(ctx); err != nil {
		log.Printf("[api] no se pudo crear el usuario admin: %v", err)
	}
	if n, err := s.store.CerrarInterrumpidos(ctx); err == nil && n > 0 {
		s.logEvent("alerta", "api", fmt.Sprintf("%d entrenamiento(s) quedaron en curso al reiniciar la API y se marcaron como fallidos", n))
	}
	s.loadModels()
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/salud", s.handleSalud)
	mux.HandleFunc("POST /api/auth/login", s.handleLogin)
	mux.HandleFunc("GET /api/auth/yo", s.auth(s.handleYo, ""))
	mux.HandleFunc("POST /api/usuarios", s.auth(s.handleCrearUsuario, RolAdmin))

	mux.HandleFunc("GET /api/cluster", s.auth(s.handleCluster, ""))
	mux.HandleFunc("GET /api/cluster/metricas", s.auth(s.handleMetricas, ""))

	mux.HandleFunc("POST /api/entrenamientos", s.auth(s.handleEntrenar, RolAdmin))
	mux.HandleFunc("GET /api/entrenamientos", s.auth(s.handleEntrenamientos, ""))
	mux.HandleFunc("GET /api/entrenamientos/{id}", s.auth(s.handleEntrenamiento, ""))
	mux.HandleFunc("GET /api/modelos", s.auth(s.handleModelos, ""))

	mux.HandleFunc("POST /api/predicciones", s.auth(s.handlePredecir, ""))
	mux.HandleFunc("GET /api/colegios", s.auth(s.handleRanking, ""))
	mux.HandleFunc("GET /api/colegios/{codmod}", s.auth(s.handleColegio, ""))

	mux.HandleFunc("GET /api/logs", s.auth(s.handleLogs, RolAdmin))
	mux.HandleFunc("GET /api/estadisticas", s.auth(s.handleEstadisticas, RolAdmin))

	mux.HandleFunc("GET /ws", s.handleWS)
	mux.HandleFunc("GET /swagger/", s.handleSwagger)
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/swagger/", http.StatusFound)
	})
	return s.withMiddleware(mux)
}

// withMiddleware agrega CORS, el registro de cada petición y el contador de
// peticiones por ruta en Redis.
func (s *Server) withMiddleware(next *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := s.cfg.CORSOrigin
		if origin == "" {
			origin = "*"
		}
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		_, pattern := next.Handler(r)
		if pattern != "" && pattern != "GET /ws" {
			go s.cache.Incr(context.Background(), "api:peticiones", pattern)
		}
		if r.URL.Path != "/api/salud" {
			log.Printf("[api] %s %s %d %.1fms", r.Method, r.URL.Path, rec.status, float64(time.Since(start).Microseconds())/1000)
		}
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// Hijack permite que el WebSocket tome la conexión aunque pase por el middleware.
func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return http.NewResponseController(r.ResponseWriter).Hijack()
}

func (r *statusRecorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (s *Server) logEvent(nivel, origen, msg string) {
	log.Printf("[%s] %s", origen, msg)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s.store.GuardarLog(ctx, store.Log{Fecha: time.Now(), Nivel: nivel, Origen: origen, Mensaje: msg})
	s.hub.Broadcast(map[string]any{"tipo": "log", "nivel": nivel, "origen": origen, "mensaje": msg})
}
