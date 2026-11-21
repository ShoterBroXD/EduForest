package api

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/ShoterBroXD/EduForest/internal/cluster"
	"github.com/ShoterBroXD/EduForest/internal/dataset"
	"github.com/ShoterBroXD/EduForest/internal/store"
)

func intParam(r *http.Request, name string, def, max int) int {
	v, err := strconv.Atoi(r.URL.Query().Get(name))
	if err != nil || v < 1 {
		return def
	}
	return min(v, max)
}

func objetivoParam(r *http.Request) (string, error) {
	o := r.URL.Query().Get("objetivo")
	if o == "" {
		return "riesgo", nil
	}
	if o != "riesgo" && o != "logro" {
		return "", errors.New("objetivo inválido: usa riesgo o logro")
	}
	return o, nil
}

func (s *Server) handleSalud(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), time.Second)
	defer cancel()
	estado := func(err error) string {
		if err != nil {
			return "error: " + err.Error()
		}
		return "ok"
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"estado":            "ok",
		"mongo":             estado(s.store.Ping(ctx)),
		"redis":             estado(s.cache.Ping(ctx)),
		"nodos_registrados": len(s.coord.Nodes),
		"muestras":          s.ds.Len(),
		"entrenando":        s.training.Load(),
		"uptime_seg":        int(time.Since(s.started).Seconds()),
	})
}

// clusterStatus devuelve el último estado que guardó el monitor en Redis;
// si no hay, consulta a los nodos en ese momento.
func (s *Server) clusterStatus(ctx context.Context) ([]cluster.NodeStatus, string) {
	if b, ok := s.cache.Get(ctx, "cluster:estado"); ok {
		var st []cluster.NodeStatus
		if json.Unmarshal(b, &st) == nil {
			return st, "cache"
		}
	}
	return s.coord.Status(ctx), "en_vivo"
}

func (s *Server) handleCluster(w http.ResponseWriter, r *http.Request) {
	st, fuente := s.clusterStatus(r.Context())
	if r.URL.Query().Get("en_vivo") == "1" {
		st, fuente = s.coord.Status(r.Context()), "en_vivo"
	}
	activos := 0
	for _, n := range st {
		if n.Alive {
			activos++
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"nodos": st, "activos": activos, "total": len(st), "fuente": fuente})
}

func (s *Server) handleMetricas(w http.ResponseWriter, r *http.Request) {
	ms, err := s.store.Metricas(r.Context(), r.URL.Query().Get("nodo"), intParam(r, "limite", 100, 2000))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, ms)
}

// monitor consulta el clúster cada cierto tiempo: guarda las métricas en
// MongoDB, deja el estado en Redis y lo envía por el WebSocket.
func (s *Server) monitor(ctx context.Context) {
	t := time.NewTicker(s.cfg.MonitorInterval)
	defer t.Stop()
	prev := map[string]bool{}
	for {
		st := s.coord.Status(ctx)
		now := time.Now()
		ms := make([]store.MetricaNodo, 0, len(st))
		for _, n := range st {
			m := store.MetricaNodo{Nodo: n.Addr, Activo: n.Alive, LatenciaMs: n.LatencyMs, Fecha: now}
			if n.Info != nil {
				m.Nombre, m.CPUPct, m.HeapMB = n.Info.Nombre, n.Info.CPUPct, n.Info.HeapMB
				m.Goroutines, m.Arboles = n.Info.Goroutines, n.Info.Arboles
			}
			ms = append(ms, m)
			if was, seen := prev[n.Addr]; seen && was != n.Alive {
				if n.Alive {
					go s.logEvent("info", "cluster", n.Addr+" volvió a responder")
				} else {
					go s.logEvent("alerta", "cluster", n.Addr+" dejó de responder")
				}
			}
			prev[n.Addr] = n.Alive
		}
		wctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		s.store.GuardarMetricas(wctx, ms)
		if b, err := json.Marshal(st); err == nil {
			s.cache.Set(wctx, "cluster:estado", b, 2*s.cfg.MonitorInterval)
		}
		cancel()
		s.hub.Broadcast(map[string]any{"tipo": "cluster", "nodos": st})
		select {
		case <-t.C:
		case <-ctx.Done():
			return
		}
	}
}

func (s *Server) handleEntrenamientos(w http.ResponseWriter, r *http.Request) {
	es, err := s.store.Entrenamientos(r.Context(), intParam(r, "limite", 20, 200))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, es)
}

func (s *Server) handleEntrenamiento(w http.ResponseWriter, r *http.Request) {
	e, err := s.store.Entrenamiento(r.Context(), r.PathValue("id"))
	if errors.Is(err, store.ErrNoEncontrado) {
		writeError(w, http.StatusNotFound, "entrenamiento no encontrado")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, e)
}

func (s *Server) handleModelos(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.modelList())
}

type predecirReq struct {
	Objetivo  string             `json:"objetivo"`
	Variables map[string]float64 `json:"variables"`
	Valores   []float64          `json:"valores"`
}

type predecirResp struct {
	Objetivo       string             `json:"objetivo"`
	Clase          int                `json:"clase"`
	Etiqueta       string             `json:"etiqueta"`
	Probabilidades map[string]float64 `json:"probabilidades"`
	Votos          []int              `json:"votos"`
	Fuente         string             `json:"fuente"` // cluster, local o cache
	ModeloID       string             `json:"modelo_id"`
	LatenciaMs     float64            `json:"latencia_ms"`
}

// handlePredecir clasifica un grupo de estudiantes. Primero busca en Redis;
// si no está, pide a los nodos que voten con sus árboles y suma los votos;
// si algún nodo ya no tiene el modelo, vota con el bosque guardado en la API.
func (s *Server) handlePredecir(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var req predecirReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "JSON inválido")
		return
	}
	if req.Objetivo == "" {
		req.Objetivo = "riesgo"
	}
	s.mu.RLock()
	m := s.models[req.Objetivo]
	s.mu.RUnlock()
	if m == nil {
		writeError(w, http.StatusNotFound, "no hay un modelo entrenado para "+req.Objetivo)
		return
	}
	x, err := featureVector(req)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	key := "pred:" + m.ID + ":" + hashFeatures(x)
	if b, ok := s.cache.Get(r.Context(), key); ok {
		var resp predecirResp
		if json.Unmarshal(b, &resp) == nil {
			resp.Fuente = "cache"
			resp.LatenciaMs = msSince(start)
			writeJSON(w, http.StatusOK, resp)
			return
		}
	}

	resp := predecirResp{Objetivo: m.Objetivo, ModeloID: m.ID, Fuente: "local"}
	var votes []int
	if len(m.Nodos) > 0 {
		ctx, cancel := context.WithTimeout(r.Context(), 80*time.Millisecond)
		_, vs, err := s.coord.Predict(ctx, m.Nodos, m.ID, [][]float64{x}, len(m.Clases), m.Arboles)
		cancel()
		if err == nil {
			votes, resp.Fuente = vs[0], "cluster"
		}
	}
	if votes == nil {
		votes = m.Forest.Votes(x)
	}
	resp.Votos = votes
	resp.Clase = argmax(votes)
	resp.Etiqueta = m.Clases[resp.Clase]
	resp.Probabilidades = map[string]float64{}
	for c, v := range votes {
		resp.Probabilidades[m.Clases[c]] = math.Round(float64(v)/float64(m.Arboles)*1000) / 1000
	}
	if b, err := json.Marshal(resp); err == nil {
		s.cache.Set(r.Context(), key, b, 30*time.Minute)
	}
	resp.LatenciaMs = msSince(start)
	writeJSON(w, http.StatusOK, resp)
}

func featureVector(req predecirReq) ([]float64, error) {
	names := dataset.FeatureNames
	if len(req.Valores) > 0 {
		if len(req.Valores) != len(names) {
			return nil, fmt.Errorf("valores debe tener %d números en este orden: %v", len(names), names)
		}
		return req.Valores, nil
	}
	if len(req.Variables) == 0 {
		return nil, fmt.Errorf("envía variables (objeto con %v) o valores", names)
	}
	x := make([]float64, len(names))
	for i, n := range names {
		v, ok := req.Variables[n]
		if !ok {
			return nil, fmt.Errorf("falta la variable %q", n)
		}
		x[i] = v
	}
	return x, nil
}

func hashFeatures(x []float64) string {
	h := sha1.New()
	for _, v := range x {
		h.Write([]byte(strconv.FormatFloat(v, 'g', -1, 64) + ","))
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

func (s *Server) currentModelID(objetivo string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if m := s.models[objetivo]; m != nil {
		return m.ID
	}
	return "sin-modelo"
}

// handleColegio devuelve el riesgo estimado de un colegio (todos sus anexos).
// La respuesta se guarda en Redis hasta que haya un modelo nuevo.
func (s *Server) handleColegio(w http.ResponseWriter, r *http.Request) {
	objetivo, err := objetivoParam(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	cod, err := strconv.Atoi(r.PathValue("codmod"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "cod_mod debe ser un número")
		return
	}
	key := fmt.Sprintf("colegio:%s:%s:%d", objetivo, s.currentModelID(objetivo), cod)
	if b, ok := s.cache.Get(r.Context(), key); ok {
		w.Header().Set("X-Cache", "HIT")
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Write(b)
		return
	}
	cs, err := s.store.Colegios(r.Context(), objetivo, cod)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if len(cs) == 0 {
		writeError(w, http.StatusNotFound, "no hay predicciones para ese colegio (¿ya se entrenó un modelo?)")
		return
	}
	b, _ := json.Marshal(cs)
	s.cache.Set(r.Context(), key, b, time.Hour)
	w.Header().Set("X-Cache", "MISS")
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(b)
}

// handleRanking lista los colegios con más alumnos en la clase más alta
// (riesgo alto o logro bajo), que es por donde una UGEL debería empezar.
func (s *Server) handleRanking(w http.ResponseWriter, r *http.Request) {
	objetivo, err := objetivoParam(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	limite := intParam(r, "limite", 20, 500)
	key := rankingKey(objetivo, s.currentModelID(objetivo), limite)
	if b, ok := s.cache.Get(r.Context(), key); ok {
		w.Header().Set("X-Cache", "HIT")
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.Write(b)
		return
	}
	cs, err := s.store.Ranking(r.Context(), objetivo, limite)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	b, _ := json.Marshal(cs)
	s.cache.Set(r.Context(), key, b, time.Hour)
	w.Header().Set("X-Cache", "MISS")
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Write(b)
}

func rankingKey(objetivo, modelID string, limite int) string {
	return fmt.Sprintf("ranking:%s:%s:%d", objetivo, modelID, limite)
}

// warmRanking deja en Redis los rankings más pedidos apenas hay un modelo
// nuevo, así la primera consulta del dashboard no tiene que ir a MongoDB.
func (s *Server) warmRanking(ctx context.Context, objetivo, modelID string) {
	for _, limite := range []int{10, 20, 50} {
		cs, err := s.store.Ranking(ctx, objetivo, limite)
		if err != nil {
			return
		}
		if b, err := json.Marshal(cs); err == nil {
			s.cache.Set(ctx, rankingKey(objetivo, modelID, limite), b, time.Hour)
		}
	}
}

func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	ls, err := s.store.Logs(r.Context(), intParam(r, "limite", 100, 1000))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, ls)
}

func (s *Server) handleEstadisticas(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"peticiones_por_ruta": s.cache.Counters(r.Context(), "api:peticiones"),
		"uptime_seg":          int(time.Since(s.started).Seconds()),
	})
}

func msSince(t time.Time) float64 { return math.Round(float64(time.Since(t).Microseconds())/10) / 100 }
