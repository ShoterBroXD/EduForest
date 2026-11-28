package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/ShoterBroXD/EduForest/internal/cluster"
	"github.com/ShoterBroXD/EduForest/internal/dataset"
	"github.com/ShoterBroXD/EduForest/internal/forest"
	"github.com/ShoterBroXD/EduForest/internal/metrics"
	"github.com/ShoterBroXD/EduForest/internal/store"
)

type entrenarReq struct {
	Objetivo        string   `json:"objetivo"`
	Modo            string   `json:"modo"`
	Arboles         int      `json:"arboles"`
	Profundidad     int      `json:"profundidad"`
	MinSamplesSplit int      `json:"min_samples_split"`
	Balance         *float64 `json:"balance"` // puntero para distinguir 0 de "no enviado"
	Semilla         int64    `json:"semilla"`
}

func (r *entrenarReq) defaults() error {
	if r.Objetivo == "" {
		r.Objetivo = "riesgo"
	}
	if r.Objetivo != "riesgo" && r.Objetivo != "logro" {
		return errors.New("objetivo inválido: usa riesgo o logro")
	}
	if r.Modo == "" {
		r.Modo = cluster.ModoSharding
	}
	if r.Modo != cluster.ModoSharding && r.Modo != cluster.ModoReplicado {
		return errors.New("modo inválido: usa sharding o replicado")
	}
	if r.Arboles == 0 {
		r.Arboles = 100
	}
	if r.Profundidad == 0 {
		r.Profundidad = 14
	}
	if r.MinSamplesSplit == 0 {
		r.MinSamplesSplit = 100
	}
	if r.Semilla == 0 {
		r.Semilla = 42
	}
	if r.Balance == nil {
		b := 0.5 // el valor que mejor funcionó en la PC3
		r.Balance = &b
	}
	switch {
	case r.Arboles < 1 || r.Arboles > 1000:
		return errors.New("arboles debe estar entre 1 y 1000")
	case r.Profundidad < 1 || r.Profundidad > 40:
		return errors.New("profundidad debe estar entre 1 y 40")
	case *r.Balance < 0 || *r.Balance > 1:
		return errors.New("balance debe estar entre 0 y 1")
	}
	return nil
}

// targetData lo prepara una sola vez por objetivo.
func (s *Server) targetData(objetivo string) (*targetData, error) {
	s.mu.RLock()
	td := s.data[objetivo]
	s.mu.RUnlock()
	if td != nil {
		return td, nil
	}
	labels, clases, err := s.ds.Labels(objetivo)
	if err != nil {
		return nil, err
	}
	sp, err := s.ds.SplitByYear(labels, s.cfg.TestYear)
	if err != nil {
		return nil, err
	}
	td = &targetData{clases: clases, split: sp, bins: forest.NewBinned(sp.Xtr, 64, s.cfg.Workers)}
	s.mu.Lock()
	s.data[objetivo] = td
	s.mu.Unlock()
	return td, nil
}

func (s *Server) handleEntrenar(w http.ResponseWriter, r *http.Request) {
	var req entrenarReq
	if r.ContentLength != 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "JSON inválido")
			return
		}
	}
	if err := req.defaults(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !s.training.CompareAndSwap(false, true) {
		writeError(w, http.StatusConflict, "ya hay un entrenamiento en curso")
		return
	}
	e := &store.Entrenamiento{
		ID:          fmt.Sprintf("%s-%s-%s", req.Objetivo, req.Modo, time.Now().Format("20060102-150405")),
		Objetivo:    req.Objetivo,
		Modo:        req.Modo,
		Estado:      "en_curso",
		Usuario:     claimsFrom(r).Subject,
		Arboles:     req.Arboles,
		Profundidad: req.Profundidad,
		Balance:     *req.Balance,
		Inicio:      time.Now(),
	}
	if err := s.store.GuardarEntrenamiento(r.Context(), e); err != nil {
		s.training.Store(false)
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	go s.runTraining(e, req, cluster.RequestID(r.Context()))
	writeJSON(w, http.StatusAccepted, map[string]any{
		"id": e.ID, "estado": e.Estado,
		"mensaje": "entrenamiento iniciado: sigue el progreso en /ws o en GET /api/entrenamientos/" + e.ID,
	})
}

// runTraining entrena en segundo plano, avisa por WebSocket y guarda modelo, métricas y riesgo.
func (s *Server) runTraining(e *store.Entrenamiento, req entrenarReq, reqID string) {
	defer s.training.Store(false)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	fail := func(err error) {
		now := time.Now()
		e.Estado, e.Error, e.Fin = "fallido", err.Error(), &now
		s.store.GuardarEntrenamiento(ctx, e)
		s.logEvent("error", "entrenamiento", fmt.Sprintf("%s falló: %v", e.ID, err))
		s.hub.Broadcast(map[string]any{"tipo": "entrenamiento_fin", "id": e.ID, "estado": e.Estado, "error": e.Error})
	}

	s.logEvent("info", "entrenamiento", fmt.Sprintf("%s iniciado por %s (%d árboles, modo %s, petición id=%s)", e.ID, e.Usuario, e.Arboles, e.Modo, reqID))
	s.hub.Broadcast(map[string]any{"tipo": "entrenamiento_inicio", "id": e.ID, "objetivo": e.Objetivo, "modo": e.Modo, "arboles": e.Arboles})

	td, err := s.targetData(req.Objetivo)
	if err != nil {
		fail(err)
		return
	}
	e.FilasEntrenamiento, e.FilasPrueba = len(td.split.Ytr), len(td.split.Yte)
	cfg := forest.Config{
		NumTrees: req.Arboles, MaxDepth: req.Profundidad, MinSamplesSplit: req.MinSamplesSplit,
		NumClasses: len(td.clases), Balance: *req.Balance, MaxBins: 64, Seed: req.Semilla,
	}
	job := cluster.TrainJob{ID: e.ID, Bins: td.bins, Y: td.split.Ytr, Config: cfg, Mode: req.Modo, Seed: req.Semilla}
	out, err := s.coord.Train(ctx, job, func(ev cluster.Event) {
		s.hub.Broadcast(map[string]any{"tipo": ev.Tipo, "id": e.ID, "evento": ev})
		if ev.Tipo == "nodo_caido" || ev.Tipo == "reasignacion" {
			msg := ev.Mensaje
			if ev.Nodo != "" {
				msg = ev.Nodo + ": " + msg
			}
			go s.logEvent("alerta", "cluster", e.ID+" | "+msg)
		}
	})
	if err != nil {
		fail(err)
		return
	}

	// Evaluación en el año de prueba con el bosque reunido.
	votes := votesBatch(out.Forest, td.split.Xte, s.cfg.Workers)
	pred := make([]int, len(votes))
	for i, v := range votes {
		pred[i] = argmax(v)
	}
	rep := metrics.Evaluate(td.split.Yte, pred, len(td.clases))

	model := &Model{
		ID: e.ID, Objetivo: e.Objetivo, Modo: e.Modo, Clases: td.clases, Nodos: out.Nodes,
		Arboles: len(out.Forest.Trees), Creado: time.Now(), Forest: out.Forest,
	}
	colegios := s.aggregateSchools(model, td, votes, pred)
	if err := s.store.GuardarColegios(ctx, e.Objetivo, colegios); err != nil {
		fail(fmt.Errorf("guardando colegios: %w", err))
		return
	}
	if err := s.saveModel(model); err != nil {
		log.Printf("[api] no se pudo guardar el modelo en disco: %v", err)
	}
	s.warmRanking(ctx, model.Objetivo, model.ID)
	imp := computeImpact(colegios, model.Objetivo, model.ID, td.clases[len(td.clases)-1])
	s.setImpact(ctx, imp, true)
	s.mu.Lock()
	s.models[model.Objetivo] = model
	s.mu.Unlock()

	now := time.Now()
	e.Estado, e.Fin = "completado", &now
	e.Nodos = out.Nodes
	e.CargaMs, e.EntrenamientoMs, e.TotalMs = out.CargaMs, out.TrainMs, out.TotalMs
	e.Reasignados, e.Reportes = out.Reasignados, out.Reports
	e.Impacto = imp
	e.Metricas = &store.Metricas{
		Clases: td.clases, Exactitud: rep.Accuracy, ExactitudBalanceada: rep.BalancedAcc, F1Macro: rep.MacroF1,
		Precision: rep.Precision, Recall: rep.Recall, F1: rep.F1, Matriz: rep.Confusion,
	}
	for i, v := range out.Forest.Importance {
		e.Importancia = append(e.Importancia, store.Importancia{Variable: dataset.FeatureNames[i], Valor: v})
	}
	sort.Slice(e.Importancia, func(a, b int) bool { return e.Importancia[a].Valor > e.Importancia[b].Valor })
	if err := s.store.GuardarEntrenamiento(ctx, e); err != nil {
		log.Printf("[api] no se pudo guardar el entrenamiento: %v", err)
	}
	s.logEvent("info", "entrenamiento", fmt.Sprintf("%s completado en %.1f s: F1 macro %.3f, exactitud balanceada %.3f, %d colegios",
		e.ID, out.TotalMs/1000, rep.MacroF1, rep.BalancedAcc, len(colegios)))
	s.hub.Broadcast(map[string]any{"tipo": "entrenamiento_fin", "id": e.ID, "estado": e.Estado, "metricas": e.Metricas, "total_ms": e.TotalMs})
}

// votesBatch calcula los votos de cada fila repartiendo las filas en rangos
// contiguos, uno por goroutine (cada una escribe solo en su rango).
func votesBatch(f *forest.Forest, X [][]float64, workers int) [][]int {
	out := make([][]int, len(X))
	var wg sync.WaitGroup
	n := len(X)
	for w := 0; w < workers; w++ {
		lo, hi := w*n/workers, (w+1)*n/workers
		if lo == hi {
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := lo; i < hi; i++ {
				out[i] = f.Votes(X[i])
			}
		}()
	}
	wg.Wait()
	return out
}

func argmax(v []int) int {
	best := 0
	for c := range v {
		if v[c] > v[best] {
			best = c
		}
	}
	return best
}

var gestiones = []string{"Pública de gestión directa", "Privada", "Pública de gestión privada", "Otra"}

// aggregateSchools junta las predicciones de los grupos de cada servicio
// educativo (cod_mod + anexo) del año de prueba.
func (s *Server) aggregateSchools(m *Model, td *targetData, votes [][]int, pred []int) []store.Colegio {
	type key struct{ cod, anexo int }
	k := len(m.Clases)
	alta := k - 1
	byKey := map[key]*store.Colegio{}
	probSum := map[key]float64{}
	for i, row := range td.split.TestRows {
		kk := key{s.ds.CodMod[row], s.ds.Anexo[row]}
		c := byKey[kk]
		if c == nil {
			x := s.ds.X[row]
			g := int(x[2])
			if g < 0 || g >= len(gestiones) {
				g = len(gestiones) - 1
			}
			c = &store.Colegio{
				ID: fmt.Sprintf("%s:%d-%d", m.Objetivo, kk.cod, kk.anexo), Objetivo: m.Objetivo,
				CodMod: kk.cod, Anexo: kk.anexo, Anio: s.ds.Year[row], Gestion: gestiones[g],
				Nombre: s.schoolName(kk.cod, kk.anexo), Nivel: nivelNombre(x[1]),
				AlumnosPorClase: make([]int, k), RealesPorClase: make([]int, k),
				ModeloID: m.ID, Actualizado: time.Now(),
			}
			byKey[kk] = c
		}
		alumnos := int(s.ds.X[row][4])
		c.Grupos++
		c.Alumnos += alumnos
		c.AlumnosPorClase[pred[i]] += alumnos
		c.RealesPorClase[td.split.Yte[i]] += alumnos
		probSum[kk] += float64(votes[i][alta]) / float64(m.Arboles) * float64(alumnos)
	}
	out := make([]store.Colegio, 0, len(byKey))
	for kk, c := range byKey {
		if c.Alumnos > 0 {
			c.ProbClaseAlta = probSum[kk] / float64(c.Alumnos)
		}
		c.AlumnosAlta = c.AlumnosPorClase[alta]
		c.AltaEsperados = math.Round(probSum[kk]*10) / 10
		out = append(out, *c)
	}
	return out
}

// Los modelos se guardan en disco (gob + metadatos en JSON) para que la API
// pueda predecir después de reiniciarse sin volver a entrenar.
func (s *Server) saveModel(m *Model) error {
	if s.cfg.ModelsDir == "" {
		return nil
	}
	if err := os.MkdirAll(s.cfg.ModelsDir, 0o755); err != nil {
		return err
	}
	base := filepath.Join(s.cfg.ModelsDir, m.Objetivo)
	if err := m.Forest.Save(base + ".gob"); err != nil {
		return err
	}
	meta, _ := json.MarshalIndent(m, "", "  ")
	return os.WriteFile(base+".json", meta, 0o644)
}

func (s *Server) loadModels() {
	if s.cfg.ModelsDir == "" {
		return
	}
	for _, obj := range []string{"riesgo", "logro"} {
		base := filepath.Join(s.cfg.ModelsDir, obj)
		f, err := forest.Load(base + ".gob")
		if err != nil {
			continue
		}
		m := &Model{ID: obj + "-pc3", Objetivo: obj, Modo: "local", Arboles: len(f.Trees), Forest: f}
		if b, err := os.ReadFile(base + ".json"); err == nil {
			json.Unmarshal(b, m)
			m.Forest = f
		}
		if m.Clases == nil {
			_, m.Clases, _ = s.ds.Labels(obj)
		}
		s.mu.Lock()
		s.models[obj] = m
		s.mu.Unlock()
		log.Printf("[api] modelo %s cargado desde disco (%d árboles)", m.ID, m.Arboles)
	}
}

func (s *Server) modelList() []*Model {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := []*Model{}
	for _, obj := range []string{"riesgo", "logro"} {
		if m := s.models[obj]; m != nil {
			out = append(out, m)
		}
	}
	return out
}
