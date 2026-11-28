package api

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ShoterBroXD/EduForest/internal/dataset"
	"github.com/ShoterBroXD/EduForest/internal/store"
)

// Endpoints que agregó el TB2 para la aplicación web.

func (s *Server) handleUsuarios(w http.ResponseWriter, r *http.Request) {
	us, err := s.store.Usuarios(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, us)
}

// handleBuscar busca colegios por nombre, o por código modular si el texto
// es un número.
func (s *Server) handleBuscar(w http.ResponseWriter, r *http.Request) {
	objetivo, err := objetivoParam(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if len([]rune(q)) < 3 {
		writeError(w, http.StatusBadRequest, "escribe al menos 3 caracteres del nombre o el código modular")
		return
	}
	var cs []store.Colegio
	if cod, err := strconv.Atoi(q); err == nil {
		cs, err = s.store.Colegios(r.Context(), objetivo, cod)
	} else {
		cs, err = s.store.BuscarColegios(r.Context(), objetivo, q, intParam(r, "limite", 20, 100))
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, cs)
}

// handleImpacto responde la pregunta social del proyecto: si una UGEL visita
// los colegios en el orden del ranking, ¿a cuántos alumnos en riesgo alcanza?
func (s *Server) handleImpacto(w http.ResponseWriter, r *http.Request) {
	objetivo, err := objetivoParam(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	imp, fuente, err := s.impact(r.Context(), objetivo)
	if errors.Is(err, store.ErrNoEncontrado) {
		writeError(w, http.StatusNotFound, "todavía no hay predicciones por colegio: entrena un modelo primero")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("X-Cache", fuente)
	writeJSON(w, http.StatusOK, imp)
}

// impact busca el impacto en memoria, luego en Redis y, si no está, lo
// calcula a partir de los colegios guardados en MongoDB.
func (s *Server) impact(ctx context.Context, objetivo string) (*store.Impacto, string, error) {
	s.impactMu.Lock()
	imp := s.impacts[objetivo]
	s.impactMu.Unlock()
	if imp != nil {
		return imp, "MEM", nil
	}
	key := "impacto:" + objetivo
	if b, ok := s.cache.Get(ctx, key); ok {
		var cached store.Impacto
		if json.Unmarshal(b, &cached) == nil {
			s.setImpact(ctx, &cached, false)
			return &cached, "HIT", nil
		}
	}
	cs, err := s.store.Ranking(ctx, objetivo, math.MaxInt32)
	if err != nil {
		return nil, "", err
	}
	if len(cs) == 0 {
		return nil, "", store.ErrNoEncontrado
	}
	_, clases, _ := s.ds.Labels(objetivo)
	imp = computeImpact(cs, objetivo, cs[0].ModeloID, clases[len(clases)-1])
	s.setImpact(ctx, imp, true)
	return imp, "MISS", nil
}

func (s *Server) setImpact(ctx context.Context, imp *store.Impacto, toRedis bool) {
	s.impactMu.Lock()
	s.impacts[imp.Objetivo] = imp
	s.impactMu.Unlock()
	if toRedis {
		if b, err := json.Marshal(imp); err == nil {
			s.cache.Set(ctx, "impacto:"+imp.Objetivo, b, 24*time.Hour)
		}
	}
}

var pctCurva = []float64{1, 2, 5, 10, 15, 20, 30, 40, 50, 60, 70, 80, 90, 100}

// computeImpact arma el resumen de impacto. La curva compara tres formas de
// elegir qué colegios visitar primero: el ranking del modelo, el azar y el
// tamaño del colegio (visitar primero los más grandes, que es lo que haría
// una UGEL sin el modelo).
func computeImpact(cs []store.Colegio, objetivo, modeloID, claseAlta string) *store.Impacto {
	imp := &store.Impacto{Objetivo: objetivo, ModeloID: modeloID, ClaseAlta: claseAlta, Colegios: len(cs)}
	if len(cs) == 0 {
		return imp
	}
	alta := len(cs[0].AlumnosPorClase) - 1
	gest := map[string]*store.GrupoImpacto{}
	niv := map[string]*store.GrupoImpacto{}
	add := func(m map[string]*store.GrupoImpacto, k string, c store.Colegio) {
		g := m[k]
		if g == nil {
			g = &store.GrupoImpacto{Grupo: k}
			m[k] = g
		}
		g.Colegios++
		g.Alumnos += c.Alumnos
		g.AltaPredichos += c.AlumnosPorClase[alta]
		g.AltaReales += c.RealesPorClase[alta]
	}
	for _, c := range cs {
		imp.Alumnos += c.Alumnos
		imp.AltaPredichos += c.AlumnosPorClase[alta]
		imp.AltaReales += c.RealesPorClase[alta]
		if c.AlumnosPorClase[alta] > 0 {
			imp.ColegiosConAlta++
		}
		add(gest, c.Gestion, c)
		nivel := c.Nivel
		if nivel == "" {
			nivel = "Sin dato"
		}
		add(niv, nivel, c)
	}
	imp.PorGestion = sortedGroups(gest)
	imp.PorNivel = sortedGroups(niv)

	byModel := append([]store.Colegio(nil), cs...)
	sort.SliceStable(byModel, func(i, j int) bool { return byModel[i].AltaEsperados > byModel[j].AltaEsperados })
	bySize := append([]store.Colegio(nil), cs...)
	sort.SliceStable(bySize, func(i, j int) bool { return bySize[i].Alumnos > bySize[j].Alumnos })
	cumModel := cumulative(byModel, alta)
	cumSize := cumulative(bySize, alta)
	total := float64(imp.AltaReales)
	n := len(cs)
	for _, p := range pctCurva {
		k := int(math.Ceil(p / 100 * float64(n)))
		pt := store.PuntoCurva{PctColegios: p, Colegios: k, Azar: round1(100 * float64(k) / float64(n))}
		if total > 0 {
			pt.Modelo = round1(100 * float64(cumModel[k]) / total)
			pt.PorTamano = round1(100 * float64(cumSize[k]) / total)
		}
		imp.Curva = append(imp.Curva, pt)
	}
	return imp
}

// cumulative[k] = alumnos reales de la clase alta en los primeros k colegios.
func cumulative(cs []store.Colegio, alta int) []int {
	out := make([]int, len(cs)+1)
	for i, c := range cs {
		out[i+1] = out[i] + c.RealesPorClase[alta]
	}
	return out
}

func sortedGroups(m map[string]*store.GrupoImpacto) []store.GrupoImpacto {
	out := make([]store.GrupoImpacto, 0, len(m))
	for _, g := range m {
		out = append(out, *g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Alumnos > out[j].Alumnos })
	return out
}

func round1(v float64) float64 { return math.Round(v*10) / 10 }

// Variable describe una entrada del modelo para el formulario de simulación.
type Variable struct {
	Nombre      string    `json:"nombre"`
	Etiqueta    string    `json:"etiqueta"`
	Descripcion string    `json:"descripcion"`
	Tipo        string    `json:"tipo"` // entero, proporcion, binaria, categoria
	Min         float64   `json:"min"`
	Max         float64   `json:"max"`
	Mediana     float64   `json:"mediana"`
	Opciones    []Opcion  `json:"opciones,omitempty"`
	Percentiles []float64 `json:"percentiles,omitempty"` // p5 y p95
}

type Opcion struct {
	Valor    float64 `json:"valor"`
	Etiqueta string  `json:"etiqueta"`
}

var variableInfo = map[string]Variable{
	"anio":                   {Etiqueta: "Año", Descripcion: "Año escolar que se quiere predecir", Tipo: "entero"},
	"secundaria":             {Etiqueta: "Nivel", Descripcion: "Primaria o secundaria", Tipo: "categoria", Opciones: []Opcion{{0, "Primaria"}, {1, "Secundaria"}}},
	"gestion":                {Etiqueta: "Tipo de gestión", Descripcion: "Quién administra el colegio", Tipo: "categoria", Opciones: []Opcion{{0, "Pública de gestión directa"}, {1, "Privada"}, {2, "Pública de gestión privada"}}},
	"edad":                   {Etiqueta: "Edad", Descripcion: "Edad de los estudiantes del grupo", Tipo: "entero"},
	"alumnos_grupo":          {Etiqueta: "Alumnos del grupo", Descripcion: "Estudiantes del grupo: misma edad y tipo de discapacidad en el colegio", Tipo: "entero"},
	"pct_mujer":              {Etiqueta: "Proporción de mujeres", Descripcion: "Mujeres sobre el total del grupo", Tipo: "proporcion"},
	"pct_discapacidad":       {Etiqueta: "Proporción con discapacidad", Descripcion: "Estudiantes con discapacidad sobre el total", Tipo: "proporcion"},
	"tipo_disca_registrado":  {Etiqueta: "Discapacidad registrada", Descripcion: "El grupo tiene un tipo de discapacidad registrado", Tipo: "binaria", Opciones: []Opcion{{0, "No"}, {1, "Sí"}}},
	"pct_extranjero":         {Etiqueta: "Proporción de extranjeros", Descripcion: "Estudiantes extranjeros o venezolanos sobre el total", Tipo: "proporcion"},
	"pct_sin_dni":            {Etiqueta: "Proporción sin DNI validado", Descripcion: "Estudiantes sin DNI o con DNI sin validar", Tipo: "proporcion"},
	"pct_atraso":             {Etiqueta: "Proporción con atraso escolar", Descripcion: "Estudiantes con edad mayor a la de su grado", Tipo: "proporcion"},
	"alumnos_servicio":       {Etiqueta: "Alumnos del colegio", Descripcion: "Estudiantes de ese nivel en todo el colegio", Tipo: "entero"},
	"pct_atraso_servicio":    {Etiqueta: "Atraso en el colegio", Descripcion: "Proporción con atraso escolar en todo el colegio", Tipo: "proporcion"},
	"no_logro_servicio_prev": {Etiqueta: "No aprobación del colegio el año anterior", Descripcion: "Proporción que no aprobó el año anterior. -1 si el colegio no tiene año anterior", Tipo: "proporcion"},
	"retiro_servicio_prev":   {Etiqueta: "Retiro del colegio el año anterior", Descripcion: "Proporción de retirados el año anterior. -1 si el colegio no tiene año anterior", Tipo: "proporcion"},
}

// handleVariables entrega las 15 variables con sus rangos reales (calculados
// con los datos del año de prueba), para que la web arme el formulario.
func (s *Server) handleVariables(w http.ResponseWriter, r *http.Request) {
	s.varsOnce.Do(func() { s.vars = s.buildVariables() })
	writeJSON(w, http.StatusOK, s.vars)
}

func (s *Server) buildVariables() []Variable {
	names := dataset.FeatureNames
	vals := make([][]float64, len(names))
	for i, x := range s.ds.X {
		if s.ds.Year[i] != s.cfg.TestYear || i%5 != 0 { // una de cada 5 filas basta para los rangos
			continue
		}
		for f, v := range x {
			vals[f] = append(vals[f], v)
		}
	}
	out := make([]Variable, len(names))
	for f, name := range names {
		v := variableInfo[name]
		v.Nombre = name
		if v.Etiqueta == "" {
			v.Etiqueta = name
		}
		if xs := vals[f]; len(xs) > 0 {
			sort.Float64s(xs)
			q := func(p float64) float64 { return xs[int(p*float64(len(xs)-1))] }
			v.Min, v.Max, v.Mediana = xs[0], xs[len(xs)-1], q(0.5)
			v.Percentiles = []float64{q(0.05), q(0.95)}
		}
		out[f] = v
	}
	return out
}

func nivelNombre(secundaria float64) string {
	if secundaria == 1 {
		return "Secundaria"
	}
	return "Primaria"
}
