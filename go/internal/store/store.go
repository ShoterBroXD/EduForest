// Package store: MongoDB para persistencia, Redis como caché.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/ShoterBroXD/EduForest/internal/cluster"
)

var ErrNoEncontrado = errors.New("no encontrado")

const ErrInterrumpido = "interrumpido: la API se detuvo antes de terminar"

type Metricas struct {
	Clases              []string  `json:"clases" bson:"clases"`
	Exactitud           float64   `json:"exactitud" bson:"exactitud"`
	ExactitudBalanceada float64   `json:"exactitud_balanceada" bson:"exactitud_balanceada"`
	F1Macro             float64   `json:"f1_macro" bson:"f1_macro"`
	Precision           []float64 `json:"precision" bson:"precision"`
	Recall              []float64 `json:"recall" bson:"recall"`
	F1                  []float64 `json:"f1" bson:"f1"`
	Matriz              [][]int   `json:"matriz_confusion" bson:"matriz_confusion"`
}

type Importancia struct {
	Variable string  `json:"variable" bson:"variable"`
	Valor    float64 `json:"valor" bson:"valor"`
}

// Entrenamiento es una corrida del Random Forest distribuido.
type Entrenamiento struct {
	ID                 string               `json:"id" bson:"_id"`
	Objetivo           string               `json:"objetivo" bson:"objetivo"`
	Modo               string               `json:"modo" bson:"modo"`
	Estado             string               `json:"estado" bson:"estado"` // en_curso, completado, fallido
	Usuario            string               `json:"usuario" bson:"usuario"`
	Arboles            int                  `json:"arboles" bson:"arboles"`
	Profundidad        int                  `json:"profundidad" bson:"profundidad"`
	Balance            float64              `json:"balance" bson:"balance"`
	Nodos              []string             `json:"nodos" bson:"nodos"`
	FilasEntrenamiento int                  `json:"filas_entrenamiento" bson:"filas_entrenamiento"`
	FilasPrueba        int                  `json:"filas_prueba" bson:"filas_prueba"`
	CargaMs            float64              `json:"carga_ms" bson:"carga_ms"`
	EntrenamientoMs    float64              `json:"entrenamiento_ms" bson:"entrenamiento_ms"`
	TotalMs            float64              `json:"total_ms" bson:"total_ms"`
	Reasignados        int                  `json:"arboles_reasignados" bson:"arboles_reasignados"`
	Metricas           *Metricas            `json:"metricas,omitempty" bson:"metricas,omitempty"`
	Importancia        []Importancia        `json:"importancia,omitempty" bson:"importancia,omitempty"`
	Reportes           []cluster.NodeReport `json:"reportes,omitempty" bson:"reportes,omitempty"`
	Impacto            *Impacto             `json:"impacto,omitempty" bson:"impacto,omitempty"`
	Error              string               `json:"error,omitempty" bson:"error,omitempty"`
	Inicio             time.Time            `json:"inicio" bson:"inicio"`
	Fin                *time.Time           `json:"fin,omitempty" bson:"fin,omitempty"`
}

// MetricaNodo es una medición periódica de un nodo del clúster.
type MetricaNodo struct {
	Nodo       string    `json:"nodo" bson:"nodo"`
	Nombre     string    `json:"nombre" bson:"nombre"`
	Activo     bool      `json:"activo" bson:"activo"`
	LatenciaMs float64   `json:"latencia_ms" bson:"latencia_ms"`
	CPUPct     float64   `json:"cpu_pct" bson:"cpu_pct"`
	HeapMB     float64   `json:"heap_mb" bson:"heap_mb"`
	Goroutines int       `json:"goroutines" bson:"goroutines"`
	Arboles    int       `json:"arboles_entrenados" bson:"arboles_entrenados"`
	Fecha      time.Time `json:"fecha" bson:"fecha"`
}

// Colegio resume las predicciones de un servicio educativo (cod_mod + anexo)
// para el año de prueba.
type Colegio struct {
	ID              string  `json:"id" bson:"_id"` // objetivo:codmod-anexo
	Objetivo        string  `json:"objetivo" bson:"objetivo"`
	CodMod          int     `json:"cod_mod" bson:"cod_mod"`
	Anexo           int     `json:"anexo" bson:"anexo"`
	Nombre          string  `json:"nombre" bson:"nombre"`
	Nivel           string  `json:"nivel" bson:"nivel"` // Primaria o Secundaria
	Anio            int     `json:"anio" bson:"anio"`
	Gestion         string  `json:"gestion" bson:"gestion"`
	Grupos          int     `json:"grupos" bson:"grupos"`
	Alumnos         int     `json:"alumnos" bson:"alumnos"`
	AlumnosPorClase []int   `json:"alumnos_por_clase" bson:"alumnos_por_clase"` // según la predicción
	RealesPorClase  []int   `json:"alumnos_reales_por_clase" bson:"alumnos_reales_por_clase"`
	ProbClaseAlta   float64 `json:"prob_clase_alta" bson:"prob_clase_alta"` // promedio ponderado por alumnos
	AlumnosAlta     int     `json:"alumnos_clase_alta" bson:"alumnos_clase_alta"`
	// AltaEsperados = probabilidad de la clase alta × alumnos: cuántos alumnos
	// se espera que estén en grupos de la clase alta. Ordena el ranking.
	AltaEsperados float64   `json:"alumnos_alta_esperados" bson:"alumnos_alta_esperados"`
	ModeloID      string    `json:"modelo_id" bson:"modelo_id"`
	Actualizado   time.Time `json:"actualizado" bson:"actualizado"`
}

type Usuario struct {
	Nombre string    `json:"usuario" bson:"_id"`
	Hash   string    `json:"-" bson:"hash"`
	Sal    string    `json:"-" bson:"sal"`
	Rol    string    `json:"rol" bson:"rol"` // admin o analista
	Creado time.Time `json:"creado" bson:"creado"`
}

// Impacto resume a cuántos alumnos alcanza el modelo si una UGEL visita los
// colegios en el orden del ranking. Responde la pregunta social del proyecto.
type Impacto struct {
	Objetivo        string         `json:"objetivo" bson:"objetivo"`
	ModeloID        string         `json:"modelo_id" bson:"modelo_id"`
	ClaseAlta       string         `json:"clase_alta" bson:"clase_alta"` // Riesgo alto o Logro bajo
	Colegios        int            `json:"colegios" bson:"colegios"`
	Alumnos         int            `json:"alumnos" bson:"alumnos"`
	AltaPredichos   int            `json:"alumnos_alta_predichos" bson:"alumnos_alta_predichos"`
	AltaReales      int            `json:"alumnos_alta_reales" bson:"alumnos_alta_reales"`
	ColegiosConAlta int            `json:"colegios_con_alta" bson:"colegios_con_alta"`
	PorGestion      []GrupoImpacto `json:"por_gestion" bson:"por_gestion"`
	PorNivel        []GrupoImpacto `json:"por_nivel" bson:"por_nivel"`
	Curva           []PuntoCurva   `json:"curva" bson:"curva"`
}

type GrupoImpacto struct {
	Grupo         string `json:"grupo" bson:"grupo"`
	Colegios      int    `json:"colegios" bson:"colegios"`
	Alumnos       int    `json:"alumnos" bson:"alumnos"`
	AltaPredichos int    `json:"alumnos_alta_predichos" bson:"alumnos_alta_predichos"`
	AltaReales    int    `json:"alumnos_alta_reales" bson:"alumnos_alta_reales"`
}

// PuntoCurva: si se visita el PctColegios% de los colegios, qué porcentaje de
// los alumnos que de verdad estuvieron en la clase alta se alcanza, siguiendo
// el ranking del modelo, eligiendo colegios al azar o visitando primero los
// más grandes.
type PuntoCurva struct {
	PctColegios float64 `json:"pct_colegios" bson:"pct_colegios"`
	Colegios    int     `json:"colegios" bson:"colegios"`
	Modelo      float64 `json:"pct_modelo" bson:"pct_modelo"`
	Azar        float64 `json:"pct_azar" bson:"pct_azar"`
	PorTamano   float64 `json:"pct_por_tamano" bson:"pct_por_tamano"`
}

type Log struct {
	Fecha   time.Time `json:"fecha" bson:"fecha"`
	Nivel   string    `json:"nivel" bson:"nivel"` // info, alerta, error
	Origen  string    `json:"origen" bson:"origen"`
	Mensaje string    `json:"mensaje" bson:"mensaje"`
}

// Store es la base de datos persistente. Hay dos implementaciones: MongoDB
// (la de Docker Compose) y una en memoria para las pruebas.
type Store interface {
	Ping(ctx context.Context) error
	GuardarEntrenamiento(ctx context.Context, e *Entrenamiento) error
	Entrenamiento(ctx context.Context, id string) (*Entrenamiento, error)
	Entrenamientos(ctx context.Context, limite int) ([]Entrenamiento, error)
	// CerrarInterrumpidos marca como fallidos los entrenamientos que quedaron
	// en curso porque la API se detuvo antes de terminarlos.
	CerrarInterrumpidos(ctx context.Context) (int, error)
	GuardarMetricas(ctx context.Context, m []MetricaNodo) error
	Metricas(ctx context.Context, nodo string, limite int) ([]MetricaNodo, error)
	GuardarColegios(ctx context.Context, objetivo string, cs []Colegio) error
	Colegios(ctx context.Context, objetivo string, codMod int) ([]Colegio, error)
	Ranking(ctx context.Context, objetivo string, limite int) ([]Colegio, error)
	// BuscarColegios busca por parte del nombre, sin distinguir mayúsculas.
	BuscarColegios(ctx context.Context, objetivo, texto string, limite int) ([]Colegio, error)
	Usuario(ctx context.Context, nombre string) (*Usuario, error)
	GuardarUsuario(ctx context.Context, u *Usuario) error
	Usuarios(ctx context.Context) ([]Usuario, error)
	GuardarLog(ctx context.Context, l Log) error
	Logs(ctx context.Context, limite int) ([]Log, error)
	Close(ctx context.Context) error
}

// Cache guarda respuestas ya calculadas por un tiempo limitado.
type Cache interface {
	Ping(ctx context.Context) error
	Get(ctx context.Context, key string) ([]byte, bool)
	Set(ctx context.Context, key string, val []byte, ttl time.Duration)
	Incr(ctx context.Context, hash, field string)
	Counters(ctx context.Context, hash string) map[string]int64
	Close() error
}
