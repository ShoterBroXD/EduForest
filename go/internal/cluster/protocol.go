// Package cluster: Random Forest distribuido. El coordinador reparte árboles a los nodos
// (fan-out) y los junta (fan-in) por TCP con JSON.
package cluster

import (
	"encoding/json"
	"io"
	"math"

	"github.com/ShoterBroXD/EduForest/internal/forest"
)

// Tipos de mensaje.
const (
	TipoPing     = "PING"     // coordinador -> nodo: estado y métricas
	TipoLoad     = "LOAD"     // coordinador -> nodo: fragmento de datos
	TipoTrain    = "TRAIN"    // coordinador -> nodo: árboles a construir
	TipoPredict  = "PREDICT"  // coordinador -> nodo: filas a votar
	TipoProgress = "PROGRESS" // nodo -> coordinador: terminó un árbol
	TipoResult   = "RESULT"   // nodo -> coordinador: respuesta final
	TipoError    = "ERROR"
)

// Mensaje es el contrato entre el coordinador y los nodos. Solo se llena el
// campo que corresponde a cada tipo.
type Mensaje struct {
	Tipo    string        `json:"tipo"`
	Origen  string        `json:"origen"`
	Error   string        `json:"error,omitempty"`
	Info    *NodeInfo     `json:"info,omitempty"`
	Shard   *Shard        `json:"shard,omitempty"`
	Train   *TrainTask    `json:"train,omitempty"`
	Result  *TrainResult  `json:"result,omitempty"`
	Predict *PredictTask  `json:"predict,omitempty"`
	Votes   *PredictVotes `json:"votes,omitempty"`
	Arbol   int           `json:"arbol,omitempty"` // PROGRESS: número del árbol terminado
}

// Shard: fragmento discretizado, una columna []byte por feature (~4 MB por 190k filas).
type Shard struct {
	DatasetID  string      `json:"dataset_id"`
	NumClasses int         `json:"num_clases"`
	Cols       [][]byte    `json:"cols"`
	Edges      [][]float64 `json:"edges"` // sin el +Inf final, que JSON no admite
	Y          []byte      `json:"y"`
}

type TrainTask struct {
	DatasetID string        `json:"dataset_id"`
	ModelID   string        `json:"model_id"`
	Config    forest.Config `json:"config"`
	Trees     []int         `json:"arboles"` // números globales de los árboles a construir
}

type TrainResult struct {
	Trees      []*forest.Node `json:"arboles,omitempty"`
	Importance []float64      `json:"importancia,omitempty"`
	Rows       int            `json:"filas"`
	Workers    int            `json:"workers"`
	Millis     float64        `json:"ms"` // tiempo de cómputo dentro del nodo
}

type PredictTask struct {
	ModelID string      `json:"model_id"`
	X       [][]float64 `json:"x"`
}

// PredictVotes son los votos de los árboles de un nodo: Votes[fila][clase].
type PredictVotes struct {
	Votes [][]int `json:"votos"`
	Trees int     `json:"arboles"`
}

// NodeInfo es lo que un nodo reporta en cada PING.
type NodeInfo struct {
	Nombre     string  `json:"nombre"`
	Workers    int     `json:"workers"`
	NumCPU     int     `json:"num_cpu"`
	Goroutines int     `json:"goroutines"`
	HeapMB     float64 `json:"heap_mb"`
	CPUPct     float64 `json:"cpu_pct"` // uso de CPU desde el PING anterior (-1 si no se puede medir)
	Datasets   int     `json:"datasets"`
	Modelos    int     `json:"modelos"`
	Arboles    int     `json:"arboles_entrenados"`
	UptimeSeg  float64 `json:"uptime_seg"`
}

func edgesToWire(edges [][]float64) [][]float64 {
	out := make([][]float64, len(edges))
	for f, e := range edges {
		out[f] = e[:len(e)-1]
	}
	return out
}

func edgesFromWire(edges [][]float64) [][]float64 {
	out := make([][]float64, len(edges))
	for f, e := range edges {
		out[f] = append(append(make([]float64, 0, len(e)+1), e...), math.Inf(1))
	}
	return out
}

// countingConn cuenta los bytes que pasan por la conexión para medir el
// costo de red de cada modo de distribución.
type countingConn struct {
	rw      io.ReadWriter
	in, out int64
}

func (c *countingConn) Read(p []byte) (int, error) {
	n, err := c.rw.Read(p)
	c.in += int64(n)
	return n, err
}

func (c *countingConn) Write(p []byte) (int, error) {
	n, err := c.rw.Write(p)
	c.out += int64(n)
	return n, err
}

func newCodec(rw io.ReadWriter) (*countingConn, *json.Encoder, *json.Decoder) {
	cc := &countingConn{rw: rw}
	return cc, json.NewEncoder(cc), json.NewDecoder(cc)
}
