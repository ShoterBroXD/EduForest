package cluster

import (
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ShoterBroXD/EduForest/internal/forest"
)

// Node es un nodo de cómputo del clúster. Guarda en memoria los fragmentos
// que le envió el coordinador y los árboles que entrenó, y atiende cada
// conexión TCP en su propia goroutine.
type Node struct {
	Name    string
	Workers int

	mu       sync.RWMutex // protege datasets, models y trained
	datasets map[string]*nodeData
	models   map[string]*nodeModel
	trained  int
	order    []string // claves en orden de llegada, para no guardar más de maxGuardados

	trainSem chan struct{} // semáforo: un entrenamiento a la vez por nodo
	started  time.Time

	cpuMu        sync.Mutex
	lastCPU      float64
	lastCPUCheck time.Time
}

// nodeModel guarda los árboles de un modelo por su número global. Si el
// coordinador reasigna árboles de un nodo caído, se suman a los que ya había.
type nodeModel struct {
	numClasses int
	trees      map[int]*forest.Node
}

type nodeData struct {
	bins       *forest.Binned
	y          []int
	numClasses int
}

func NewNode(name string, workers int) *Node {
	if workers < 1 {
		workers = runtime.GOMAXPROCS(0)
	}
	n := &Node{
		Name:     name,
		Workers:  workers,
		datasets: make(map[string]*nodeData),
		models:   make(map[string]*nodeModel),
		trainSem: make(chan struct{}, 1),
		started:  time.Now(),
	}
	n.lastCPU, n.lastCPUCheck = cpuSeconds(), time.Now()
	return n
}

// Serve acepta conexiones hasta que se cierre el listener.
func (n *Node) Serve(ln net.Listener) error {
	log.Printf("[%s] nodo ML escuchando en %s con %d workers", n.Name, ln.Addr(), n.Workers)
	for {
		conn, err := ln.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return nil
			}
			log.Printf("[%s] error aceptando conexión: %v", n.Name, err)
			continue
		}
		go n.handle(conn)
	}
}

func (n *Node) handle(conn net.Conn) {
	defer conn.Close()
	_, enc, dec := newCodec(conn)
	var req Mensaje
	if err := dec.Decode(&req); err != nil {
		log.Printf("[%s] mensaje inválido de %s: %v", n.Name, conn.RemoteAddr(), err)
		return
	}
	var encMu sync.Mutex // TRAIN escribe PROGRESS desde varios workers
	send := func(m Mensaje) error {
		m.Origen = n.Name
		encMu.Lock()
		defer encMu.Unlock()
		return enc.Encode(m)
	}

	var resp Mensaje
	var err error
	switch req.Tipo {
	case TipoPing:
		resp = Mensaje{Tipo: TipoResult, Info: n.info()}
	case TipoLoad:
		resp, err = n.load(req.Shard)
	case TipoTrain:
		resp, err = n.train(req.Train, send)
	case TipoPredict:
		resp, err = n.predict(req.Predict)
	default:
		err = fmt.Errorf("tipo de mensaje desconocido %q", req.Tipo)
	}
	if err != nil {
		log.Printf("[%s] %s falló: %v", n.Name, req.Tipo, err)
		resp = Mensaje{Tipo: TipoError, Error: err.Error()}
	}
	if err := send(resp); err != nil {
		log.Printf("[%s] no se pudo responder a %s: %v", n.Name, req.Origen, err)
	}
}

func (n *Node) load(s *Shard) (Mensaje, error) {
	if s == nil || len(s.Cols) == 0 {
		return Mensaje{}, errors.New("LOAD sin datos")
	}
	rows := len(s.Y)
	for f, col := range s.Cols {
		if len(col) != rows {
			return Mensaje{}, fmt.Errorf("la columna %d tiene %d filas y se esperaban %d", f, len(col), rows)
		}
	}
	y := make([]int, rows)
	for i, c := range s.Y {
		if int(c) >= s.NumClasses {
			return Mensaje{}, fmt.Errorf("clase %d fuera de rango en la fila %d", c, i)
		}
		y[i] = int(c)
	}
	bins := &forest.Binned{Cols: make([][]uint8, len(s.Cols)), Edges: edgesFromWire(s.Edges), N: rows}
	for f, col := range s.Cols {
		bins.Cols[f] = col
	}
	n.mu.Lock()
	n.datasets[s.DatasetID] = &nodeData{bins: bins, y: y, numClasses: s.NumClasses}
	n.remember("d:" + s.DatasetID)
	n.mu.Unlock()
	log.Printf("[%s] fragmento %s cargado: %d filas", n.Name, s.DatasetID, rows)
	return Mensaje{Tipo: TipoResult, Result: &TrainResult{Rows: rows}}, nil
}

func (n *Node) train(t *TrainTask, send func(Mensaje) error) (Mensaje, error) {
	if t == nil || len(t.Trees) == 0 {
		return Mensaje{}, errors.New("TRAIN sin árboles")
	}
	n.mu.RLock()
	data := n.datasets[t.DatasetID]
	n.mu.RUnlock()
	if data == nil {
		return Mensaje{}, fmt.Errorf("el fragmento %s no está cargado", t.DatasetID)
	}

	n.trainSem <- struct{}{}
	defer func() { <-n.trainSem }()

	start := time.Now()
	log.Printf("[%s] entrenando %d árboles sobre %d filas", n.Name, len(t.Trees), len(data.y))
	trainer := forest.NewTrainerFromBins(data.bins, data.y, t.Config)
	trees, imp := trainer.TrainTrees(t.Trees, n.Workers, func(treeIdx int) {
		send(Mensaje{Tipo: TipoProgress, Arbol: treeIdx})
	})
	ms := float64(time.Since(start).Microseconds()) / 1000

	n.mu.Lock()
	m := n.models[t.ModelID]
	if m == nil {
		m = &nodeModel{numClasses: t.Config.NumClasses, trees: map[int]*forest.Node{}}
		n.models[t.ModelID] = m
	}
	for k, idx := range t.Trees {
		m.trees[idx] = trees[k]
	}
	n.remember("m:" + t.ModelID)
	n.trained += len(trees)
	n.mu.Unlock()
	log.Printf("[%s] %d árboles listos en %.0f ms", n.Name, len(trees), ms)
	return Mensaje{Tipo: TipoResult, Result: &TrainResult{
		Trees: trees, Importance: imp, Rows: len(data.y), Workers: n.Workers, Millis: ms,
	}}, nil
}

// predict hace que los árboles del nodo voten por cada fila. El coordinador
// suma los votos de todos los nodos y elige la clase con más votos.
func (n *Node) predict(p *PredictTask) (Mensaje, error) {
	if p == nil {
		return Mensaje{}, errors.New("PREDICT sin filas")
	}
	n.mu.RLock()
	m := n.models[p.ModelID]
	var f *forest.Forest
	if m != nil {
		f = &forest.Forest{NumClasses: m.numClasses}
		for _, t := range m.trees {
			f.Trees = append(f.Trees, t)
		}
	}
	n.mu.RUnlock()
	if f == nil {
		return Mensaje{}, fmt.Errorf("el modelo %s no está en este nodo", p.ModelID)
	}
	votes := make([][]int, len(p.X))
	for i, x := range p.X {
		votes[i] = f.Votes(x)
	}
	return Mensaje{Tipo: TipoResult, Votes: &PredictVotes{Votes: votes, Trees: len(f.Trees)}}, nil
}

const maxGuardados = 6

// remember registra una clave y, si hay demasiadas, libera la más antigua.
// Se llama con n.mu tomado.
func (n *Node) remember(key string) {
	for i, k := range n.order {
		if k == key {
			n.order = append(n.order[:i], n.order[i+1:]...)
			break
		}
	}
	n.order = append(n.order, key)
	for len(n.order) > maxGuardados {
		old := n.order[0]
		n.order = n.order[1:]
		if strings.HasPrefix(old, "d:") {
			delete(n.datasets, old[2:])
		} else {
			delete(n.models, old[2:])
		}
	}
}

func (n *Node) info() *NodeInfo {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	n.mu.RLock()
	defer n.mu.RUnlock()
	return &NodeInfo{
		Nombre:     n.Name,
		Workers:    n.Workers,
		NumCPU:     runtime.NumCPU(),
		Goroutines: runtime.NumGoroutine(),
		HeapMB:     float64(m.HeapAlloc) / (1 << 20),
		CPUPct:     n.cpuPercent(),
		Datasets:   len(n.datasets),
		Modelos:    len(n.models),
		Arboles:    n.trained,
		UptimeSeg:  time.Since(n.started).Seconds(),
	}
}

// cpuPercent es el uso de CPU del proceso desde la consulta anterior, como
// porcentaje del total de núcleos que puede usar el nodo.
func (n *Node) cpuPercent() float64 {
	n.cpuMu.Lock()
	defer n.cpuMu.Unlock()
	now, cpu := time.Now(), cpuSeconds()
	if cpu < 0 {
		return -1
	}
	wall := now.Sub(n.lastCPUCheck).Seconds()
	pct := 0.0
	if wall > 0 {
		pct = 100 * (cpu - n.lastCPU) / wall / float64(runtime.GOMAXPROCS(0))
	}
	n.lastCPU, n.lastCPUCheck = cpu, now
	return pct
}

// cpuSeconds lee el tiempo de CPU del proceso en /proc (Linux, es decir,
// dentro del contenedor). En otros sistemas devuelve -1.
func cpuSeconds() float64 {
	b, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return -1
	}
	s := string(b)
	fields := strings.Fields(s[strings.LastIndexByte(s, ')')+1:])
	if len(fields) < 13 {
		return -1
	}
	utime, _ := strconv.ParseFloat(fields[11], 64)
	stime, _ := strconv.ParseFloat(fields[12], 64)
	return (utime + stime) / 100 // ticks de reloj (USER_HZ = 100)
}
