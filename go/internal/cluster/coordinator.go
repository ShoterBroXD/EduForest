package cluster

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ShoterBroXD/EduForest/internal/forest"
)

const (
	ModoSharding  = "sharding"  // cada nodo recibe solo su fragmento de filas
	ModoReplicado = "replicado" // cada nodo recibe todas las filas
)

// Coordinator habla por TCP con los nodos de cómputo. No guarda estado de
// los entrenamientos: cada llamada abre sus propias conexiones.
type Coordinator struct {
	Nodes       []string // direcciones host:puerto
	DialTimeout time.Duration
	Retries     int
}

func NewCoordinator(nodes []string) *Coordinator {
	return &Coordinator{Nodes: nodes, DialTimeout: 2 * time.Second, Retries: 3}
}

type NodeStatus struct {
	Addr      string    `json:"direccion"`
	Alive     bool      `json:"activo"`
	LatencyMs float64   `json:"latencia_ms"`
	Info      *NodeInfo `json:"info,omitempty"`
	Error     string    `json:"error,omitempty"`
	CheckedAt time.Time `json:"revisado"`
}

// call envía un mensaje a un nodo y lee respuestas hasta la final. Los
// mensajes PROGRESS que lleguen en el camino se pasan a onProgress.
func (c *Coordinator) call(ctx context.Context, addr string, req Mensaje, timeout time.Duration, retries int, onProgress func(Mensaje)) (Mensaje, *countingConn, error) {
	var conn net.Conn
	var err error
	for attempt := 0; attempt <= retries; attempt++ {
		d := net.Dialer{Timeout: c.DialTimeout}
		conn, err = d.DialContext(ctx, "tcp", addr)
		if err == nil {
			break
		}
		if attempt < retries {
			select {
			case <-time.After(time.Duration(attempt+1) * 200 * time.Millisecond):
			case <-ctx.Done():
				return Mensaje{}, nil, ctx.Err()
			}
		}
	}
	if err != nil {
		return Mensaje{}, nil, fmt.Errorf("no se pudo conectar con %s: %w", addr, err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(timeout))
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()

	cc, enc, dec := newCodec(conn)
	req.Origen = "coordinador"
	if err := enc.Encode(req); err != nil {
		return Mensaje{}, cc, fmt.Errorf("enviando %s a %s: %w", req.Tipo, addr, err)
	}
	for {
		var m Mensaje
		if err := dec.Decode(&m); err != nil {
			return Mensaje{}, cc, fmt.Errorf("leyendo respuesta de %s: %w", addr, err)
		}
		switch m.Tipo {
		case TipoProgress:
			if onProgress != nil {
				onProgress(m)
			}
		case TipoError:
			return m, cc, fmt.Errorf("%s respondió: %s", addr, m.Error)
		default:
			return m, cc, nil
		}
	}
}

// Status consulta a todos los nodos a la vez (una goroutine por nodo) y
// mide la latencia de ida y vuelta de cada PING.
func (c *Coordinator) Status(ctx context.Context) []NodeStatus {
	out := make([]NodeStatus, len(c.Nodes))
	var wg sync.WaitGroup
	for i, addr := range c.Nodes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st := NodeStatus{Addr: addr, CheckedAt: time.Now()}
			start := time.Now()
			m, _, err := c.call(ctx, addr, Mensaje{Tipo: TipoPing}, 3*time.Second, 0, nil)
			st.LatencyMs = float64(time.Since(start).Microseconds()) / 1000
			if err != nil {
				st.Error = err.Error()
			} else {
				st.Alive, st.Info = true, m.Info
			}
			out[i] = st // cada goroutine escribe solo su posición
		}()
	}
	wg.Wait()
	return out
}

// TrainJob describe un entrenamiento distribuido.
type TrainJob struct {
	ID     string
	Bins   *forest.Binned
	Y      []int
	Config forest.Config
	Mode   string
	Seed   int64 // semilla del reparto de filas entre nodos
}

type Event struct {
	Tipo    string `json:"tipo"` // carga, progreso, nodo_listo, nodo_caido, reasignacion
	Nodo    string `json:"nodo,omitempty"`
	Arbol   int    `json:"arbol,omitempty"`
	Hechos  int    `json:"hechos,omitempty"`
	Total   int    `json:"total,omitempty"`
	Mensaje string `json:"mensaje,omitempty"`
}

type NodeReport struct {
	Nodo            string  `json:"nodo"`
	Filas           int     `json:"filas"`
	Arboles         int     `json:"arboles"`
	BytesEnviados   int64   `json:"bytes_enviados"`
	BytesRecibidos  int64   `json:"bytes_recibidos"`
	CargaMs         float64 `json:"carga_ms"`
	EntrenamientoMs float64 `json:"entrenamiento_ms"` // desde que se envía TRAIN hasta recibir los árboles
	ComputoMs       float64 `json:"computo_ms"`       // lo que el nodo reporta que tardó en construirlos
	Workers         int     `json:"workers"`
	Fallo           string  `json:"fallo,omitempty"`
}

type TrainOutcome struct {
	Forest      *forest.Forest `json:"-"`
	ModelID     string         `json:"model_id"`
	Mode        string         `json:"modo"`
	Nodes       []string       `json:"nodos"` // nodos que guardan árboles del modelo
	CargaMs     float64        `json:"carga_ms"`
	TrainMs     float64        `json:"entrenamiento_ms"`
	TotalMs     float64        `json:"total_ms"`
	Reasignados int            `json:"arboles_reasignados"`
	Reports     []NodeReport   `json:"reportes"`
}

// Train: LOAD envía los datos (fragmento o copia), TRAIN asigna el árbol t al nodo t mod k
// y los árboles vuelven por un canal. Si un nodo cae, sus árboles se reasignan.
func (c *Coordinator) Train(ctx context.Context, job TrainJob, onEvent func(Event)) (*TrainOutcome, error) {
	if job.Mode != ModoSharding && job.Mode != ModoReplicado {
		return nil, fmt.Errorf("modo desconocido %q: usa %s o %s", job.Mode, ModoSharding, ModoReplicado)
	}
	var evMu sync.Mutex
	emit := func(e Event) {
		if onEvent != nil {
			evMu.Lock()
			onEvent(e)
			evMu.Unlock()
		}
	}
	start := time.Now()

	var alive []string
	for _, st := range c.Status(ctx) {
		if st.Alive {
			alive = append(alive, st.Addr)
		}
	}
	if len(alive) == 0 {
		return nil, errors.New("no hay nodos activos en el clúster")
	}

	// 1. Fan-out de datos.
	rows := splitRows(job.Y, job.Config.NumClasses, len(alive), job.Mode, job.Seed)
	reports := make([]NodeReport, len(alive))
	var full *Shard
	if job.Mode == ModoReplicado {
		full = makeShard(job, nil, job.ID+"-full")
	}
	var wg sync.WaitGroup
	for i, addr := range alive {
		wg.Add(1)
		go func() {
			defer wg.Done()
			shard := full
			if shard == nil {
				shard = makeShard(job, rows[i], fmt.Sprintf("%s-s%d", job.ID, i))
			}
			t := time.Now()
			_, cc, err := c.call(ctx, addr, Mensaje{Tipo: TipoLoad, Shard: shard}, 2*time.Minute, c.Retries, nil)
			r := NodeReport{Nodo: addr, Filas: len(shard.Y), CargaMs: msSince(t)}
			if cc != nil {
				r.BytesEnviados, r.BytesRecibidos = cc.out, cc.in
			}
			if err != nil {
				r.Fallo = err.Error()
				emit(Event{Tipo: "nodo_caido", Nodo: addr, Mensaje: err.Error()})
			} else {
				emit(Event{Tipo: "carga", Nodo: addr, Mensaje: fmt.Sprintf("%d filas", len(shard.Y))})
			}
			reports[i] = r
		}()
	}
	wg.Wait()
	cargaMs := msSince(start)

	datasetOf := make(map[string]string)
	var ready []int // índices en alive de los nodos que cargaron su fragmento
	for i, r := range reports {
		if r.Fallo == "" {
			ready = append(ready, i)
			if job.Mode == ModoReplicado {
				datasetOf[alive[i]] = job.ID + "-full"
			} else {
				datasetOf[alive[i]] = fmt.Sprintf("%s-s%d", job.ID, i)
			}
		}
	}
	if len(ready) == 0 {
		return nil, errors.New("ningún nodo pudo cargar su fragmento")
	}

	// 2. Fan-out de trabajo.
	nt := job.Config.NumTrees
	pending := make(map[int][]int) // índice de nodo -> árboles por construir
	for t := 0; t < nt; t++ {
		node := ready[t%len(ready)]
		pending[node] = append(pending[node], t)
	}

	type result struct {
		node   int
		trees  []int
		msg    Mensaje
		cc     *countingConn
		ms     float64
		err    error
		hechos int // árboles que el nodo alcanzó a avisar antes de terminar o caerse
	}
	trees := make([]*forest.Node, nt)
	importance := make([]float64, len(job.Bins.Edges))
	var done atomic.Int64
	trainStart := time.Now()
	reassigned := 0
	modelNodes := make(map[string]bool)

	for round := 0; len(pending) > 0; round++ {
		if round > len(alive) {
			return nil, errors.New("no se pudieron completar los árboles: se cayeron demasiados nodos")
		}
		results := make(chan result, len(pending))
		for node, list := range pending {
			go func() {
				addr := alive[node]
				task := &TrainTask{DatasetID: datasetOf[addr], ModelID: job.ID, Config: job.Config, Trees: list}
				t := time.Now()
				hechos := 0
				m, cc, err := c.call(ctx, addr, Mensaje{Tipo: TipoTrain, Train: task}, 15*time.Minute, c.Retries, func(p Mensaje) {
					hechos++
					emit(Event{Tipo: "progreso", Nodo: addr, Arbol: p.Arbol, Hechos: int(done.Add(1)), Total: nt})
				})
				results <- result{node: node, trees: list, msg: m, cc: cc, ms: msSince(t), err: err, hechos: hechos}
			}()
		}

		var failed []int
		for range len(pending) {
			r := <-results
			rep := &reports[r.node]
			if r.cc != nil {
				rep.BytesEnviados += r.cc.out
				rep.BytesRecibidos += r.cc.in
			}
			if r.err == nil && (r.msg.Result == nil || len(r.msg.Result.Trees) != len(r.trees)) {
				r.err = fmt.Errorf("%s devolvió una respuesta incompleta", alive[r.node])
			}
			if r.err != nil {
				rep.Fallo = r.err.Error()
				failed = append(failed, r.trees...)
				done.Add(-int64(r.hechos)) // se volverán a contar al reconstruirlos
				emit(Event{Tipo: "nodo_caido", Nodo: alive[r.node], Mensaje: r.err.Error()})
				continue
			}
			for k, t := range r.trees {
				trees[t] = r.msg.Result.Trees[k]
			}
			for f, v := range r.msg.Result.Importance {
				importance[f] += v
			}
			rep.Arboles += len(r.trees)
			rep.EntrenamientoMs += r.ms
			rep.ComputoMs += r.msg.Result.Millis
			rep.Workers = r.msg.Result.Workers
			modelNodes[alive[r.node]] = true
			emit(Event{Tipo: "nodo_listo", Nodo: alive[r.node], Mensaje: fmt.Sprintf("%d árboles", len(r.trees))})
		}

		pending = make(map[int][]int)
		if len(failed) == 0 {
			break
		}
		var healthy []int
		for _, i := range ready {
			if reports[i].Fallo == "" {
				healthy = append(healthy, i)
			}
		}
		if len(healthy) == 0 {
			return nil, errors.New("todos los nodos fallaron durante el entrenamiento")
		}
		sort.Ints(failed)
		for k, t := range failed {
			node := healthy[k%len(healthy)]
			pending[node] = append(pending[node], t)
		}
		reassigned += len(failed)
		emit(Event{Tipo: "reasignacion", Mensaje: fmt.Sprintf("%d árboles reasignados a %d nodos", len(failed), len(healthy))})
	}

	out := &TrainOutcome{
		Forest:      forest.NewForest(trees, job.Config.NumClasses, importance),
		ModelID:     job.ID,
		Mode:        job.Mode,
		CargaMs:     cargaMs,
		TrainMs:     msSince(trainStart),
		TotalMs:     msSince(start),
		Reasignados: reassigned,
		Reports:     reports,
	}
	for _, addr := range alive {
		if modelNodes[addr] {
			out.Nodes = append(out.Nodes, addr)
		}
	}
	return out, nil
}

// ErrIncompleto indica que los nodos ya no tienen todos los árboles del
// modelo (por ejemplo, porque uno se reinició).
var ErrIncompleto = errors.New("los nodos no tienen el modelo completo")

// Predict pide a cada nodo que vote con sus árboles y suma los votos
// (fan-in). Devuelve la clase ganadora y los votos de cada fila.
func (c *Coordinator) Predict(ctx context.Context, nodes []string, modelID string, X [][]float64, numClasses, totalTrees int) ([]int, [][]int, error) {
	type res struct {
		v   *PredictVotes
		err error
	}
	ch := make(chan res, len(nodes))
	for _, addr := range nodes {
		go func() {
			m, _, err := c.call(ctx, addr, Mensaje{Tipo: TipoPredict, Predict: &PredictTask{ModelID: modelID, X: X, ReqID: RequestID(ctx)}}, 5*time.Second, 1, nil)
			ch <- res{m.Votes, err}
		}()
	}
	votes := make([][]int, len(X))
	for i := range votes {
		votes[i] = make([]int, numClasses)
	}
	counted := 0
	var firstErr error
	for range nodes {
		r := <-ch
		if r.err != nil || r.v == nil || len(r.v.Votes) != len(X) {
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		counted += r.v.Trees
		for i, row := range r.v.Votes {
			for cl, n := range row {
				votes[i][cl] += n
			}
		}
	}
	if counted != totalTrees {
		if firstErr != nil {
			return nil, nil, fmt.Errorf("%w: %v", ErrIncompleto, firstErr)
		}
		return nil, nil, ErrIncompleto
	}
	pred := make([]int, len(X))
	for i, v := range votes {
		for cl := range v {
			if v[cl] > v[pred[i]] {
				pred[i] = cl
			}
		}
	}
	return pred, votes, nil
}

// splitRows reparte las filas entre k nodos; en sharding estratifica por clase
// para que ningún nodo se quede sin casos de riesgo alto (solo el 2%).
func splitRows(y []int, numClasses, k int, mode string, seed int64) [][]int {
	if mode == ModoReplicado {
		return nil
	}
	byClass := make([][]int, numClasses)
	for i, c := range y {
		byClass[c] = append(byClass[c], i)
	}
	rng := rand.New(rand.NewSource(seed))
	shards := make([][]int, k)
	turn := 0
	for _, rows := range byClass {
		rng.Shuffle(len(rows), func(a, b int) { rows[a], rows[b] = rows[b], rows[a] })
		for _, r := range rows {
			shards[turn%k] = append(shards[turn%k], r)
			turn++
		}
	}
	for _, s := range shards {
		sort.Ints(s) // en orden de fila, para recorrer la memoria de forma secuencial
	}
	return shards
}

// makeShard copia las filas indicadas (todas si rows es nil) al formato
// que viaja por la red.
func makeShard(job TrainJob, rows []int, id string) *Shard {
	b := job.Bins
	s := &Shard{DatasetID: id, NumClasses: job.Config.NumClasses, Edges: edgesToWire(b.Edges), Cols: make([][]byte, len(b.Cols))}
	if rows == nil {
		for f, col := range b.Cols {
			s.Cols[f] = col
		}
		s.Y = make([]byte, len(job.Y))
		for i, c := range job.Y {
			s.Y[i] = byte(c)
		}
		return s
	}
	for f, col := range b.Cols {
		sub := make([]byte, len(rows))
		for k, r := range rows {
			sub[k] = col[r]
		}
		s.Cols[f] = sub
	}
	s.Y = make([]byte, len(rows))
	for k, r := range rows {
		s.Y[k] = byte(job.Y[r])
	}
	return s
}

func msSince(t time.Time) float64 { return float64(time.Since(t).Microseconds()) / 1000 }
