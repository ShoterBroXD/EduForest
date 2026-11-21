package cluster

import (
	"context"
	"math/rand"
	"net"
	"reflect"
	"testing"

	"github.com/ShoterBroXD/EduForest/internal/forest"
)

func synthetic(n int, seed int64) ([][]float64, []int) {
	rng := rand.New(rand.NewSource(seed))
	X := make([][]float64, n)
	y := make([]int, n)
	for i := range X {
		a, b := rng.Float64(), rng.Float64()
		X[i] = []float64{a, b, rng.Float64(), float64(rng.Intn(4))}
		switch {
		case a > 0.95:
			y[i] = 2
		case b > 0.6:
			y[i] = 1
		}
	}
	return X, y
}

func testConfig() forest.Config {
	return forest.Config{NumTrees: 12, MaxDepth: 8, MinSamplesSplit: 10, NumClasses: 3, MaxFeatures: 2, MaxBins: 32, Seed: 7}
}

// startNodes levanta k nodos en puertos libres de localhost.
func startNodes(t *testing.T, k int) []string {
	t.Helper()
	addrs := make([]string, k)
	for i := range addrs {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { ln.Close() })
		go NewNode("nodo-test", 2).Serve(ln)
		addrs[i] = ln.Addr().String()
	}
	return addrs
}

func newJob(mode string) (TrainJob, [][]float64, []int) {
	X, y := synthetic(3000, 1)
	cfg := testConfig()
	bins := forest.NewBinned(X, cfg.MaxBins, 2)
	return TrainJob{ID: "test-" + mode, Bins: bins, Y: y, Config: cfg, Mode: mode, Seed: 3}, X, y
}

// En modo replicado el bosque debe ser idéntico al de la PC3 (misma semilla por árbol).
func TestReplicadoIgualQueCentralizado(t *testing.T) {
	job, X, y := newJob(ModoReplicado)
	local := forest.NewTrainer(X, y, job.Config, 2).TrainConcurrent(2)
	out, err := NewCoordinator(startNodes(t, 3)).Train(context.Background(), job, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(local.Trees, out.Forest.Trees) {
		t.Fatal("el bosque distribuido no coincide con el centralizado")
	}
	if len(out.Nodes) != 3 {
		t.Fatalf("se esperaban 3 nodos con árboles, hubo %d", len(out.Nodes))
	}
}

func TestShardingEntrenaYPredice(t *testing.T) {
	job, _, _ := newJob(ModoSharding)
	nodes := startNodes(t, 3)
	c := NewCoordinator(nodes)
	var progreso int
	out, err := c.Train(context.Background(), job, func(e Event) {
		if e.Tipo == "progreso" {
			progreso++
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if progreso != job.Config.NumTrees {
		t.Fatalf("se esperaban %d avisos de progreso, hubo %d", job.Config.NumTrees, progreso)
	}
	filas := 0
	for _, r := range out.Reports {
		filas += r.Filas
		if r.Arboles != 4 {
			t.Fatalf("%s construyó %d árboles, se esperaban 4", r.Nodo, r.Arboles)
		}
	}
	if filas != len(job.Y) {
		t.Fatalf("los fragmentos suman %d filas y el set tiene %d", filas, len(job.Y))
	}

	Xte, yte := synthetic(1000, 9)
	pred, _, err := c.Predict(context.Background(), out.Nodes, out.ModelID, Xte, 3, job.Config.NumTrees)
	if err != nil {
		t.Fatal(err)
	}
	ok := 0
	for i, p := range pred {
		if p != out.Forest.PredictOne(Xte[i]) {
			t.Fatalf("fila %d: la votación en los nodos difiere del bosque reunido", i)
		}
		if p == yte[i] {
			ok++
		}
	}
	if acc := float64(ok) / float64(len(pred)); acc < 0.9 {
		t.Fatalf("exactitud %.3f demasiado baja", acc)
	}
}

func TestSplitRowsEstratificado(t *testing.T) {
	_, y := synthetic(9000, 2)
	shards := splitRows(y, 3, 4, ModoSharding, 1)
	seen := make([]bool, len(y))
	for _, s := range shards {
		count := make([]int, 3)
		for _, r := range s {
			if seen[r] {
				t.Fatalf("la fila %d está en dos fragmentos", r)
			}
			seen[r] = true
			count[y[r]]++
		}
		total := make([]int, 3)
		for _, c := range y {
			total[c]++
		}
		for c := range count {
			if d := count[c] - total[c]/4; d < -1 || d > 1 {
				t.Fatalf("clase %d: el fragmento tiene %d filas, se esperaban ~%d", c, count[c], total[c]/4)
			}
		}
	}
	for r, ok := range seen {
		if !ok {
			t.Fatalf("la fila %d no quedó en ningún fragmento", r)
		}
	}
}

// flakyNode responde PING y LOAD pero corta la conexión cuando le piden
// entrenar, como un nodo que se cae a mitad del trabajo.
func flakyNode(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	real := NewNode("nodo-inestable", 1)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				_, enc, dec := newCodec(conn)
				var m Mensaje
				dec.Decode(&m)
				if m.Tipo == TipoTrain {
					conn.Close()
					return
				}
				resp := Mensaje{Tipo: TipoResult, Info: real.info()}
				enc.Encode(resp)
				conn.Close()
			}()
		}
	}()
	return ln.Addr().String()
}

func TestReasignaArbolesSiUnNodoCae(t *testing.T) {
	job, X, y := newJob(ModoReplicado)
	nodes := append(startNodes(t, 2), flakyNode(t))
	out, err := NewCoordinator(nodes).Train(context.Background(), job, nil)
	if err != nil {
		t.Fatal(err)
	}
	if out.Reasignados != 4 {
		t.Fatalf("se esperaban 4 árboles reasignados, hubo %d", out.Reasignados)
	}
	// Al ser replicado, aunque otro nodo construya esos árboles el resultado no cambia.
	local := forest.NewTrainer(X, y, job.Config, 2).TrainConcurrent(2)
	if !reflect.DeepEqual(local.Trees, out.Forest.Trees) {
		t.Fatal("el bosque con reasignación no coincide con el centralizado")
	}
	// Los nodos que recibieron árboles reasignados los suman a los que ya
	// tenían, así que entre los dos siguen votando con el bosque completo.
	pred, _, err := NewCoordinator(out.Nodes).Predict(context.Background(), out.Nodes, out.ModelID, X[:200], 3, job.Config.NumTrees)
	if err != nil {
		t.Fatal(err)
	}
	for i, p := range pred {
		if p != local.PredictOne(X[i]) {
			t.Fatalf("fila %d: la votación en los nodos difiere del bosque centralizado", i)
		}
	}
}

func TestSinNodos(t *testing.T) {
	job, _, _ := newJob(ModoSharding)
	c := NewCoordinator([]string{"127.0.0.1:1"})
	c.Retries = 0
	if _, err := c.Train(context.Background(), job, nil); err == nil {
		t.Fatal("se esperaba error sin nodos activos")
	}
}
