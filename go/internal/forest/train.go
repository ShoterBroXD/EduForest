package forest

import (
	"fmt"
	"sync"
	"time"
)

// Trainer prepara una vez lo que comparten todos los árboles: el set
// discretizado y las filas de cada clase. Después de NewTrainer todo es de
// solo lectura, por eso varias goroutines pueden construir árboles a la vez
// sin bloquearse.
type Trainer struct {
	Bins    *Binned
	y       []int
	byClass [][]int
	cfg     Config
}

func NewTrainer(X [][]float64, y []int, cfg Config, workers int) *Trainer {
	if cfg.MaxBins == 0 {
		cfg.MaxBins = 64
	}
	if cfg.SampleSize <= 0 {
		cfg.SampleSize = len(y)
	}
	byClass := make([][]int, cfg.NumClasses)
	for i, c := range y {
		byClass[c] = append(byClass[c], i)
	}
	return &Trainer{
		Bins:    NewBinned(X, cfg.MaxBins, workers),
		y:       y,
		byClass: byClass,
		cfg:     cfg,
	}
}

func (t *Trainer) Config() Config { return t.cfg }

// treeSeed da a cada árbol su propia semilla según su número, no según el
// worker que lo construye. Así el bosque sale idéntico con 1 o con 20 workers,
// y se puede comprobar que la versión concurrente no cambia el resultado.
func (t *Trainer) treeSeed(treeIdx int) int64 {
	return t.cfg.Seed + int64(treeIdx)*7919
}

// workspace son los buffers grandes que necesita un árbol (las filas del
// bootstrap y el buffer de partición). Cada worker crea uno y lo reutiliza en
// todos sus árboles: sin esto, cada árbol reservaba ~6 MB nuevos y el
// recolector de basura (GC) competía por CPU con los workers.
type workspace struct {
	idx, scratch []int
	counts       []uint32 // para ordenar idx por conteo
}

func (t *Trainer) newWorkspace() *workspace {
	return &workspace{
		idx:     make([]int, t.cfg.SampleSize),
		scratch: make([]int, t.cfg.SampleSize),
		counts:  make([]uint32, len(t.y)),
	}
}

// sortByCounting ordena idx en O(n) contando cuántas veces salió cada fila
// (counting sort). Es mucho más barato que un ordenamiento por comparación
// porque los valores son posiciones entre 0 y len(counts)-1.
func sortByCounting(idx []int, counts []uint32) {
	clear(counts)
	for _, v := range idx {
		counts[v]++
	}
	k := 0
	for v, c := range counts {
		for ; c > 0; c-- {
			idx[k] = v
			k++
		}
	}
}

func (t *Trainer) buildOne(treeIdx int, ws *workspace) (*Node, []float64) {
	tb := newTreeBuilder(t.Bins, t.y, t.cfg, t.treeSeed(treeIdx))
	tb.scratch = ws.scratch
	bootstrap(ws.idx, len(t.y), t.byClass, t.cfg.Balance, tb.rng)
	// Ordenar las filas sorteadas hace que los histogramas lean la memoria de
	// forma secuencial en vez de saltar al azar entre 800 mil posiciones.
	sortByCounting(ws.idx, ws.counts)
	return tb.build(ws.idx, 0), tb.importance
}

func (t *Trainer) newForest() *Forest {
	return &Forest{
		Trees:      make([]*Node, t.cfg.NumTrees),
		NumClasses: t.cfg.NumClasses,
		Importance: make([]float64, len(t.Bins.Edges)),
	}
}

// TrainSequential construye los árboles uno tras otro (línea base).
func (t *Trainer) TrainSequential() *Forest {
	f := t.newForest()
	ws := t.newWorkspace()
	start := time.Now()
	for i := 0; i < t.cfg.NumTrees; i++ {
		treeStart := time.Now()
		tree, imp := t.buildOne(i, ws)
		f.Trees[i] = tree
		addInto(f.Importance, imp)
		if t.cfg.Verbose {
			fmt.Printf("[t=%6.2fs] hilo principal termina el árbol %03d (%.2f s)\n",
				time.Since(start).Seconds(), i, time.Since(treeStart).Seconds())
		}
	}
	f.normalizeImportance()
	return f
}

// TrainConcurrent reparte los árboles entre `workers` goroutines (worker pool).
//
//   - jobs: canal con los números de árbol pendientes. Cada worker toma el
//     siguiente libre, así los workers rápidos no esperan a los lentos.
//   - f.Trees[i]: cada árbol se guarda en su propia posición, que ningún otro
//     worker toca, por eso esa escritura no necesita bloqueo.
//   - f.Importance: es un acumulador compartido por todos los workers, así que
//     sumarle la importancia de un árbol es una sección crítica protegida con
//     sync.Mutex.
//   - sync.WaitGroup: espera a que todos los workers terminen antes de devolver
//     el bosque.
func (t *Trainer) TrainConcurrent(workers int) *Forest {
	if workers < 1 {
		workers = 1
	}
	f := t.newForest()
	start := time.Now()

	jobs := make(chan int, t.cfg.NumTrees)
	for i := 0; i < t.cfg.NumTrees; i++ {
		jobs <- i
	}
	close(jobs)

	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			ws := t.newWorkspace() // buffers propios del worker, reutilizados en cada árbol
			for treeIdx := range jobs {
				treeStart := time.Now()
				tree, imp := t.buildOne(treeIdx, ws)
				f.Trees[treeIdx] = tree

				mu.Lock()
				addInto(f.Importance, imp)
				mu.Unlock()

				if t.cfg.Verbose {
					fmt.Printf("[t=%6.2fs] worker %02d termina el árbol %03d (%.2f s)\n",
						time.Since(start).Seconds(), workerID, treeIdx, time.Since(treeStart).Seconds())
				}
			}
		}(w)
	}
	wg.Wait()
	f.normalizeImportance()
	return f
}

func addInto(dst, src []float64) {
	for i, v := range src {
		dst[i] += v
	}
}
