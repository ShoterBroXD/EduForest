package forest

import "sync"

// NewTrainerFromBins arma un Trainer con datos que ya llegan discretizados.
// Lo usan los nodos del clúster: el coordinador discretiza una sola vez y
// cada nodo recibe solo las columnas de su fragmento (shard).
func NewTrainerFromBins(bins *Binned, y []int, cfg Config) *Trainer {
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
	return &Trainer{Bins: bins, y: y, byClass: byClass, cfg: cfg}
}

// TrainTrees construye solo los árboles de treeIdx con un worker pool. Cada
// árbol usa la semilla de su número global, así que el árbol 7 sale igual sin
// importar qué nodo o qué worker lo construye. Devuelve los árboles en el
// mismo orden que treeIdx y la importancia sin normalizar, para que el
// coordinador pueda sumar la de todos los nodos.
func (t *Trainer) TrainTrees(treeIdx []int, workers int, onTree func(treeIdx int)) ([]*Node, []float64) {
	if workers < 1 {
		workers = 1
	}
	trees := make([]*Node, len(treeIdx))
	importance := make([]float64, len(t.Bins.Edges))

	jobs := make(chan int, len(treeIdx)) // posiciones dentro de treeIdx
	for pos := range treeIdx {
		jobs <- pos
	}
	close(jobs)

	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ws := t.newWorkspace()
			for pos := range jobs {
				tree, imp := t.buildOne(treeIdx[pos], ws)
				trees[pos] = tree
				mu.Lock()
				addInto(importance, imp)
				mu.Unlock()
				if onTree != nil {
					onTree(treeIdx[pos])
				}
			}
		}()
	}
	wg.Wait()
	return trees, importance
}

// NewForest junta árboles que llegaron de distintos nodos en un solo bosque.
func NewForest(trees []*Node, numClasses int, importance []float64) *Forest {
	f := &Forest{Trees: trees, NumClasses: numClasses, Importance: importance}
	f.normalizeImportance()
	return f
}
