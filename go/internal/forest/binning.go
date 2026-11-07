package forest

import (
	"math"
	"sort"
	"sync"
)

// Binned es el set de entrenamiento discretizado. Con 800 mil filas, ordenar
// los valores en cada nodo (como hacía HydroForest) es demasiado lento; en su
// lugar cada feature se corta una sola vez en hasta MaxBins intervalos por
// cuantiles y los nodos solo cuentan cuántas filas de cada clase caen en cada
// intervalo (un histograma). Es la misma idea que usan LightGBM y XGBoost-hist.
type Binned struct {
	Cols  [][]uint8   // Cols[f][i]: intervalo de la fila i en la feature f (por columnas, para recorrerlas rápido)
	Edges [][]float64 // Edges[f][b]: límite superior del intervalo b; el último es +Inf
	N     int
}

// NumBins devuelve cuántos intervalos tiene la feature f.
func (b *Binned) NumBins(f int) int { return len(b.Edges[f]) }

// NewBinned discretiza X. Cada feature es independiente de las demás, así que
// se procesan en paralelo: una goroutine por feature, limitadas por un
// semáforo (canal con buffer) a `workers` a la vez.
func NewBinned(X [][]float64, maxBins, workers int) *Binned {
	if maxBins < 2 || maxBins > 255 {
		maxBins = 255
	}
	if workers < 1 {
		workers = 1
	}
	n := len(X)
	nf := 0
	if n > 0 {
		nf = len(X[0])
	}
	b := &Binned{Cols: make([][]uint8, nf), Edges: make([][]float64, nf), N: n}

	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for f := 0; f < nf; f++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(f int) {
			defer wg.Done()
			defer func() { <-sem }()
			vals := make([]float64, n)
			for i := range X {
				vals[i] = X[i][f]
			}
			edges := cutPoints(vals, maxBins) // ordena vals
			col := make([]uint8, n)
			for i := range X {
				col[i] = uint8(sort.SearchFloat64s(edges, X[i][f]))
			}
			b.Edges[f] = edges // cada goroutine escribe solo en su índice f
			b.Cols[f] = col
		}(f)
	}
	wg.Wait()
	return b
}

// cutPoints devuelve los límites superiores de cada intervalo, en orden.
// Si la feature tiene pocos valores distintos (año, edad, gestión...) cada
// valor es su propio intervalo; si tiene muchos, se usan cuantiles.
func cutPoints(vals []float64, maxBins int) []float64 {
	sort.Float64s(vals)
	var cuts []float64
	for i, v := range vals {
		if i == 0 || v != vals[i-1] {
			cuts = append(cuts, v)
			if len(cuts) > maxBins {
				break
			}
		}
	}
	if len(cuts) > maxBins {
		cuts = cuts[:0]
		n := len(vals)
		for k := 1; k < maxBins; k++ {
			v := vals[k*n/maxBins-1]
			if len(cuts) == 0 || v > cuts[len(cuts)-1] {
				cuts = append(cuts, v)
			}
		}
	} else if len(cuts) > 0 {
		cuts = cuts[:len(cuts)-1] // el máximo no sirve como corte
	}
	return append(cuts, math.Inf(1))
}
