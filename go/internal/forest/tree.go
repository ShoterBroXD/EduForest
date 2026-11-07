// Package forest implementa el Random Forest de EduForest: árboles CART con
// índice de Gini, bootstrap (opcionalmente balanceado por clase), subconjunto
// aleatorio de features por nodo y voto por mayoría.
package forest

import (
	"math"
	"math/rand"
)

type Config struct {
	NumTrees        int
	MaxDepth        int
	MinSamplesSplit int
	NumClasses      int
	MaxFeatures     int // features evaluadas por nodo (0 = raíz cuadrada del total)
	SampleSize      int // filas del bootstrap de cada árbol (0 = tantas como el set de entrenamiento)
	// Balance controla cuánto se corrige el desbalance de clases en el bootstrap:
	// 0 = muestreo normal (cada clase pesa según su frecuencia), 1 = todas las
	// clases aportan lo mismo. Valores intermedios suavizan la corrección: la
	// probabilidad de cada clase es proporcional a frecuencia^(1-Balance).
	Balance float64
	MaxBins int // intervalos por feature para los histogramas (máx. 255)
	Seed    int64
	Verbose bool
}

// Node es un nodo del árbol. Threshold está en la escala original de la
// feature, así que para predecir no hace falta discretizar nada.
type Node struct {
	IsLeaf     bool
	ClassLabel int
	FeatureIdx int
	Threshold  float64
	Left       *Node
	Right      *Node
}

// treeBuilder tiene todo lo que necesita un árbol mientras se construye.
// Cada árbol usa el suyo, así que ninguno de estos campos se comparte entre
// goroutines. Lo único compartido (bins e y) es de solo lectura.
type treeBuilder struct {
	bins       *Binned
	y          []int
	cfg        Config
	rng        *rand.Rand
	hist       []int     // histograma reutilizable: [intervalo][clase]
	scratch    []int     // buffer para particionar sin reservar memoria nueva
	importance []float64 // reducción de Gini acumulada por feature en este árbol
}

func newTreeBuilder(bins *Binned, y []int, cfg Config, seed int64) *treeBuilder {
	maxB := 0
	for f := range bins.Edges {
		if nb := bins.NumBins(f); nb > maxB {
			maxB = nb
		}
	}
	return &treeBuilder{
		bins:       bins,
		y:          y,
		cfg:        cfg,
		rng:        rand.New(rand.NewSource(seed)),
		hist:       make([]int, maxB*cfg.NumClasses),
		importance: make([]float64, len(bins.Edges)),
	}
}

// build crece el árbol de forma recursiva sobre las filas idx. idx se
// reordena en el lugar al particionar, sin crear slices nuevos por nodo.
func (tb *treeBuilder) build(idx []int, depth int) *Node {
	k := tb.cfg.NumClasses
	counts := make([]int, k)
	for _, i := range idx {
		counts[tb.y[i]]++
	}
	leaf := &Node{IsLeaf: true, ClassLabel: argmax(counts)}
	if depth >= tb.cfg.MaxDepth || len(idx) < tb.cfg.MinSamplesSplit || isPure(counts) {
		return leaf
	}

	f, bin, gain := tb.bestSplit(idx, counts)
	if f < 0 || gain <= 0 {
		return leaf
	}

	// Partición estable: a la izquierda las filas con intervalo <= bin y a la
	// derecha el resto, sin cambiar su orden relativo. Como idx viene ordenado,
	// cada hijo sigue ordenado y los recorridos de memoria siguen siendo
	// secuenciales (mejor uso de la caché del procesador).
	col := tb.bins.Cols[f]
	right := tb.scratch[:0]
	lo := 0
	for _, i := range idx {
		if int(col[i]) <= bin {
			idx[lo] = i
			lo++
		} else {
			right = append(right, i)
		}
	}
	copy(idx[lo:], right)
	if lo == 0 || lo == len(idx) {
		return leaf
	}
	tb.importance[f] += gain * float64(len(idx))

	return &Node{
		FeatureIdx: f,
		Threshold:  tb.bins.Edges[f][bin],
		Left:       tb.build(idx[:lo], depth+1),
		Right:      tb.build(idx[lo:], depth+1),
	}
}

// bestSplit prueba MaxFeatures features al azar. Por cada una arma el
// histograma (filas por intervalo y clase) en una pasada y luego evalúa todos
// los cortes posibles recorriendo los intervalos de izquierda a derecha.
func (tb *treeBuilder) bestSplit(idx []int, total []int) (bestF, bestBin int, bestGain float64) {
	k := tb.cfg.NumClasses
	n := len(idx)
	parent := gini(total, n)
	bestF, bestBin, bestGain = -1, -1, 0

	left := make([]int, k)
	right := make([]int, k)
	for _, f := range tb.featureSubset() {
		nb := tb.bins.NumBins(f)
		if nb < 2 {
			continue
		}
		h := tb.hist[:nb*k]
		for j := range h {
			h[j] = 0
		}
		col := tb.bins.Cols[f]
		for _, i := range idx {
			h[int(col[i])*k+tb.y[i]]++
		}

		for c := range left {
			left[c] = 0
		}
		nLeft := 0
		for b := 0; b < nb-1; b++ {
			for c := 0; c < k; c++ {
				left[c] += h[b*k+c]
				nLeft += h[b*k+c]
			}
			if nLeft == 0 {
				continue
			}
			if nLeft == n {
				break
			}
			for c := 0; c < k; c++ {
				right[c] = total[c] - left[c]
			}
			nRight := n - nLeft
			weighted := (float64(nLeft)*gini(left, nLeft) + float64(nRight)*gini(right, nRight)) / float64(n)
			if g := parent - weighted; g > bestGain {
				bestF, bestBin, bestGain = f, b, g
			}
		}
	}
	return
}

func (tb *treeBuilder) featureSubset() []int {
	nf := len(tb.bins.Edges)
	m := tb.cfg.MaxFeatures
	if m <= 0 {
		m = int(math.Sqrt(float64(nf)))
	}
	if m < 1 {
		m = 1
	}
	if m > nf {
		m = nf
	}
	return tb.rng.Perm(nf)[:m]
}

func gini(counts []int, total int) float64 {
	if total == 0 {
		return 0
	}
	sum := 0.0
	for _, c := range counts {
		p := float64(c) / float64(total)
		sum += p * p
	}
	return 1 - sum
}

func argmax(counts []int) int {
	best := 0
	for c, v := range counts {
		if v > counts[best] {
			best = c
		}
	}
	return best
}

func isPure(counts []int) bool {
	nonZero := 0
	for _, c := range counts {
		if c > 0 {
			nonZero++
		}
	}
	return nonZero <= 1
}

func predictTree(node *Node, x []float64) int {
	for !node.IsLeaf {
		if x[node.FeatureIdx] <= node.Threshold {
			node = node.Left
		} else {
			node = node.Right
		}
	}
	return node.ClassLabel
}

// bootstrap llena idx con las filas de un árbol, elegidas con reemplazo. Con balance = 0 es el
// bootstrap clásico. Con balance > 0 primero se sortea la clase (con
// probabilidad proporcional a frecuencia^(1-balance)) y luego una fila al azar
// de esa clase: así la clase minoritaria (riesgo alto, 2% de los datos) gana
// peso. Con balance = 1 es el "Balanced Random Forest" de Chen, Liaw y
// Breiman (2004).
func bootstrap(idx []int, n int, byClass [][]int, balance float64, rng *rand.Rand) {
	if balance <= 0 {
		for i := range idx {
			idx[i] = rng.Intn(n)
		}
		return
	}
	var pools [][]int
	var cum []float64 // probabilidad acumulada de cada clase
	total := 0.0
	for _, p := range byClass {
		if len(p) > 0 {
			pools = append(pools, p)
			total += math.Pow(float64(len(p)), 1-balance)
			cum = append(cum, total)
		}
	}
	for i := range idx {
		r := rng.Float64() * total
		c := 0
		for c < len(cum)-1 && r >= cum[c] {
			c++
		}
		p := pools[c]
		idx[i] = p[rng.Intn(len(p))]
	}
}
