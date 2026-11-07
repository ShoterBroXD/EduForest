package forest

import (
	"encoding/gob"
	"fmt"
	"os"
	"sync"
)

type Forest struct {
	Trees      []*Node
	NumClasses int
	Importance []float64 // importancia de cada feature (reducción de Gini), suma 1
}

// Votes devuelve cuántos árboles votaron por cada clase.
func (f *Forest) Votes(x []float64) []int {
	votes := make([]int, f.NumClasses)
	for _, t := range f.Trees {
		votes[predictTree(t, x)]++
	}
	return votes
}

// PredictOne devuelve la clase más votada.
func (f *Forest) PredictOne(x []float64) int {
	return argmax(f.Votes(x))
}

// PredictBatch predice muchas filas repartiéndolas en rangos contiguos, uno
// por goroutine. Cada goroutine escribe solo en su rango de out, así que no
// hace falta mutex: los rangos no se cruzan.
func (f *Forest) PredictBatch(X [][]float64, workers int) []int {
	out := make([]int, len(X))
	if workers < 1 {
		workers = 1
	}
	var wg sync.WaitGroup
	n := len(X)
	for w := 0; w < workers; w++ {
		lo, hi := w*n/workers, (w+1)*n/workers
		if lo == hi {
			continue
		}
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			for i := lo; i < hi; i++ {
				out[i] = f.PredictOne(X[i])
			}
		}(lo, hi)
	}
	wg.Wait()
	return out
}

func (f *Forest) normalizeImportance() {
	sum := 0.0
	for _, v := range f.Importance {
		sum += v
	}
	if sum == 0 {
		return
	}
	for i := range f.Importance {
		f.Importance[i] /= sum
	}
}

// Save guarda el bosque en disco (formato gob de la librería estándar) para
// que la API de la PC4 lo cargue sin volver a entrenar.
func (f *Forest) Save(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := gob.NewEncoder(file).Encode(f); err != nil {
		return fmt.Errorf("no se pudo guardar el modelo: %w", err)
	}
	return nil
}

func Load(path string) (*Forest, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var f Forest
	if err := gob.NewDecoder(file).Decode(&f); err != nil {
		return nil, fmt.Errorf("no se pudo leer el modelo: %w", err)
	}
	return &f, nil
}
