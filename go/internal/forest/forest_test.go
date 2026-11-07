package forest

import (
	"math"
	"math/rand"
	"path/filepath"
	"reflect"
	"testing"
)

// synthetic genera un problema de 3 clases que depende de las features 0 y 1,
// con la clase 2 rara (~5%), parecido al desbalance del riesgo de deserción.
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

func testConfig() Config {
	return Config{NumTrees: 12, MaxDepth: 8, MinSamplesSplit: 10, NumClasses: 3, MaxFeatures: 2, MaxBins: 32, Seed: 7}
}

// El resultado no debe depender de cuántos workers entrenan: cada árbol tiene
// su propia semilla y su propia posición en el bosque.
func TestConcurrentEqualsSequential(t *testing.T) {
	X, y := synthetic(3000, 1)
	tr := NewTrainer(X, y, testConfig(), 4)
	seq := tr.TrainSequential()
	for _, w := range []int{1, 2, 3, 8} {
		con := tr.TrainConcurrent(w)
		if !reflect.DeepEqual(seq.Trees, con.Trees) {
			t.Fatalf("workers=%d: los árboles difieren de la versión secuencial", w)
		}
		for i := range seq.Importance {
			if math.Abs(seq.Importance[i]-con.Importance[i]) > 1e-9 {
				t.Fatalf("workers=%d: importancia distinta en la feature %d", w, i)
			}
		}
	}
}

func TestLearnsAndBalances(t *testing.T) {
	X, y := synthetic(6000, 2)
	Xte, yte := synthetic(2000, 3)
	cfg := testConfig()

	plain := NewTrainer(X, y, cfg, 2).TrainConcurrent(2)
	cfg.Balance = 1
	bal := NewTrainer(X, y, cfg, 2).TrainConcurrent(2)

	acc := func(f *Forest) float64 {
		pred := f.PredictBatch(Xte, 3)
		ok := 0
		for i := range pred {
			if pred[i] == yte[i] {
				ok++
			}
		}
		return float64(ok) / float64(len(pred))
	}
	recallRare := func(f *Forest) float64 {
		hit, total := 0, 0
		for i, x := range Xte {
			if yte[i] == 2 {
				total++
				if f.PredictOne(x) == 2 {
					hit++
				}
			}
		}
		return float64(hit) / float64(total)
	}
	if a := acc(plain); a < 0.95 {
		t.Fatalf("exactitud %.3f demasiado baja para un problema casi separable", a)
	}
	if recallRare(bal) < recallRare(plain) {
		t.Fatalf("el bootstrap balanceado debería detectar al menos igual de bien la clase rara (%.2f vs %.2f)",
			recallRare(bal), recallRare(plain))
	}
	// La importancia debe concentrarse en las dos features que definen la clase.
	if bal.Importance[0]+bal.Importance[1] < 0.8 {
		t.Fatalf("importancia mal repartida: %v", bal.Importance)
	}
}

func TestBalancedBootstrap(t *testing.T) {
	byClass := [][]int{{0, 1, 2, 3, 4, 5, 6, 7}, {8}, {9, 10}}
	y := []int{0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 2}
	share := func(balance float64) []float64 {
		idx := make([]int, 30000)
		bootstrap(idx, 11, byClass, balance, rand.New(rand.NewSource(1)))
		out := make([]float64, 3)
		for _, i := range idx {
			out[y[i]] += 1.0 / 30000
		}
		return out
	}
	// balance = 1: un tercio de filas por clase.
	for c, s := range share(1) {
		if math.Abs(s-1.0/3) > 0.02 {
			t.Fatalf("balance=1, clase %d: %.3f, se esperaba 0.333", c, s)
		}
	}
	// balance = 0.5: proporcional a la raíz de la frecuencia (8, 1, 2).
	w := []float64{math.Sqrt(8), 1, math.Sqrt(2)}
	sum := w[0] + w[1] + w[2]
	for c, s := range share(0.5) {
		if math.Abs(s-w[c]/sum) > 0.02 {
			t.Fatalf("balance=0.5, clase %d: %.3f, se esperaba %.3f", c, s, w[c]/sum)
		}
	}
	// balance = 0: bootstrap clásico, la clase 0 domina (8 de 11).
	if s := share(0)[0]; math.Abs(s-8.0/11) > 0.02 {
		t.Fatalf("balance=0: clase 0 %.3f, se esperaba 0.727", s)
	}
}

func TestBinning(t *testing.T) {
	X := [][]float64{{1, 0.1}, {2, 0.5}, {2, 0.9}, {3, 0.3}, {1, 0.7}}
	b := NewBinned(X, 255, 2)
	// Feature 0 tiene 3 valores distintos: cortes en 1 y 2, y +Inf.
	if !reflect.DeepEqual(b.Edges[0], []float64{1, 2, math.Inf(1)}) {
		t.Fatalf("cortes de la feature 0: %v", b.Edges[0])
	}
	if !reflect.DeepEqual(b.Cols[0], []uint8{0, 1, 1, 2, 0}) {
		t.Fatalf("intervalos de la feature 0: %v", b.Cols[0])
	}
	// Con muchos valores distintos se limita a maxBins intervalos.
	many := make([][]float64, 1000)
	for i := range many {
		many[i] = []float64{float64(i)}
	}
	if nb := NewBinned(many, 16, 1).NumBins(0); nb > 16 || nb < 8 {
		t.Fatalf("se esperaban hasta 16 intervalos, hubo %d", nb)
	}
}

func TestSaveLoad(t *testing.T) {
	X, y := synthetic(500, 4)
	f := NewTrainer(X, y, testConfig(), 1).TrainSequential()
	path := filepath.Join(t.TempDir(), "m.gob")
	if err := f.Save(path); err != nil {
		t.Fatal(err)
	}
	g, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range X[:50] {
		if f.PredictOne(x) != g.PredictOne(x) {
			t.Fatal("el modelo cargado predice distinto")
		}
	}
}
