package metrics

import (
	"math"
	"strings"
	"testing"
)

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9 }

func TestEvaluate(t *testing.T) {
	// 10 muestras, 3 clases. Matriz esperada (filas = real, columnas = predicha):
	//   clase 0: 4 bien, 1 como clase 1
	//   clase 1: 2 bien, 1 como clase 0
	//   clase 2: 1 bien, 1 como clase 1
	yTrue := []int{0, 0, 0, 0, 0, 1, 1, 1, 2, 2}
	yPred := []int{0, 0, 0, 0, 1, 1, 1, 0, 2, 1}
	r := Evaluate(yTrue, yPred, 3)

	want := [][]int{{4, 1, 0}, {1, 2, 0}, {0, 1, 1}}
	for i := range want {
		for j := range want[i] {
			if r.Confusion[i][j] != want[i][j] {
				t.Fatalf("Confusion[%d][%d] = %d, se esperaba %d", i, j, r.Confusion[i][j], want[i][j])
			}
		}
	}
	if !near(r.Accuracy, 0.7) {
		t.Fatalf("exactitud = %v, se esperaba 0.7", r.Accuracy)
	}
	// Precisión: clase 0 = 4/5, clase 1 = 2/4, clase 2 = 1/1.
	// Recall:    clase 0 = 4/5, clase 1 = 2/3, clase 2 = 1/2.
	precision := []float64{0.8, 0.5, 1}
	recall := []float64{0.8, 2.0 / 3, 0.5}
	for c := range precision {
		if !near(r.Precision[c], precision[c]) || !near(r.Recall[c], recall[c]) {
			t.Fatalf("clase %d: precisión %v recall %v", c, r.Precision[c], r.Recall[c])
		}
		f1 := 2 * precision[c] * recall[c] / (precision[c] + recall[c])
		if !near(r.F1[c], f1) {
			t.Fatalf("clase %d: F1 %v, se esperaba %v", c, r.F1[c], f1)
		}
	}
	if !near(r.BalancedAcc, (0.8+2.0/3+0.5)/3) {
		t.Fatalf("exactitud balanceada = %v", r.BalancedAcc)
	}
	if r.Support[0] != 5 || r.Support[1] != 3 || r.Support[2] != 2 {
		t.Fatalf("soporte = %v", r.Support)
	}
}

// Un modelo que siempre predice la clase mayoritaria tiene exactitud alta pero
// F1 macro y exactitud balanceada bajas: por eso se reportan esas métricas.
func TestMajorityClassIsPenalized(t *testing.T) {
	yTrue := make([]int, 100)
	for i := 94; i < 100; i++ {
		yTrue[i] = 2 // 6% de clase rara, como el riesgo alto
	}
	yPred := make([]int, 100) // siempre clase 0
	r := Evaluate(yTrue, yPred, 3)
	if !near(r.Accuracy, 0.94) {
		t.Fatalf("exactitud = %v", r.Accuracy)
	}
	if r.Recall[2] != 0 || r.F1[2] != 0 || r.Precision[1] != 0 {
		t.Fatal("una clase nunca predicha debe tener recall, precisión y F1 en 0 (sin dividir entre cero)")
	}
	if r.BalancedAcc > 0.34 || r.MacroF1 > 0.33 {
		t.Fatalf("balanceada %v y F1 macro %v deberían delatar al modelo trivial", r.BalancedAcc, r.MacroF1)
	}
}

func TestString(t *testing.T) {
	r := Evaluate([]int{0, 1}, []int{0, 1}, 2)
	out := r.String([]string{"Riesgo bajo", "Riesgo alto"})
	for _, s := range []string{"Riesgo bajo", "Matriz de confusión", "F1 macro: 1.000"} {
		if !strings.Contains(out, s) {
			t.Fatalf("el reporte no contiene %q:\n%s", s, out)
		}
	}
}
