package bench

import "testing"

func TestTrimmedMean(t *testing.T) {
	if got := TrimmedMean([]float64{10, 1, 2, 3, 100}, 1); got != 5 {
		t.Fatalf("TrimmedMean = %v, se esperaba 5 (descarta 1 y 100)", got)
	}
	if got := TrimmedMean([]float64{4, 6}, 1); got != 5 {
		t.Fatalf("con trim demasiado grande debe promediar todo: %v", got)
	}
}

func TestMarkEquilibrium(t *testing.T) {
	// Tiempos con un salto ruidoso en 3 workers, como en un CPU híbrido.
	times := []float64{8.3, 4.0, 4.6, 4.0, 1.8, 1.6, 1.5, 1.43, 1.30, 1.29, 1.36}
	rows := make([]SweepRow, len(times))
	for i, v := range times {
		rows[i] = SweepRow{Workers: i + 1, TrimmedAvg: v}
	}
	// Mejor tiempo 1.29; a menos de 5% es <= 1.3545 -> 9 workers.
	if eq := MarkEquilibrium(rows, 5); eq != 9 || !rows[8].IsEquilibrium {
		t.Fatalf("equilibrio = %d, se esperaba 9", eq)
	}
	if eq := MarkEquilibrium(rows[:1], 5); eq != 1 {
		t.Fatalf("con una sola fila el equilibrio es esa fila: %d", eq)
	}
}
