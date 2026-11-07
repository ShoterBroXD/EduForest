package bench

import (
	"encoding/csv"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"time"
)

func TrimmedMean(values []float64, trim int) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	if 2*trim >= len(sorted) {
		trim = 0
	}
	sorted = sorted[trim : len(sorted)-trim]
	sum := 0.0
	for _, v := range sorted {
		sum += v
	}
	return sum / float64(len(sorted))
}

// TimeRuns ejecuta fn `runs` veces y devuelve los segundos de cada corrida.
func TimeRuns(runs int, fn func()) []float64 {
	times := make([]float64, runs)
	for i := range times {
		runtime.GC() // que la basura de la corrida anterior no se cobre en esta
		start := time.Now()
		fn()
		times[i] = time.Since(start).Seconds()
	}
	return times
}

type SweepRow struct {
	Workers       int
	Times         []float64
	TrimmedAvg    float64
	Speedup       float64
	Efficiency    float64
	MarginalGain  float64 // % en que bajó el tiempo respecto a la fila anterior
	IsEquilibrium bool
}

func Sweep(workerCounts []int, runs, trim int, baselineSeq float64, train func(workers int)) []SweepRow {
	rows := make([]SweepRow, 0, len(workerCounts))
	fmt.Printf("%-8s %-16s %-9s %-11s %s\n", "Workers", "Media recortada", "Speedup", "Eficiencia", "Ganancia marginal")
	for _, w := range workerCounts {
		times := TimeRuns(runs, func() { train(w) })
		row := SweepRow{Workers: w, Times: times, TrimmedAvg: TrimmedMean(times, trim)}
		row.Speedup = baselineSeq / row.TrimmedAvg
		row.Efficiency = row.Speedup / float64(w)
		gain := "      -"
		if len(rows) > 0 {
			prev := rows[len(rows)-1].TrimmedAvg
			row.MarginalGain = (prev - row.TrimmedAvg) / prev * 100
			gain = fmt.Sprintf("%6.1f%%", row.MarginalGain)
		}
		rows = append(rows, row)
		fmt.Printf("%-8d %-16.3f %-9.2f %-11.2f %s\n", w, row.TrimmedAvg, row.Speedup, row.Efficiency, gain)
	}
	return rows
}

func MarkEquilibrium(rows []SweepRow, threshold float64) int {
	if len(rows) == 0 {
		return 0
	}
	best := rows[0].TrimmedAvg
	for _, r := range rows {
		if r.TrimmedAvg < best {
			best = r.TrimmedAvg
		}
	}
	limit := best * (1 + threshold/100)
	for i := range rows {
		if rows[i].TrimmedAvg <= limit {
			rows[i].IsEquilibrium = true
			return rows[i].Workers
		}
	}
	return 0
}

// WriteSweepCSV guarda el barrido para graficarlo.
func WriteSweepCSV(path string, baselineSeq float64, rows []SweepRow) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	w.Write([]string{"workers", "media_recortada_s", "speedup", "eficiencia", "ganancia_marginal_pct", "equilibrio", "secuencial_s"})
	ff := func(v float64) string { return strconv.FormatFloat(v, 'f', 4, 64) }
	for _, r := range rows {
		w.Write([]string{strconv.Itoa(r.Workers), ff(r.TrimmedAvg), ff(r.Speedup), ff(r.Efficiency),
			ff(r.MarginalGain), strconv.FormatBool(r.IsEquilibrium), ff(baselineSeq)})
	}
	w.Flush()
	if err := w.Error(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

type Resources struct {
	NumCPU, GOMAXPROCS, NumGoroutine int
	HeapAllocMB, TotalAllocMB        float64
	NumGC                            uint32
}

func CaptureResources() Resources {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return Resources{
		NumCPU:       runtime.NumCPU(),
		GOMAXPROCS:   runtime.GOMAXPROCS(0),
		NumGoroutine: runtime.NumGoroutine(),
		HeapAllocMB:  float64(m.HeapAlloc) / (1 << 20),
		TotalAllocMB: float64(m.TotalAlloc) / (1 << 20),
		NumGC:        m.NumGC,
	}
}

func PrintResources(before, after Resources) {
	fmt.Println("\n=== Uso de recursos ===")
	fmt.Printf("CPU lógicas: %d | GOMAXPROCS: %d | goroutines al terminar: %d\n", after.NumCPU, after.GOMAXPROCS, after.NumGoroutine)
	fmt.Printf("Heap en uso: %.1f MB (antes: %.1f MB) | memoria reservada en el periodo: %.1f MB\n",
		after.HeapAllocMB, before.HeapAllocMB, after.TotalAllocMB-before.TotalAllocMB)
	fmt.Printf("Ciclos de garbage collector: %d\n", after.NumGC-before.NumGC)
}
