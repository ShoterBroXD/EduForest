// Comando clusterbench: sharding vs replicado con 1..N nodos, contra la versión de un proceso (PC3).
//
//	go run ./cmd/clusterbench -nodes localhost:9101,localhost:9102,localhost:9103,localhost:9104
package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/ShoterBroXD/EduForest/internal/bench"
	"github.com/ShoterBroXD/EduForest/internal/cluster"
	"github.com/ShoterBroXD/EduForest/internal/dataset"
	"github.com/ShoterBroXD/EduForest/internal/forest"
	"github.com/ShoterBroXD/EduForest/internal/metrics"
)

type row struct {
	modo                          string
	nodos, filasNodo              int
	carga, entrenamiento, total   float64
	mbEnviados, mbRecibidos       float64
	exactitud, balanceada, f1, ra float64
}

func main() {
	dataDir := flag.String("data", "../data/siagie", "carpeta con los siagie_matricula_*.csv")
	target := flag.String("target", "riesgo", "riesgo o logro")
	nodesFlag := flag.String("nodes", os.Getenv("NODES"), "direcciones de los nodos separadas por coma")
	modes := flag.String("modes", "sharding,replicado", "modos a comparar")
	countsFlag := flag.String("counts", "", "cantidades de nodos a probar, por ejemplo 1,2,4 (por defecto 1..N)")
	runs := flag.Int("runs", 3, "corridas por medición")
	trim := flag.Int("trim", 0, "corridas descartadas en cada extremo")
	workers := flag.Int("workers", runtime.NumCPU(), "workers de la línea base centralizada")
	trees := flag.Int("trees", 100, "número de árboles")
	maxDepth := flag.Int("max-depth", 14, "profundidad máxima")
	minSplit := flag.Int("min-samples-split", 100, "mínimo de filas para dividir")
	balance := flag.Float64("balance", 0.5, "corrección del desbalance")
	seed := flag.Int64("seed", 42, "semilla")
	resultsDir := flag.String("results", "../results", "carpeta de resultados")
	flag.Parse()

	var nodes []string
	for _, n := range strings.Split(*nodesFlag, ",") {
		if n = strings.TrimSpace(n); n != "" {
			nodes = append(nodes, n)
		}
	}
	if len(nodes) == 0 {
		log.Fatal("indica los nodos con -nodes o la variable NODES")
	}
	counts := parseCounts(*countsFlag, len(nodes))

	t := time.Now()
	ds, rep, err := dataset.LoadDir(*dataDir, 3, runtime.NumCPU())
	if err != nil {
		log.Fatal(err)
	}
	labels, classes, err := ds.Labels(*target)
	if err != nil {
		log.Fatal(err)
	}
	sp, err := ds.SplitByYear(labels, 2024)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Datos: %d muestras (%v) | entrenamiento %d | prueba %d\n", rep.Kept, time.Since(t).Round(time.Millisecond), len(sp.Xtr), len(sp.Xte))

	cfg := forest.Config{NumTrees: *trees, MaxDepth: *maxDepth, MinSamplesSplit: *minSplit,
		NumClasses: len(classes), Balance: *balance, MaxBins: 64, Seed: *seed}
	t = time.Now()
	trainer := forest.NewTrainer(sp.Xtr, sp.Ytr, cfg, runtime.NumCPU())
	fmt.Printf("Discretización en el coordinador: %v\n\n", time.Since(t).Round(time.Millisecond))

	evaluate := func(f *forest.Forest) metrics.Report {
		return metrics.Evaluate(sp.Yte, f.PredictBatch(sp.Xte, runtime.NumCPU()), len(classes))
	}

	var rows []row
	fmt.Printf("%-12s %5s %10s %8s %8s %8s %9s %9s %7s %7s %9s\n",
		"Modo", "Nodos", "Filas/nodo", "Carga", "Entren.", "Total", "MB env.", "MB rec.", "F1", "Bal.", "Recall a.")
	show := func(r row) {
		fmt.Printf("%-12s %5d %10d %7.2fs %7.2fs %7.2fs %9.1f %9.1f %7.3f %7.3f %9.3f\n",
			r.modo, r.nodos, r.filasNodo, r.carga, r.entrenamiento, r.total, r.mbEnviados, r.mbRecibidos, r.f1, r.balanceada, r.ra)
	}

	// Línea base: un solo proceso con el worker pool de la PC3.
	var base *forest.Forest
	times := bench.TimeRuns(*runs, func() { base = trainer.TrainConcurrent(*workers) })
	r := row{modo: "centralizado", nodos: 1, filasNodo: len(sp.Ytr), entrenamiento: bench.TrimmedMean(times, *trim)}
	r.total = r.entrenamiento
	fillMetrics(&r, evaluate(base))
	rows = append(rows, r)
	show(r)

	for _, mode := range strings.Split(*modes, ",") {
		for _, k := range counts {
			c := cluster.NewCoordinator(nodes[:k])
			var out *cluster.TrainOutcome
			var carga, entren, total []float64
			for i := 0; i < *runs; i++ {
				runtime.GC()
				job := cluster.TrainJob{ID: fmt.Sprintf("bench-%s-%d", mode, k), Bins: trainer.Bins, Y: sp.Ytr, Config: cfg, Mode: mode, Seed: *seed}
				out, err = c.Train(context.Background(), job, nil)
				if err != nil {
					log.Fatalf("%s con %d nodos: %v", mode, k, err)
				}
				carga = append(carga, out.CargaMs/1000)
				entren = append(entren, out.TrainMs/1000)
				total = append(total, out.TotalMs/1000)
			}
			r := row{modo: mode, nodos: k, filasNodo: out.Reports[0].Filas,
				carga: bench.TrimmedMean(carga, *trim), entrenamiento: bench.TrimmedMean(entren, *trim), total: bench.TrimmedMean(total, *trim)}
			for _, rep := range out.Reports {
				r.mbEnviados += float64(rep.BytesEnviados) / (1 << 20)
				r.mbRecibidos += float64(rep.BytesRecibidos) / (1 << 20)
			}
			fillMetrics(&r, evaluate(out.Forest))
			rows = append(rows, r)
			show(r)
		}
	}

	if err := os.MkdirAll(*resultsDir, 0o755); err != nil {
		log.Fatal(err)
	}
	path := filepath.Join(*resultsDir, "cluster_"+*target+".csv")
	if err := writeCSV(path, rows); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nResultados en %s\n", path)
}

func fillMetrics(r *row, m metrics.Report) {
	r.exactitud, r.balanceada, r.f1 = m.Accuracy, m.BalancedAcc, m.MacroF1
	r.ra = m.Recall[len(m.Recall)-1]
}

func parseCounts(s string, max int) []int {
	var out []int
	if s == "" {
		for k := 1; k <= max; k++ {
			out = append(out, k)
		}
		return out
	}
	for _, p := range strings.Split(s, ",") {
		k, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil || k < 1 || k > max {
			log.Fatalf("cantidad de nodos inválida %q (hay %d nodos)", p, max)
		}
		out = append(out, k)
	}
	return out
}

func writeCSV(path string, rows []row) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := csv.NewWriter(f)
	w.Write([]string{"modo", "nodos", "filas_por_nodo", "carga_s", "entrenamiento_s", "total_s",
		"mb_enviados", "mb_recibidos", "exactitud", "exactitud_balanceada", "f1_macro", "recall_clase_alta"})
	ff := func(v float64) string { return strconv.FormatFloat(v, 'f', 4, 64) }
	for _, r := range rows {
		w.Write([]string{r.modo, strconv.Itoa(r.nodos), strconv.Itoa(r.filasNodo), ff(r.carga), ff(r.entrenamiento), ff(r.total),
			ff(r.mbEnviados), ff(r.mbRecibidos), ff(r.exactitud), ff(r.balanceada), ff(r.f1), ff(r.ra)})
	}
	w.Flush()
	if err := w.Error(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
