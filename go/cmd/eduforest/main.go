package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	"github.com/ShoterBroXD/EduForest/internal/bench"
	"github.com/ShoterBroXD/EduForest/internal/dataset"
	"github.com/ShoterBroXD/EduForest/internal/forest"
	"github.com/ShoterBroXD/EduForest/internal/metrics"
)

func main() {
	dataDir := flag.String("data", "../data/siagie", "carpeta con los siagie_matricula_*.csv")
	target := flag.String("target", "riesgo", "variable objetivo: riesgo (deserción) o logro (rendimiento)")
	mode := flag.String("mode", "train", "train | bench | sweep")
	workers := flag.Int("workers", runtime.NumCPU(), "workers para cargar, entrenar y predecir")
	testYear := flag.Int("test-year", 2024, "año reservado para evaluar (se entrena con los anteriores)")
	minAlumnos := flag.Int("min-alumnos", 3, "descarta grupos con menos alumnos")

	trees := flag.Int("trees", 100, "número de árboles")
	maxDepth := flag.Int("max-depth", 14, "profundidad máxima de cada árbol")
	minSplit := flag.Int("min-samples-split", 100, "mínimo de filas para dividir un nodo")
	sampleSize := flag.Int("sample-size", 0, "filas del bootstrap por árbol (0 = todo el set de entrenamiento)")
	balance := flag.Float64("balance", 0.5, "corrección del desbalance en el bootstrap: 0 = ninguna, 1 = clases iguales")
	maxBins := flag.Int("max-bins", 64, "intervalos por feature (máx. 255)")
	seed := flag.Int64("seed", 42, "semilla")
	verbose := flag.Bool("verbose", false, "imprime cuando termina cada árbol")

	runs := flag.Int("runs", 5, "corridas por medición (bench y sweep)")
	trim := flag.Int("trim", 1, "corridas descartadas en cada extremo para la media recortada")
	maxWorkers := flag.Int("max-workers", runtime.NumCPU(), "máximo de workers del barrido")
	threshold := flag.Float64("threshold", 5, "punto de equilibrio: menor número de workers a menos de este % del mejor tiempo")
	modelOut := flag.String("model-out", "", "dónde guardar el modelo (por defecto ../models/<target>.gob)")
	resultsDir := flag.String("results", "../results", "carpeta para los CSV de bench y sweep")
	flag.Parse()

	var classNames []string
	switch *target {
	case "riesgo":
		classNames = dataset.ClasesRiesgo
	case "logro":
		classNames = dataset.ClasesLogro
	default:
		log.Fatalf("target desconocido %q: usa riesgo o logro", *target)
	}

	// 1. Carga concurrente y limpieza.
	paths, _ := filepath.Glob(filepath.Join(*dataDir, "siagie_matricula_*.csv"))
	if len(paths) == 0 {
		log.Fatalf("no se encontraron siagie_matricula_*.csv en %s", *dataDir)
	}
	sort.Strings(paths)
	t := time.Now()
	recs, st, err := dataset.LoadConcurrent(paths, *workers)
	if err != nil {
		log.Fatal(err)
	}
	ds, rep := dataset.Prepare(recs, *minAlumnos, *workers)
	recs = nil
	fmt.Printf("Datos: %d filas leídas, %d muestras tras la limpieza (%v)\n", st.Rows, rep.Kept, time.Since(t).Round(time.Millisecond))

	labels := ds.Riesgo
	if *target == "logro" {
		labels = ds.Logro
	}
	var Xtr, Xte [][]float64
	var ytr, yte []int
	for i, x := range ds.X {
		if ds.Year[i] == *testYear {
			Xte, yte = append(Xte, x), append(yte, labels[i])
		} else {
			Xtr, ytr = append(Xtr, x), append(ytr, labels[i])
		}
	}
	if len(Xtr) == 0 || len(Xte) == 0 {
		log.Fatalf("la separación por año %d dejó un conjunto vacío", *testYear)
	}
	fmt.Printf("Objetivo: %s | entrenamiento: %d (años < %d) | prueba: %d (año %d)\n",
		*target, len(Xtr), *testYear, len(Xte), *testYear)
	printDistribution(classNames, ytr)

	cfg := forest.Config{
		NumTrees: *trees, MaxDepth: *maxDepth, MinSamplesSplit: *minSplit,
		NumClasses: len(classNames), SampleSize: *sampleSize, Balance: *balance,
		MaxBins: *maxBins, Seed: *seed, Verbose: *verbose,
	}
	t = time.Now()
	trainer := forest.NewTrainer(Xtr, ytr, cfg, *workers)
	cfg = trainer.Config()
	fmt.Printf("Discretización en histogramas (%d intervalos máx.): %v\n", cfg.MaxBins, time.Since(t).Round(time.Millisecond))
	fmt.Printf("Bosque: %d árboles, profundidad %d, bootstrap de %d filas, balance=%.2f\n",
		cfg.NumTrees, cfg.MaxDepth, cfg.SampleSize, cfg.Balance)

	if err := os.MkdirAll(*resultsDir, 0o755); err != nil {
		log.Fatal(err)
	}

	switch *mode {
	case "train":
		runTrain(trainer, Xte, yte, classNames, *workers, *target, *modelOut)
	case "bench":
		runBench(trainer, *workers, *runs, *trim, filepath.Join(*resultsDir, "bench_"+*target+".csv"))
	case "sweep":
		runSweep(trainer, *maxWorkers, *runs, *trim, *threshold, filepath.Join(*resultsDir, "sweep_"+*target+".csv"))
	default:
		log.Fatalf("modo desconocido %q: usa train, bench o sweep", *mode)
	}
}

func runTrain(trainer *forest.Trainer, Xte [][]float64, yte []int, classNames []string, workers int, target, modelOut string) {
	t := time.Now()
	f := trainer.TrainConcurrent(workers)
	fmt.Printf("\nEntrenamiento concurrente (%d workers): %v\n", workers, time.Since(t).Round(time.Millisecond))

	t = time.Now()
	pred := f.PredictBatch(Xte, workers)
	fmt.Printf("Predicción de %d filas: %v\n\n", len(Xte), time.Since(t).Round(time.Millisecond))
	fmt.Print(metrics.Evaluate(yte, pred, len(classNames)).String(classNames))

	fmt.Println("\nImportancia de las features (reducción de Gini):")
	order := make([]int, len(f.Importance))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool { return f.Importance[order[a]] > f.Importance[order[b]] })
	for _, i := range order {
		fmt.Printf("  %-24s %6.2f%%\n", dataset.FeatureNames[i], 100*f.Importance[i])
	}

	if modelOut == "" {
		modelOut = filepath.Join("..", "models", target+".gob")
	}
	if err := os.MkdirAll(filepath.Dir(modelOut), 0o755); err != nil {
		log.Fatal(err)
	}
	if err := f.Save(modelOut); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nModelo guardado en %s\n", modelOut)
}

func runBench(trainer *forest.Trainer, workers, runs, trim int, out string) {
	before := bench.CaptureResources()
	fmt.Printf("\n=== Secuencial (%d corridas) ===\n", runs)
	seqTimes := bench.TimeRuns(runs, func() { trainer.TrainSequential() })
	seq := bench.TrimmedMean(seqTimes, trim)
	fmt.Printf("Tiempos: %v | media recortada: %.3f s\n", round(seqTimes), seq)

	fmt.Printf("\n=== Concurrente, %d workers (%d corridas) ===\n", workers, runs)
	rows := bench.Sweep([]int{workers}, runs, trim, seq, func(w int) { trainer.TrainConcurrent(w) })
	fmt.Printf("Tiempos: %v\n", round(rows[0].Times))
	fmt.Printf("\nSpeedup = %.3f / %.3f = %.2fx | eficiencia = %.2f\n", seq, rows[0].TrimmedAvg, rows[0].Speedup, rows[0].Efficiency)
	bench.PrintResources(before, bench.CaptureResources())

	if err := bench.WriteSweepCSV(out, seq, rows); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("\nResultados en %s\n", out)
}

func runSweep(trainer *forest.Trainer, maxWorkers, runs, trim int, threshold float64, out string) {
	fmt.Printf("\n=== Línea base secuencial (%d corridas) ===\n", runs)
	seq := bench.TrimmedMean(bench.TimeRuns(runs, func() { trainer.TrainSequential() }), trim)
	fmt.Printf("Media recortada: %.3f s\n\n=== Barrido de workers ===\n", seq)

	counts := make([]int, maxWorkers)
	for i := range counts {
		counts[i] = i + 1
	}
	rows := bench.Sweep(counts, runs, trim, seq, func(w int) { trainer.TrainConcurrent(w) })
	if eq := bench.MarkEquilibrium(rows, threshold); eq > 0 {
		fmt.Printf("\nPunto de equilibrio: %d workers (a menos de %.0f%% del mejor tiempo; más workers casi no mejoran)\n", eq, threshold)
	}
	if err := bench.WriteSweepCSV(out, seq, rows); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Resultados en %s\n", out)
}

func printDistribution(names []string, y []int) {
	counts := dataset.ClassCounts(y, len(names))
	fmt.Print("Clases en entrenamiento:")
	for i, name := range names {
		fmt.Printf("  %s %.1f%%", name, 100*float64(counts[i])/float64(len(y)))
	}
	fmt.Println()
}

func round(v []float64) []float64 {
	out := make([]float64, len(v))
	for i, x := range v {
		out[i] = float64(int(x*1000)) / 1000
	}
	return out
}
