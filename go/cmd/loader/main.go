package main

import (
	"flag"
	"fmt"
	"log"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	"github.com/ShoterBroXD/EduForest/internal/dataset"
)

func main() {
	dataDir := flag.String("data", "../data/siagie", "carpeta con los archivos siagie_matricula_*.csv")
	workers := flag.Int("workers", runtime.NumCPU(), "goroutines del worker pool de parseo")
	minAlumnos := flag.Int("min-alumnos", 3, "descarta grupos con menos alumnos (sus tasas son muy ruidosas)")
	out := flag.String("out", "../data/siagie_clean.csv", "CSV limpio de salida (vacío para no escribir)")
	compare := flag.Bool("compare", true, "medir también la carga secuencial para calcular el speedup")
	flag.Parse()

	paths, err := filepath.Glob(filepath.Join(*dataDir, "siagie_matricula_*.csv"))
	if err != nil || len(paths) == 0 {
		log.Fatalf("no se encontraron siagie_matricula_*.csv en %s", *dataDir)
	}
	sort.Strings(paths)
	fmt.Printf("Archivos: %d | CPU lógicas: %d | workers: %d\n", len(paths), runtime.NumCPU(), *workers)

	var seqDur time.Duration
	if *compare {
		_, st, err := dataset.LoadSequential(paths) // solo interesa el tiempo
		if err != nil {
			log.Fatal(err)
		}
		seqDur = st.Duration
		fmt.Printf("[secuencial]  %d filas (%d inválidas) en %v\n", st.Rows, st.BadRows, st.Duration.Round(time.Millisecond))
		runtime.GC() // liberar esa copia antes de medir la concurrente
	}

	recs, st, err := dataset.LoadConcurrent(paths, *workers)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("[concurrente] %d filas (%d inválidas) en %v\n", st.Rows, st.BadRows, st.Duration.Round(time.Millisecond))
	if *compare {
		fmt.Printf("Speedup de carga: %.2fx\n", seqDur.Seconds()/st.Duration.Seconds())
	}

	t := time.Now()
	ds, rep := dataset.Prepare(recs, *minAlumnos, *workers)
	fmt.Printf("\nLimpieza y features en %v\n", time.Since(t).Round(time.Millisecond))
	fmt.Printf("  filas de entrada:        %9d\n", rep.Input)
	fmt.Printf("  Inicial (sin notas):    -%9d\n", rep.NoEscolar)
	fmt.Printf("  conteos inconsistentes: -%9d\n", rep.Inconsistent)
	fmt.Printf("  grupos con < %d alumnos: -%9d\n", *minAlumnos, rep.PocosAlumnos)
	fmt.Printf("  muestras finales:        %9d (%d features)\n", rep.Kept, len(ds.FeatureNames))

	printClasses("Nivel de logro", dataset.ClasesLogro, dataset.ClassCounts(ds.Logro, len(dataset.ClasesLogro)), ds.Len())
	printClasses("Riesgo de deserción", dataset.ClasesRiesgo, dataset.ClassCounts(ds.Riesgo, len(dataset.ClasesRiesgo)), ds.Len())

	if *out != "" {
		t = time.Now()
		if err := dataset.WriteCSV(ds, *out); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("\nDataset limpio guardado en %s (%v)\n", *out, time.Since(t).Round(time.Millisecond))
	}
}

func printClasses(title string, names []string, counts []int, total int) {
	fmt.Printf("\n%s:\n", title)
	for i, name := range names {
		fmt.Printf("  %-13s %9d (%5.2f%%)\n", name, counts[i], 100*float64(counts[i])/float64(total))
	}
}
