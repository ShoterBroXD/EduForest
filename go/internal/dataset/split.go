package dataset

import (
	"fmt"
	"path/filepath"
	"sort"
)

// LoadDir carga y limpia todos los siagie_matricula_*.csv de una carpeta.
// Lo comparten los comandos de la PC3, la API y el benchmark del clúster.
func LoadDir(dir string, minAlumnos, workers int) (*Dataset, CleanReport, error) {
	paths, _ := filepath.Glob(filepath.Join(dir, "siagie_matricula_*.csv"))
	if len(paths) == 0 {
		return nil, CleanReport{}, fmt.Errorf("no se encontraron siagie_matricula_*.csv en %s", dir)
	}
	sort.Strings(paths)
	recs, _, err := LoadConcurrent(paths, workers)
	if err != nil {
		return nil, CleanReport{}, err
	}
	ds, rep := Prepare(recs, minAlumnos, workers)
	return ds, rep, nil
}

// Labels devuelve la variable objetivo y los nombres de sus clases.
func (d *Dataset) Labels(target string) ([]int, []string, error) {
	switch target {
	case "riesgo":
		return d.Riesgo, ClasesRiesgo, nil
	case "logro":
		return d.Logro, ClasesLogro, nil
	}
	return nil, nil, fmt.Errorf("objetivo desconocido %q: usa riesgo o logro", target)
}

// Split es la separación temporal: se entrena con los años anteriores y se
// evalúa en el año de prueba. TestRows guarda la fila del Dataset de cada
// muestra de prueba, para saber a qué colegio corresponde cada predicción.
type Split struct {
	Xtr, Xte [][]float64
	Ytr, Yte []int
	TestRows []int
}

func (d *Dataset) SplitByYear(labels []int, testYear int) (Split, error) {
	var s Split
	for i, x := range d.X {
		if d.Year[i] == testYear {
			s.Xte, s.Yte = append(s.Xte, x), append(s.Yte, labels[i])
			s.TestRows = append(s.TestRows, i)
		} else {
			s.Xtr, s.Ytr = append(s.Xtr, x), append(s.Ytr, labels[i])
		}
	}
	if len(s.Xtr) == 0 || len(s.Xte) == 0 {
		return s, fmt.Errorf("la separación por año %d dejó un conjunto vacío", testYear)
	}
	return s, nil
}
