package dataset

import (
	"sync"
)

const (
	NivelPrimaria   = "Primaria"
	NivelSecundaria = "Secundaria"

	// UmbralLogroMedio: hasta 20% de alumnos sin aprobar es logro medio; más es
	// logro bajo. 0% es logro alto. Elegido con el EDA: cerca del percentil 75
	// de la tasa de no aprobación entre los grupos que tienen alguna.
	UmbralLogroMedio = 0.20
	// UmbralRiesgoMedio: hasta 10% de retirados es riesgo medio; más es riesgo
	// alto. 0% es riesgo bajo. Cerca del percentil 90 de la tasa de retiro por
	// servicio educativo entre los que tienen algún retirado.
	UmbralRiesgoMedio = 0.10
)

var (
	ClasesLogro  = []string{"Logro alto", "Logro medio", "Logro bajo"}
	ClasesRiesgo = []string{"Riesgo bajo", "Riesgo medio", "Riesgo alto"}

	FeatureNames = []string{
		"anio", "secundaria", "gestion", "edad", "alumnos_grupo",
		"pct_mujer", "pct_discapacidad", "tipo_disca_registrado",
		"pct_extranjero", "pct_sin_dni", "pct_atraso",
		"alumnos_servicio", "pct_atraso_servicio",
		"no_logro_servicio_prev", "retiro_servicio_prev",
	}
)

// Dataset son las muestras listas para entrenar. Cada fila de X corresponde a
// un grupo (servicio educativo x edad x tipo de discapacidad) de un año.
// Logro y Riesgo son las dos variables objetivo; se entrena un bosque por cada una.
type Dataset struct {
	FeatureNames []string
	X            [][]float64
	Logro        []int
	Riesgo       []int
	Year         []int
	CodMod       []int // para identificar el colegio en las predicciones
	Anexo        []int
}

func (d *Dataset) Len() int { return len(d.X) }

// CleanReport resume qué descartó la limpieza.
type CleanReport struct {
	Input        int
	NoEscolar    int // Inicial: no tiene calificaciones
	Inconsistent int // conteos que no cuadran con TotalEstudiantes
	PocosAlumnos int // grupos con menos de minAlumnos (tasas muy ruidosas)
	Kept         int
}

func base(r Record) int { return r.Total - r.Fallecido }

// TasaNoLogro es la proporción de alumnos que no aprobaron (desaprobados o en
// promoción guiada, más los que requieren recuperación).
func TasaNoLogro(r Record) float64 {
	return float64(r.NoAprobado+r.ReqRecuperacion) / float64(base(r))
}

// TasaRetiro es la proporción de alumnos retirados durante el año.
func TasaRetiro(r Record) float64 {
	return float64(r.Retirado) / float64(base(r))
}

func ClaseLogro(tasa float64) int {
	switch {
	case tasa == 0:
		return 0
	case tasa <= UmbralLogroMedio:
		return 1
	default:
		return 2
	}
}

func ClaseRiesgo(tasa float64) int {
	switch {
	case tasa == 0:
		return 0
	case tasa <= UmbralRiesgoMedio:
		return 1
	default:
		return 2
	}
}

// consistent verifica que las situaciones finales sumen el total y que el
// desglose por sexo también cuadre.
func consistent(r Record) bool {
	sit := r.Aprobado + r.NoAprobado + r.Retirado + r.Fallecido +
		r.ReqRecuperacion + r.Matriculado + r.Posterga
	return r.Total > 0 && base(r) > 0 && sit == r.Total && r.Mujer+r.Hombre == r.Total
}

type serviceKey struct {
	Year, CodMod, Anexo int
	Nivel               string
}

func keyOf(r Record) serviceKey {
	return serviceKey{Year: r.Year, CodMod: r.CodMod, Anexo: r.Anexo, Nivel: r.Nivel}
}

type serviceStats struct {
	Base, Atraso, NoLogro, Retirado int
}

func (s serviceStats) rate(n int) float64 {
	if s.Base == 0 {
		return 0
	}
	return float64(n) / float64(s.Base)
}

// aggregateServices suma los conteos por servicio educativo y año.
// Cada worker acumula su parte en un mapa local (sin bloqueos) y al final
// fusiona ese mapa en el global protegido por un sync.Mutex. Así la sección
// crítica ocurre una vez por worker y no una vez por fila.
func aggregateServices(recs []Record, workers int) map[serviceKey]serviceStats {
	global := make(map[serviceKey]serviceStats)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, rg := range splitRanges(len(recs), workers) {
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			local := make(map[serviceKey]serviceStats)
			for _, r := range recs[lo:hi] {
				s := local[keyOf(r)]
				s.Base += base(r)
				s.Atraso += r.Atraso
				s.NoLogro += r.NoAprobado + r.ReqRecuperacion
				s.Retirado += r.Retirado
				local[keyOf(r)] = s
			}
			mu.Lock()
			for k, v := range local {
				g := global[k]
				g.Base += v.Base
				g.Atraso += v.Atraso
				g.NoLogro += v.NoLogro
				g.Retirado += v.Retirado
				global[k] = g
			}
			mu.Unlock()
		}(rg[0], rg[1])
	}
	wg.Wait()
	return global
}

// Prepare limpia los registros y construye el Dataset.
//
//  1. Limpieza: se quedan Primaria y Secundaria con conteos consistentes.
//  2. Agregación por servicio (concurrente, con mutex) usando todos los grupos
//     válidos, para tener el tamaño y la historia completa de cada colegio.
//  3. Se descartan los grupos con menos de minAlumnos.
//  4. Construcción de features (concurrente, sin mutex): cada worker escribe
//     solo en su rango de filas, que no se cruza con el de ningún otro.
func Prepare(recs []Record, minAlumnos, workers int) (*Dataset, CleanReport) {
	rep := CleanReport{Input: len(recs)}
	valid := make([]Record, 0, len(recs)/2)
	for _, r := range recs {
		if r.Nivel != NivelPrimaria && r.Nivel != NivelSecundaria {
			rep.NoEscolar++
			continue
		}
		if !consistent(r) {
			rep.Inconsistent++
			continue
		}
		valid = append(valid, r)
	}

	services := aggregateServices(valid, workers)

	samples := make([]Record, 0, len(valid))
	for _, r := range valid {
		if r.Total < minAlumnos {
			rep.PocosAlumnos++
			continue
		}
		samples = append(samples, r)
	}
	rep.Kept = len(samples)

	n, nf := len(samples), len(FeatureNames)
	d := &Dataset{
		FeatureNames: FeatureNames,
		X:            make([][]float64, n),
		Logro:        make([]int, n),
		Riesgo:       make([]int, n),
		Year:         make([]int, n),
		CodMod:       make([]int, n),
		Anexo:        make([]int, n),
	}
	flat := make([]float64, n*nf) // un solo bloque de memoria para todas las filas

	var wg sync.WaitGroup
	for _, rg := range splitRanges(n, workers) {
		wg.Add(1)
		go func(lo, hi int) {
			defer wg.Done()
			for i := lo; i < hi; i++ {
				r := samples[i]
				row := flat[i*nf : (i+1)*nf : (i+1)*nf]
				fillFeatures(row, r, services)
				d.X[i] = row
				d.Logro[i] = ClaseLogro(TasaNoLogro(r))
				d.Riesgo[i] = ClaseRiesgo(TasaRetiro(r))
				d.Year[i] = r.Year
				d.CodMod[i] = r.CodMod
				d.Anexo[i] = r.Anexo
			}
		}(rg[0], rg[1])
	}
	wg.Wait()
	return d, rep
}

// fillFeatures escribe las features de un grupo en row. Ninguna usa las
// columnas de resultado del mismo año (aprobados, retirados...), para no
// filtrar la respuesta al modelo. La historia del servicio sí usa el año
// anterior: es lo que permite la identificación temprana. En 2021 no hay año
// anterior y se marca con -1.
func fillFeatures(row []float64, r Record, services map[serviceKey]serviceStats) {
	b := float64(base(r))
	svc := services[keyOf(r)]
	prevNoLogro, prevRetiro := -1.0, -1.0
	prevKey := keyOf(r)
	prevKey.Year--
	if prev, ok := services[prevKey]; ok && prev.Base > 0 {
		prevNoLogro = prev.rate(prev.NoLogro)
		prevRetiro = prev.rate(prev.Retirado)
	}
	row[0] = float64(r.Year)
	row[1] = boolF(r.Nivel == NivelSecundaria)
	row[2] = gestionCode(r.Gestion)
	row[3] = float64(r.Edad)
	row[4] = float64(r.Total)
	row[5] = float64(r.Mujer) / b
	row[6] = float64(r.Discapacidad) / b
	row[7] = boolF(r.TipoDisca != "")
	row[8] = float64(r.Venezolanos+r.Extranjeros) / b
	row[9] = float64(r.NoDNI) / b
	row[10] = float64(r.Atraso) / b
	row[11] = float64(svc.Base)
	row[12] = svc.rate(svc.Atraso)
	row[13] = prevNoLogro
	row[14] = prevRetiro
}

func gestionCode(g string) float64 {
	switch g {
	case "Pública de gestión directa":
		return 0
	case "Privada":
		return 1
	case "Pública de gestión privada":
		return 2
	default:
		return 3
	}
}

func boolF(b bool) float64 {
	if b {
		return 1
	}
	return 0
}

// splitRanges divide [0, n) en hasta `parts` rangos contiguos de tamaño similar.
func splitRanges(n, parts int) [][2]int {
	if parts < 1 {
		parts = 1
	}
	if parts > n {
		parts = n
	}
	out := make([][2]int, 0, parts)
	for p := 0; p < parts; p++ {
		lo, hi := p*n/parts, (p+1)*n/parts
		if lo < hi {
			out = append(out, [2]int{lo, hi})
		}
	}
	return out
}

// ClassCounts cuenta cuántas muestras hay de cada clase.
func ClassCounts(labels []int, numClasses int) []int {
	counts := make([]int, numClasses)
	for _, c := range labels {
		counts[c]++
	}
	return counts
}
