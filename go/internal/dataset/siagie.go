package dataset

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

type Record struct {
	Year      int
	CodMod    int
	Anexo     int
	Gestion   string
	Nivel     string
	Edad      int
	TipoDisca string

	Total           int
	Discapacidad    int
	Mujer           int
	Hombre          int
	Venezolanos     int
	Extranjeros     int
	NoDNI           int // No_DNI + DNI_SinValidar
	Aprobado        int
	NoAprobado      int // Desaprobado (2023-2024) o PromocionGuiada (2021-2022)
	Retirado        int
	Fallecido       int
	ReqRecuperacion int
	Matriculado     int // matriculado sin situación final registrada
	Posterga        int
	Atraso          int
}

type columns struct {
	codMod, anexo, gestion, nivel, edad, tipoDisca  int
	total, disca, mujer, hombre, venez, extr        int
	dniSin, noDNI, aprob, noAprob, retirado, fallec int
	recup, matric, posterga, atraso                 int
	n                                               int
}

func mapColumns(header []string) (*columns, error) {
	idx := make(map[string]int, len(header))
	for i, h := range header {
		idx[strings.TrimSpace(strings.TrimPrefix(h, "\ufeff"))] = i
	}
	var err error
	col := func(names ...string) int {
		for _, name := range names {
			if i, ok := idx[name]; ok {
				return i
			}
		}
		if err == nil {
			err = fmt.Errorf("columna requerida no encontrada: %s", strings.Join(names, " o "))
		}
		return -1
	}
	c := &columns{
		codMod: col("cod_mod"), anexo: col("anexo"), gestion: col("gestion"),
		nivel: col("dsc_nivel"), edad: col("Edad"), tipoDisca: col("TipoDiscaIntegrada"),
		total: col("TotalEstudiantes"), disca: col("Discapacidad"), mujer: col("Mujer"),
		hombre: col("Hombre"), venez: col("Venezolanos"), extr: col("Extranjeros"),
		dniSin: col("DNI_SinValidar"), noDNI: col("No_DNI"), aprob: col("Aprobado"),
		noAprob: col("Desaprobado", "PromocionGuiada"), retirado: col("Retirado"),
		fallec: col("Fallecido"), recup: col("RequiereRecuperacion"),
		matric: col("Matriculado"), posterga: col("PostergaEvaluacion"), atraso: col("tot_atraso"),
		n: len(header),
	}
	return c, err
}

type fieldParser struct {
	f   []string
	err error
}

func (p *fieldParser) int(i int) int {
	if p.err != nil {
		return 0
	}
	s := strings.TrimSpace(p.f[i])
	if s == "" {
		return 0
	}
	if v, err := strconv.Atoi(s); err == nil {
		return v
	}
	// Algunos años guardan la edad como "4.0".
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		p.err = fmt.Errorf("columna %d: %q no es numérico", i, s)
		return 0
	}
	return int(v)
}

func (p *fieldParser) str(i int) string { return strings.TrimSpace(p.f[i]) }

func parseRecord(fields []string, c *columns, year int) (Record, error) {
	if len(fields) != c.n {
		return Record{}, fmt.Errorf("se esperaban %d campos y llegaron %d", c.n, len(fields))
	}
	p := fieldParser{f: fields}
	r := Record{
		Year:            year,
		CodMod:          p.int(c.codMod),
		Anexo:           p.int(c.anexo),
		Gestion:         p.str(c.gestion),
		Nivel:           p.str(c.nivel),
		Edad:            p.int(c.edad),
		TipoDisca:       p.str(c.tipoDisca),
		Total:           p.int(c.total),
		Discapacidad:    p.int(c.disca),
		Mujer:           p.int(c.mujer),
		Hombre:          p.int(c.hombre),
		Venezolanos:     p.int(c.venez),
		Extranjeros:     p.int(c.extr),
		NoDNI:           p.int(c.noDNI) + p.int(c.dniSin),
		Aprobado:        p.int(c.aprob),
		NoAprobado:      p.int(c.noAprob),
		Retirado:        p.int(c.retirado),
		Fallecido:       p.int(c.fallec),
		ReqRecuperacion: p.int(c.recup),
		Matriculado:     p.int(c.matric),
		Posterga:        p.int(c.posterga),
		Atraso:          p.int(c.atraso),
	}
	return r, p.err
}

var yearRe = regexp.MustCompile(`20\d{2}`)

// yearFromPath saca el año del nombre del archivo (siagie_matricula_2024.csv).
func yearFromPath(path string) (int, error) {
	m := yearRe.FindAllString(filepath.Base(path), -1)
	if len(m) == 0 {
		return 0, fmt.Errorf("%s: no se encontró el año en el nombre del archivo", path)
	}
	return strconv.Atoi(m[len(m)-1])
}
