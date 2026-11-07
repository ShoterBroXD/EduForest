package dataset

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// Cabeceras reales de 2021-2022 (PromocionGuiada) y 2023-2024 (Desaprobado).
const header2021 = "cod_mod,anexo,Nombre,gestion,id_nivel,dsc_nivel,Edad,TipoDiscaIntegrada,TotalEstudiantes,Discapacidad,Mujer,Hombre,Venezolanos,Peruanos,Extranjeros,DNI_validado,DNI_SinValidar,No_DNI,Aprobado,PromocionGuiada,Retirado,Fallecido,RequiereRecuperacion,Matriculado,PostergaEvaluacion,tot_atraso\r\n"
const header2024 = "cod_mod,anexo,Nombre,gestion,id_nivel,dsc_nivel,Edad,TipoDiscaIntegrada,TotalEstudiantes,Discapacidad,Mujer,Hombre,Venezolanos,Peruanos,Extranjeros,DNI_validado,DNI_SinValidar,No_DNI,Aprobado,Desaprobado,Retirado,Fallecido,RequiereRecuperacion,Matriculado,PostergaEvaluacion,tot_atraso\r\n"

// Filas: total, mujer, hombre, ..., aprobado, noAprobado, retirado, fallecido, recup, matric, posterga, atraso
const rows2023 = "" +
	"100,0,COLEGIO A,Pública de gestión directa,B0,Primaria,8,,10,0,5,5,0,10,0,10,0,0,10,0,0,0,0,0,0,1\r\n" +
	"100,0,COLEGIO A,Pública de gestión directa,B0,Primaria,9,,10,0,4,6,1,9,0,9,0,1,8,0,1,1,0,0,0,0\r\n" +
	"200,0,\"JESUS, FUENTES DE VIDA\",Privada,F0,Secundaria,14,,20,1,10,10,0,20,0,20,0,0,15,5,0,0,0,0,0,4\r\n"

const rows2024 = "" +
	"100,0,COLEGIO A,Pública de gestión directa,B0,Primaria,8.0,,10,0,5,5,0,10,0,10,0,0,9,1,0,0,0,0,0,0\r\n" +
	"100,0,COLEGIO A,Pública de gestión directa,B0,Primaria,9.0,TEA,2,1,1,1,0,2,0,2,0,0,2,0,0,0,0,0,0,0\r\n" +
	"300,0,JARDIN B,Privada,A2,Inicial - Jardín,4.0,,8,0,4,4,0,8,0,8,0,0,8,0,0,0,0,0,0,0\r\n" +
	"400,0,COLEGIO MAL,Privada,B0,Primaria,10.0,,9,0,4,4,0,9,0,9,0,0,9,0,0,0,0,0,0,0\r\n" + // sexo no cuadra
	"500,0,COLEGIO C,Pública de gestión privada,F0,Secundaria,15.0,,12,0,6,6,0,12,0,12,0,0,10,0,2,0,0,0,0,3\r\n"

func writeFixtures(t *testing.T) []string {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"siagie_matricula_2023.csv": header2024 + rows2023,
		"siagie_matricula_2024.csv": header2024 + rows2024,
	}
	var paths []string
	for name, content := range files {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		paths = append(paths, p)
	}
	if paths[0] > paths[1] {
		paths[0], paths[1] = paths[1], paths[0]
	}
	return paths
}

func TestConcurrentEqualsSequential(t *testing.T) {
	paths := writeFixtures(t)
	seq, sst, err := LoadSequential(paths)
	if err != nil {
		t.Fatal(err)
	}
	old := ChunkLines
	ChunkLines = 1 // fuerza muchos bloques y muchas goroutines trabajando a la vez
	defer func() { ChunkLines = old }()
	for _, workers := range []int{1, 2, 4, 8} {
		conc, cst, err := LoadConcurrent(paths, workers)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(seq, conc) {
			t.Fatalf("workers=%d: la carga concurrente difiere de la secuencial", workers)
		}
		if cst.Rows != sst.Rows || cst.BadRows != sst.BadRows {
			t.Fatalf("workers=%d: stats distintas %+v vs %+v", workers, cst, sst)
		}
	}
	if len(seq) != 8 || sst.BadRows != 0 {
		t.Fatalf("se esperaban 8 registros válidos, hubo %d (%d inválidos)", len(seq), sst.BadRows)
	}
}

func TestParseRecordVariantsAndQuotes(t *testing.T) {
	paths := writeFixtures(t)
	recs, _, err := LoadSequential(paths)
	if err != nil {
		t.Fatal(err)
	}
	quoted := recs[2]
	if quoted.CodMod != 200 || quoted.Gestion != "Privada" || quoted.NoAprobado != 5 || quoted.Year != 2023 {
		t.Fatalf("fila con comillas mal parseada: %+v", quoted)
	}
	if recs[3].Edad != 8 || recs[3].Year != 2024 || recs[3].NoAprobado != 1 {
		t.Fatalf("edad \"8.0\" o Desaprobado mal parseados: %+v", recs[3])
	}

	// La variante 2021 mapea PromocionGuiada al mismo campo NoAprobado.
	cols, err := mapColumns(splitHeader(header2021))
	if err != nil {
		t.Fatal(err)
	}
	if cols.noAprob != 19 {
		t.Fatalf("PromocionGuiada debería estar en la columna 19, quedó en %d", cols.noAprob)
	}
	if _, err := mapColumns([]string{"cod_mod", "anexo"}); err == nil {
		t.Fatal("una cabecera incompleta debería dar error")
	}
}

func splitHeader(h string) []string {
	var out []string
	cur := ""
	for _, c := range h {
		switch c {
		case ',':
			out = append(out, cur)
			cur = ""
		case '\r', '\n':
		default:
			cur += string(c)
		}
	}
	return append(out, cur)
}

func TestLabels(t *testing.T) {
	cases := []struct {
		tasa          float64
		logro, riesgo int
	}{
		{0, 0, 0},
		{0.05, 1, 1},
		{0.10, 1, 1},
		{0.15, 1, 2},
		{0.20, 1, 2},
		{0.50, 2, 2},
	}
	for _, c := range cases {
		if got := ClaseLogro(c.tasa); got != c.logro {
			t.Errorf("ClaseLogro(%v) = %d, se esperaba %d", c.tasa, got, c.logro)
		}
		if got := ClaseRiesgo(c.tasa); got != c.riesgo {
			t.Errorf("ClaseRiesgo(%v) = %d, se esperaba %d", c.tasa, got, c.riesgo)
		}
	}
}

func TestPrepare(t *testing.T) {
	paths := writeFixtures(t)
	recs, _, err := LoadSequential(paths)
	if err != nil {
		t.Fatal(err)
	}
	for _, workers := range []int{1, 3} {
		d, rep := Prepare(recs, 3, workers)
		// 8 registros: 1 Inicial, 1 inconsistente, 1 con 2 alumnos -> quedan 5.
		want := CleanReport{Input: 8, NoEscolar: 1, Inconsistent: 1, PocosAlumnos: 1, Kept: 5}
		if rep != want {
			t.Fatalf("workers=%d: reporte %+v, se esperaba %+v", workers, rep, want)
		}
		x := d.X[3]
		if d.CodMod[3] != 100 || d.Year[3] != 2024 {
			t.Fatalf("orden inesperado: %v %v", d.CodMod, d.Year)
		}
		if x[13] != 0 || x[14] != 1.0/19.0 {
			t.Fatalf("historia del año anterior mal calculada: no_logro=%v retiro=%v", x[13], x[14])
		}
		// El servicio 2024 incluye el grupo de 2 alumnos aunque no sea muestra.
		if x[11] != 12 {
			t.Fatalf("alumnos_servicio = %v, se esperaba 12", x[11])
		}
		// Los grupos de 2023 no tienen año anterior.
		if d.X[0][13] != -1 || d.X[0][14] != -1 {
			t.Fatal("sin año anterior debería marcarse con -1")
		}
		// Etiquetas: colegio 200 tiene 5/20 sin aprobar -> logro bajo.
		if d.Logro[2] != 2 || d.Riesgo[2] != 0 {
			t.Fatalf("etiquetas colegio 200: logro=%d riesgo=%d", d.Logro[2], d.Riesgo[2])
		}
		// Colegio 500: 2/12 retirados (16.7%) -> riesgo alto.
		if d.Riesgo[4] != 2 {
			t.Fatalf("riesgo colegio 500 = %d, se esperaba 2", d.Riesgo[4])
		}
	}
}

func TestSplitRanges(t *testing.T) {
	for _, tc := range []struct{ n, parts int }{{10, 3}, {2, 8}, {0, 4}, {7, 1}} {
		covered := 0
		prev := 0
		for _, r := range splitRanges(tc.n, tc.parts) {
			if r[0] != prev {
				t.Fatalf("n=%d parts=%d: rangos no contiguos %v", tc.n, tc.parts, splitRanges(tc.n, tc.parts))
			}
			covered += r[1] - r[0]
			prev = r[1]
		}
		if covered != tc.n {
			t.Fatalf("n=%d parts=%d: cubre %d", tc.n, tc.parts, covered)
		}
	}
}
