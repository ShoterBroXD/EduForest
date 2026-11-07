package dataset

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// ChunkLines es la cantidad de líneas del CSV que viajan juntas por el canal
// de trabajos. Bloques grandes reducen el costo de sincronización por fila.
var ChunkLines = 8192

// LoadStats resume una carga.
type LoadStats struct {
	Files    int
	Rows     int // filas de datos leídas (sin contar cabeceras)
	BadRows  int // filas descartadas por formato inválido
	Duration time.Duration
}

// LoadSequential es la línea base: lee los archivos uno tras otro en una sola
// goroutine. Sirve para medir el speedup de LoadConcurrent.
func LoadSequential(paths []string) ([]Record, LoadStats, error) {
	start := time.Now()
	stats := LoadStats{Files: len(paths)}
	var out []Record
	for _, path := range paths {
		year, err := yearFromPath(path)
		if err != nil {
			return nil, stats, err
		}
		f, err := os.Open(path)
		if err != nil {
			return nil, stats, err
		}
		r := csv.NewReader(bufio.NewReaderSize(f, 1<<20))
		r.FieldsPerRecord = -1
		r.ReuseRecord = true
		header, err := r.Read()
		if err != nil {
			f.Close()
			return nil, stats, fmt.Errorf("%s: cabecera: %w", path, err)
		}
		cols, err := mapColumns(header)
		if err != nil {
			f.Close()
			return nil, stats, fmt.Errorf("%s: %w", path, err)
		}
		for {
			fields, err := r.Read()
			if err == io.EOF {
				break
			}
			stats.Rows++
			if err != nil {
				stats.BadRows++
				continue
			}
			rec, err := parseRecord(fields, cols, year)
			if err != nil {
				stats.BadRows++
				continue
			}
			out = append(out, rec)
		}
		f.Close()
	}
	stats.Duration = time.Since(start)
	return out, stats, nil
}

// chunk es un bloque de líneas crudas de un archivo, listo para parsear.
type chunk struct {
	file, seq, year int
	cols            *columns // solo lectura después de crearse: compartirlo es seguro
	data            []byte
}

type chunkResult struct {
	file, seq int
	recs      []Record
	rows, bad int
}

// LoadConcurrent carga los CSV con un pipeline de tres etapas:
//
//  1. Lectores: una goroutine por archivo lee bloques de ChunkLines líneas y
//     los envía al canal jobs. Así los cuatro archivos se leen en paralelo.
//  2. Parseadores: un worker pool de `workers` goroutines toma bloques de jobs,
//     convierte cada línea en un Record y envía el resultado al canal results.
//  3. Colector: la goroutine que llamó a la función recibe los resultados. Es la
//     única que escribe en perFile y stats, por eso no necesita mutex.
//
// Dos sync.WaitGroup coordinan el cierre: cuando terminan todos los lectores se
// cierra jobs (los workers salen de su range), y cuando terminan todos los
// workers se cierra results (el colector sale de su range).
func LoadConcurrent(paths []string, workers int) ([]Record, LoadStats, error) {
	if workers < 1 {
		workers = 1
	}
	start := time.Now()
	jobs := make(chan chunk, workers*2)
	results := make(chan chunkResult, workers*2)
	errs := make(chan error, len(paths))

	var readers sync.WaitGroup
	for i, path := range paths {
		readers.Add(1)
		go func(fileIdx int, path string) {
			defer readers.Done()
			if err := readChunks(fileIdx, path, jobs); err != nil {
				errs <- err
			}
		}(i, path)
	}

	var parsers sync.WaitGroup
	for w := 0; w < workers; w++ {
		parsers.Add(1)
		go func() {
			defer parsers.Done()
			for c := range jobs {
				results <- parseChunk(c)
			}
		}()
	}

	go func() { readers.Wait(); close(jobs) }()
	go func() { parsers.Wait(); close(results) }()

	stats := LoadStats{Files: len(paths)}
	perFile := make([][]chunkResult, len(paths))
	for res := range results {
		perFile[res.file] = append(perFile[res.file], res)
		stats.Rows += res.rows
		stats.BadRows += res.bad
	}
	// Aquí todos los lectores ya terminaron, así que errs no recibe más envíos.
	close(errs)
	if err := <-errs; err != nil {
		return nil, stats, err
	}

	// Los bloques llegan en cualquier orden. Se ordenan por archivo y secuencia
	// para que el resultado sea idéntico al de LoadSequential.
	total := 0
	for _, rs := range perFile {
		sort.Slice(rs, func(a, b int) bool { return rs[a].seq < rs[b].seq })
		for _, r := range rs {
			total += len(r.recs)
		}
	}
	out := make([]Record, 0, total)
	for _, rs := range perFile {
		for _, r := range rs {
			out = append(out, r.recs...)
		}
	}
	stats.Duration = time.Since(start)
	return out, stats, nil
}

// readChunks lee la cabecera de un archivo y luego lo corta en bloques de
// ChunkLines líneas completas. Ningún campo del SIAGIE contiene saltos de
// línea, así que cortar por '\n' nunca parte un registro.
func readChunks(fileIdx int, path string, jobs chan<- chunk) error {
	year, err := yearFromPath(path)
	if err != nil {
		return err
	}
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	br := bufio.NewReaderSize(f, 1<<20)
	headerLine, err := br.ReadString('\n')
	if err != nil && err != io.EOF {
		return fmt.Errorf("%s: cabecera: %w", path, err)
	}
	header, err := csv.NewReader(strings.NewReader(headerLine)).Read()
	if err != nil {
		return fmt.Errorf("%s: cabecera: %w", path, err)
	}
	cols, err := mapColumns(header)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}

	seq, lines := 0, 0
	buf := make([]byte, 0, 1<<20)
	for {
		line, rerr := br.ReadSlice('\n')
		buf = append(buf, line...) // copia: ReadSlice reutiliza su buffer interno
		if rerr == bufio.ErrBufferFull {
			continue // línea más larga que el buffer: seguir leyendo la misma línea
		}
		if rerr != nil && rerr != io.EOF {
			return fmt.Errorf("%s: %w", path, rerr)
		}
		if rerr == nil {
			lines++
			if lines < ChunkLines {
				continue
			}
		}
		if len(buf) > 0 {
			jobs <- chunk{file: fileIdx, seq: seq, year: year, cols: cols, data: buf}
			seq++
			lines = 0
			buf = make([]byte, 0, 1<<20)
		}
		if rerr == io.EOF {
			return nil
		}
	}
}

func parseChunk(c chunk) chunkResult {
	res := chunkResult{file: c.file, seq: c.seq, recs: make([]Record, 0, ChunkLines)}
	r := csv.NewReader(bytes.NewReader(c.data))
	r.FieldsPerRecord = -1
	r.ReuseRecord = true
	for {
		fields, err := r.Read()
		if err == io.EOF {
			break
		}
		res.rows++
		if err != nil {
			res.bad++
			continue
		}
		rec, err := parseRecord(fields, c.cols, c.year)
		if err != nil {
			res.bad++
			continue
		}
		res.recs = append(res.recs, rec)
	}
	return res
}
