package dataset

import (
	"encoding/csv"
	"errors"
	"io"
	"os"
	"strconv"
	"strings"
)

// SchoolKey identifica un servicio educativo: código modular y anexo.
type SchoolKey struct{ CodMod, Anexo int }

// LoadSchoolNames lee solo las columnas cod_mod, anexo y Nombre de un CSV del
// SIAGIE. La API lo usa para mostrar el nombre de cada colegio en la web,
// sin cargar el nombre en las 2.2 millones de filas del entrenamiento.
func LoadSchoolNames(path string) (map[SchoolKey]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.ReuseRecord = true
	r.LazyQuotes = true
	header, err := r.Read()
	if err != nil {
		return nil, err
	}
	iCod, iAnexo, iNombre := -1, -1, -1
	for i, h := range header {
		switch strings.TrimSpace(strings.TrimPrefix(h, "\ufeff")) {
		case "cod_mod":
			iCod = i
		case "anexo":
			iAnexo = i
		case "Nombre":
			iNombre = i
		}
	}
	if iCod < 0 || iAnexo < 0 || iNombre < 0 {
		return nil, errors.New("el CSV no tiene las columnas cod_mod, anexo y Nombre")
	}
	names := make(map[SchoolKey]string)
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue // una fila mal formada no debe impedir cargar el resto
		}
		cod, err1 := strconv.Atoi(strings.TrimSpace(rec[iCod]))
		anexo, err2 := strconv.Atoi(strings.TrimSpace(rec[iAnexo]))
		if err1 != nil || err2 != nil {
			continue
		}
		k := SchoolKey{cod, anexo}
		if _, ok := names[k]; !ok {
			names[k] = strings.TrimSpace(rec[iNombre])
		}
	}
	return names, nil
}
