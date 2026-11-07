package dataset

import (
	"bufio"
	"os"
	"strconv"
)

func WriteCSV(d *Dataset, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	w := bufio.NewWriterSize(f, 1<<20)

	w.WriteString("cod_mod,anexo")
	for _, name := range d.FeatureNames {
		w.WriteString(",")
		w.WriteString(name)
	}
	w.WriteString(",clase_logro,clase_riesgo\n")

	buf := make([]byte, 0, 256)
	for i, row := range d.X {
		buf = buf[:0]
		buf = strconv.AppendInt(buf, int64(d.CodMod[i]), 10)
		buf = append(buf, ',')
		buf = strconv.AppendInt(buf, int64(d.Anexo[i]), 10)
		for _, v := range row {
			buf = append(buf, ',')
			buf = strconv.AppendFloat(buf, v, 'g', 6, 64)
		}
		buf = append(buf, ',')
		buf = strconv.AppendInt(buf, int64(d.Logro[i]), 10)
		buf = append(buf, ',')
		buf = strconv.AppendInt(buf, int64(d.Riesgo[i]), 10)
		buf = append(buf, '\n')
		if _, err := w.Write(buf); err != nil {
			f.Close()
			return err
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}
