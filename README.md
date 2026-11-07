# EduForest

Random Forest distribuido en Go para clasificar el **nivel de logro** y el **riesgo de deserción escolar** de los colegios del Perú, con datos del MINEDU (SIAGIE 2021-2024).

CC65 Programación Concurrente y Distribuida · UPC · Grupo 02

## Estructura

```
go/
  cmd/loader/        carga concurrente, limpieza y exportación del dataset
  cmd/eduforest/     entrenamiento, evaluación, benchmark y barrido de workers
  internal/dataset/  lectura de los CSV (pipeline con canales) y features
  internal/forest/   Random Forest: histogramas, bootstrap balanceado, worker pool
  internal/metrics/  matriz de confusión, recall, F1 macro, exactitud balanceada
  internal/bench/    media recortada, speedup, eficiencia y punto de equilibrio
notebooks/           análisis exploratorio (EDA)
scripts/             gráficos de escalabilidad
docs/PC3/            diseño de la arquitectura y figuras del informe
results/             salidas de las corridas (métricas, bench, sweep)
```

## Datos

1. Descargar los 4 CSV de [Matriculación y Trayectoria Estudiantil 2021-2024](https://www.datosabiertos.gob.pe/dataset/matriculaci%C3%B3n-y-trayectoria-estudiantil-2021-2024) (datosabiertos.gob.pe).
2. Guardarlos en `data/siagie/` con los nombres `siagie_matricula_2021.csv` … `siagie_matricula_2024.csv`.

Los CSV no se suben al repo (están en `.gitignore`): pesan unos 240 MB.

## Cómo correrlo

Requisitos: Go 1.24 o superior. Para el notebook y los gráficos, Python 3 con pandas y matplotlib.

```bash
cd go
go test -race ./...                                              # pruebas con detector de carreras

go run ./cmd/loader                                              # carga concurrente vs secuencial y limpieza
go run ./cmd/eduforest -target riesgo                            # entrena y evalúa en 2024 (riesgo de deserción)
go run ./cmd/eduforest -target logro                             # ídem para nivel de logro
go run ./cmd/eduforest -target riesgo -mode bench -runs 5        # speedup secuencial vs concurrente
go run ./cmd/eduforest -target riesgo -mode sweep -runs 5 -trees 50   # barrido de 1 a N workers

cd ..
python scripts/graficos_escalabilidad.py                         # gráfico a partir de results/sweep_riesgo.csv
```

Parámetros útiles de `eduforest`: `-trees`, `-max-depth`, `-balance` (0 = sin corregir el desbalance, 1 = clases iguales), `-workers`, `-test-year`.

## Flujo de trabajo

Usamos Git Flow: cada tarea va en una rama `feature/...` que sale de `develop` y vuelve por Pull Request revisado por otro integrante. `main` solo recibe versiones entregadas (PC3, PC4, TB2). Las tareas se siguen en el tablero Kanban del proyecto en GitHub.

## Integrantes

- Meléndez Marín, Diego Fernando (U202220345)
- Díaz Chavez, Ángel Gabriel (U20221C424)
- Villafuerte Ramírez, Diego Tomas (U202010546)
