# Entregable de Ángel · rama feature/eda

Qué hay aquí (respetar las carpetas, son las mismas del repo):
- notebooks/eda_siagie.ipynb: análisis exploratorio del SIAGIE, ya ejecutado (con salidas y gráficos)
- go/internal/metrics/metrics_test.go: tests del paquete de métricas
- docs/PC3/figuras/: las 4 figuras que genera el notebook

Antes de subirlo:
1. Lee el notebook completo y vuelve a ejecutarlo (Kernel > Restart & Run All). Necesitas pandas y matplotlib,
   y los 4 CSV en data/siagie/ (links en el README del repo).
2. Corre los tests: cd go ; go test -race ./internal/metrics/
3. Si algo no entiendes, pregunta: el profe puede preguntarte en la exposición.

Cómo subirlo (después de que Diego mergee su rama a develop):
    git clone https://github.com/ShoterBroXD/EduForest.git
    cd EduForest
    git config user.name "TU_USUARIO_GITHUB"
    git config user.email "EL_CORREO_DE_TU_CUENTA_GITHUB"
    git checkout develop
    git checkout -b feature/eda
    (copia aquí las carpetas notebooks, go y docs de este zip)
    git add notebooks go/internal/metrics/metrics_test.go docs/PC3/figuras
    git commit -m "EDA del SIAGIE en notebook y tests de métricas"
    git push -u origin feature/eda
Luego en GitHub: Pull Request de feature/eda hacia develop, y pide a Diego que lo revise.
En el Kanban, mueve "Limpieza, unificación de columnas y EDA" a Review y asígnatela.
