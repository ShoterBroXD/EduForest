"""Gráfico de la comparación sharding vs replicado (PC4).

Uso, desde la raíz del repositorio:
    python scripts/graficos_cluster.py
Lee results/cluster_riesgo.csv y guarda docs/PC4/figuras/fig_cluster.png
"""
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("results/cluster_riesgo.csv")
base = df[df.modo == "centralizado"].total_s.iloc[0]
colores = {"sharding": "#2a78d6", "replicado": "#eb6834"}
nombres = {"sharding": "Sharding", "replicado": "Replicado"}

plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 11})
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.6))

# (a) Tiempo total de entrenamiento.
for modo in ["sharding", "replicado"]:
    d = df[df.modo == modo]
    ax1.plot(d.nodos, d.total_s, marker="o", markersize=8, linewidth=2,
             color=colores[modo], label=nombres[modo])
    ultimo = d.iloc[-1]
    ax1.annotate(f"{ultimo.total_s:.2f} s", (ultimo.nodos, ultimo.total_s),
                 textcoords="offset points", xytext=(8, -4), color="#333")
ax1.axhline(base, color="#888", linestyle="--", linewidth=1.5)
ax1.text(4.1, base + 0.12, f"Centralizado PC3, 20 workers: {base:.2f} s", color="#555", fontsize=10, ha="right")
ax1.set_title("(a) Tiempo total de 100 árboles", loc="left", fontweight="bold")
ax1.set_xlabel("Nodos")
ax1.set_ylabel("Segundos")
ax1.set_xticks([1, 2, 3, 4])
ax1.set_ylim(0, 4.6)
ax1.legend(frameon=False, loc="upper right")

# (b) Datos enviados por la red en la carga.
ancho = 0.38
for k, modo in enumerate(["sharding", "replicado"]):
    d = df[df.modo == modo]
    x = d.nodos + (k - 0.5) * ancho
    barras = ax2.bar(x, d.mb_enviados, width=ancho - 0.04, color=colores[modo], label=nombres[modo])
    for b, v in zip(barras, d.mb_enviados):
        ax2.text(b.get_x() + b.get_width() / 2, v + 1, f"{v:.1f}", ha="center", fontsize=9, color="#333")
ax2.set_title("(b) Datos enviados a los nodos", loc="left", fontweight="bold")
ax2.set_xlabel("Nodos")
ax2.set_ylabel("MB")
ax2.set_xticks([1, 2, 3, 4])
ax2.set_ylim(0, 72)
ax2.legend(frameon=False, loc="upper left")

for ax in (ax1, ax2):
    ax.spines[["top", "right"]].set_visible(False)
    ax.grid(axis="y", color="#e6e6e6")
    ax.set_axisbelow(True)

fig.tight_layout()
fig.savefig("docs/PC4/figuras/fig_cluster.png", dpi=200)
print("Gráfico guardado en docs/PC4/figuras/fig_cluster.png")
