"""Gráfico de escalabilidad del entrenamiento del Random Forest (EduForest, PC3).

Lee el CSV que genera el barrido de workers:
    cd go
    go run ./cmd/eduforest -target riesgo -mode sweep -runs 5 -trees 50

y dibuja tres paneles (sin doble eje): tiempo, speedup contra el ideal y
eficiencia. Marca el punto de equilibrio que calculó Go y el menor número de
workers que alcanza el 90% del speedup máximo.

Uso (desde la raíz del repo):
    python scripts/graficos_escalabilidad.py
    python scripts/graficos_escalabilidad.py --csv results/sweep_logro.csv --out docs/PC3/figuras/fig_escalabilidad_logro.png
"""
import argparse
from pathlib import Path

import matplotlib as mpl
import matplotlib.pyplot as plt
import pandas as pd

BLUE, INK, INK2, MUTED, GRID, SHADE = "#2a78d6", "#0b0b0b", "#52514e", "#8a8984", "#e6e5e1", "#f4f3f0"

ZONAS = [(0.5, 6.5, "núcleos P\n(1 hilo c/u)"), (6.5, 12.5, "hyperthreading\nen núcleos P"), (12.5, 20.5, "núcleos E")]


def estilo():
    mpl.rcParams.update({
        "font.size": 10, "axes.edgecolor": GRID, "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
        "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8, "axes.axisbelow": True,
        "axes.spines.top": False, "axes.spines.right": False, "axes.titleweight": "bold",
        "axes.titlesize": 11, "axes.titlecolor": INK, "axes.titlelocation": "left",
        "savefig.dpi": 220, "savefig.bbox": "tight", "lines.linewidth": 2,
    })


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv", default="results/sweep_riesgo.csv")
    ap.add_argument("--out", default="docs/PC3/figuras/fig_escalabilidad.png")
    ap.add_argument("--arboles", type=int, default=50, help="árboles usados en el barrido (solo para el título)")
    ap.add_argument("--sin-zonas", action="store_true", help="no sombrear las zonas de núcleos P / HT / E")
    args = ap.parse_args()

    df = pd.read_csv(args.csv)
    w, seq = df.workers, df.secuencial_s.iloc[0]
    max_sp = df.speedup.max()
    w90 = int(df.loc[df.speedup >= 0.9 * max_sp, "workers"].min())
    eq = df.loc[df.equilibrio.astype(str).str.lower() == "true", "workers"]

    estilo()
    fig, axs = plt.subplots(1, 3, figsize=(13, 3.9))
    for ax in axs:
        if not args.sin_zonas:
            for i, (a, b, _) in enumerate(ZONAS):
                if i % 2 == 0:
                    ax.axvspan(a, b, color=SHADE, zorder=0, lw=0)
        ax.set_xlim(0.5, w.max() + 0.5)
        ax.set_xticks([1] + list(range(2, int(w.max()) + 1, 2)))
        ax.set_xlabel("Workers (goroutines)")

    ax = axs[0]
    ax.plot(w, df.media_recortada_s, color=BLUE, marker="o", ms=4)
    ax.axhline(seq, color=MUTED, lw=1, ls="--")
    ax.text(w.max() + 0.3, seq, f"secuencial {seq:.2f} s", ha="right", va="bottom", color=INK2, fontsize=9)
    ax.set_title(f"Tiempo de entrenamiento ({args.arboles} árboles)")
    ax.set_ylabel("Segundos (media recortada)")
    ax.set_ylim(0, seq * 1.12)

    ax = axs[1]
    ax.plot(w, w, color=MUTED, lw=1, ls="--")
    ax.text(max_sp * 0.95, max_sp * 1.05, "ideal (lineal)", color=INK2, fontsize=9)
    ax.plot(w, df.speedup, color=BLUE, marker="o", ms=4)
    ax.set_ylim(0, max_sp * 1.35)
    ax.set_title("Speedup")
    ax.set_ylabel("T secuencial / T concurrente")
    sp90 = float(df.loc[w == w90, "speedup"].iloc[0])
    ax.annotate(f"{w90} workers: {sp90:.2f}x\n({sp90 / max_sp:.0%} del máximo)", (w90, sp90), (w90, sp90 - max_sp * 0.32),
                ha="center", fontsize=8.5, color=INK, arrowprops=dict(arrowstyle="-", color=INK2, lw=0.8))
    if len(eq):
        e = int(eq.iloc[0])
        spe = float(df.loc[w == e, "speedup"].iloc[0])
        ax.annotate(f"equilibrio (5%):\n{e} workers, {spe:.2f}x", (e, spe), (e, spe - max_sp * 0.35),
                    ha="center", fontsize=8.5, color=INK, arrowprops=dict(arrowstyle="-", color=INK2, lw=0.8))

    ax = axs[2]
    ax.bar(w, df.eficiencia, color=BLUE, width=0.7)
    ax.set_ylim(0, 1.2)
    ax.set_title("Eficiencia (speedup / workers)")
    if not args.sin_zonas:
        for a, b, label in ZONAS:
            ax.text((a + b) / 2, 1.12, label, ha="center", va="center", fontsize=8, color=INK2)

    fig.tight_layout()
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.out)
    print(f"Speedup máximo: {max_sp:.2f}x con {int(df.loc[df.speedup.idxmax(), 'workers'])} workers")
    print(f"90% del máximo desde: {w90} workers ({sp90:.2f}x)")
    if len(eq):
        print(f"Punto de equilibrio (Go, 5%): {int(eq.iloc[0])} workers")
    print(f"Figura guardada en {args.out}")


if __name__ == "__main__":
    main()
