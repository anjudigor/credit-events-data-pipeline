import argparse
from pathlib import Path
import base64
from io import BytesIO

import pandas as pd
import matplotlib.pyplot as plt


def fig_to_base64(fig) -> str:
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode("utf-8")


def main():
    parser = argparse.ArgumentParser(description="Genera reporte HTML desde Gold.")
    parser.add_argument("--ruta_gold", required=True)
    parser.add_argument("--output_html", required=True)
    args = parser.parse_args()

    gold_dir = Path(args.ruta_gold)
    out_html = Path(args.output_html)
    out_html.parent.mkdir(parents=True, exist_ok=True)

    credit_state = pd.read_csv(gold_dir / "credit_state.csv")
    cohort = pd.read_csv(gold_dir / "cohort_metrics.csv")

    # Grafico 1: loans por mes
    fig1 = plt.figure()
    plt.plot(cohort["event_month"], cohort["unique_loans"])
    plt.xticks(rotation=45, ha="right")
    plt.title("Unique loans por mes")
    img1 = fig_to_base64(fig1)

    # Grafico 2: promedio days_past_due por mes (si existe)
    img2 = None
    if "avg_days_past_due" in cohort.columns:
        fig2 = plt.figure()
        plt.plot(cohort["event_month"], cohort["avg_days_past_due"])
        plt.xticks(rotation=45, ha="right")
        plt.title("Promedio days_past_due por mes")
        img2 = fig_to_base64(fig2)

    resumen = {
        "gold_credit_state_rows": len(credit_state),
        "gold_cohort_rows": len(cohort),
    }

    html_parts = []
    html_parts.append("<html><head><meta charset='utf-8'><title>Credit Events Report</title></head><body>")
    html_parts.append("<h1>Reporte - Credit Events Pipeline</h1>")
    html_parts.append("<h2>Resumen</h2>")
    html_parts.append(pd.DataFrame([resumen]).to_html(index=False))

    html_parts.append("<h2>Gráficos</h2>")
    html_parts.append(f"<h3>Unique loans por mes</h3><img src='data:image/png;base64,{img1}'/>")
    if img2:
        html_parts.append(f"<h3>Promedio days_past_due por mes</h3><img src='data:image/png;base64,{img2}'/>")

    html_parts.append("<h2>Vista previa Gold - credit_state (primeras 20 filas)</h2>")
    html_parts.append(credit_state.head(20).to_html(index=False))

    html_parts.append("<h2>Vista previa Gold - cohort_metrics</h2>")
    html_parts.append(cohort.to_html(index=False))

    html_parts.append("</body></html>")

    out_html.write_text("\n".join(html_parts), encoding="utf-8")
    print(f"[OK] Reporte generado: {out_html}")


if __name__ == "__main__":
    main()
