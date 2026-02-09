import argparse
from pathlib import Path
import base64
from io import BytesIO
from datetime import datetime

import pandas as pd
import matplotlib.pyplot as plt


def fig_to_base64(fig) -> str:
    buf = BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode("utf-8")


def _format_number_columns_for_html(df: pd.DataFrame) -> pd.DataFrame:
    """
    Devuelve una copia del DF con columnas numéricas formateadas como texto
    para evitar notación científica en HTML y mejorar legibilidad.
    """
    df2 = df.copy()

    money_like = {
        "Saldo pendiente",
        "Saldo promedio",
        "Monto de cuota",
        "Monto principal",
        # por si vienen nombres técnicos
        "outstanding_balance",
        "avg_outstanding_balance",
        "installment_amount",
        "principal_amount",
    }
    int_like = {
        "Eventos",
        "Créditos únicos",
        "Clientes únicos",
        "Días en mora",
        # por si vienen nombres técnicos
        "events",
        "unique_loans",
        "unique_customers",
        "days_past_due",
    }

    for col in df2.columns:
        if col in money_like:
            df2[col] = pd.to_numeric(df2[col], errors="coerce").map(
                lambda v: "" if pd.isna(v) else f"{v:,.2f}"
            )
        elif col in int_like:
            df2[col] = pd.to_numeric(df2[col], errors="coerce").map(
                lambda v: "" if pd.isna(v) else f"{int(v):,}"
            )
        elif str(col).lower().startswith("promedio") or str(col).startswith("avg_"):
            df2[col] = pd.to_numeric(df2[col], errors="coerce").map(
                lambda v: "" if pd.isna(v) else f"{v:,.2f}"
            )

    return df2


CREDIT_STATE_COLS = {
    "loan_id": "Crédito (ID)",
    "customer_id": "Cliente (ID)",
    "event_time_ts": "Fecha y hora del evento",
    "event_type": "Tipo de evento",
    "loan_status": "Estado del crédito",
    "outstanding_balance": "Saldo pendiente",
    "days_past_due": "Días en mora",
    "region": "Región",
    "channel": "Canal",
    "product_type": "Tipo de producto",
}

COHORT_COLS = {
    "event_month": "Mes",
    "events": "Eventos",
    "unique_loans": "Créditos únicos",
    "unique_customers": "Clientes únicos",
    "avg_outstanding_balance": "Saldo promedio",
    "avg_days_past_due": "Mora promedio (días)",
}


def _safe_read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"No se encontró el archivo requerido: {path}")
    return pd.read_csv(path)


def main():
    parser = argparse.ArgumentParser(description="Genera un reporte HTML (en español) desde la capa Gold.")
    parser.add_argument("--ruta_gold", required=True, help="Ruta a la carpeta Gold (ej: data/gold)")
    parser.add_argument("--output_html", required=True, help="Ruta del HTML de salida (ej: data/report/reporte.html)")
    args = parser.parse_args()

    gold_dir = Path(args.ruta_gold)
    out_html = Path(args.output_html)
    out_html.parent.mkdir(parents=True, exist_ok=True)

    # --- Lectura Gold ---
    credit_state_path = gold_dir / "credit_state.csv"
    cohort_path = gold_dir / "cohort_metrics.csv"

    credit_state = _safe_read_csv(credit_state_path)
    cohort = _safe_read_csv(cohort_path)

    # Orden temporal para gráficos (YYYY-MM ordena bien como string)
    if "event_month" in cohort.columns:
        cohort = cohort.sort_values("event_month")

    # --- Gráficos ---
    fig1 = plt.figure()
    plt.plot(cohort["event_month"], cohort["unique_loans"])
    plt.xticks(rotation=45, ha="right")
    plt.title("Créditos únicos por mes")
    img1 = fig_to_base64(fig1)

    img2 = None
    if "avg_days_past_due" in cohort.columns:
        fig2 = plt.figure()
        plt.plot(cohort["event_month"], cohort["avg_days_past_due"])
        plt.xticks(rotation=45, ha="right")
        plt.title("Promedio de días en mora por mes")
        img2 = fig_to_base64(fig2)

    # --- Resumen ejecutivo (solo español) ---
    resumen = {
        "Registros en estado de crédito (Gold)": len(credit_state),
        "Filas de métricas por cohorte (Gold)": len(cohort),
        "Generado (UTC)": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "Fuente": "Capa Gold (CSV) + visualización embebida en HTML",
    }

    # --- Tablas (renombradas + formato legible) ---
    credit_state_preview = credit_state.head(20).rename(columns=CREDIT_STATE_COLS)
    cohort_rep = cohort.rename(columns=COHORT_COLS)

    credit_state_preview = _format_number_columns_for_html(credit_state_preview)
    cohort_rep = _format_number_columns_for_html(cohort_rep)

    # --- Estilos (simple pero prolijo) ---
    style = """
    <style>
      body { font-family: Arial, sans-serif; margin: 24px; color: #111; }
      h1 { margin: 0 0 6px 0; }
      .meta { color: #555; margin: 0 0 18px 0; }
      .card { border: 1px solid #eee; border-radius: 10px; padding: 12px 14px; margin: 12px 0 18px 0; background: #fcfcfc; }
      table { border-collapse: collapse; width: 100%; margin: 10px 0 14px 0; font-size: 14px; }
      th, td { border: 1px solid #ddd; padding: 8px; vertical-align: top; }
      th { background: #f4f4f4; text-align: left; }
      tr:nth-child(even) { background: #fafafa; }
      img { max-width: 980px; width: 100%; height: auto; border: 1px solid #eee; padding: 6px; background: #fff; border-radius: 8px; }
      .grid { display: grid; grid-template-columns: 1fr; gap: 14px; }
      @media (min-width: 900px) { .grid { grid-template-columns: 1fr 1fr; } }
      .muted { color: #666; font-size: 13px; }
      .pill { display:inline-block; padding: 2px 8px; border-radius: 999px; background:#eef2ff; border:1px solid #dbeafe; font-size:12px; color:#1f2937; }
    </style>
    """

    html_parts = []
    html_parts.append(
        f"<html><head><meta charset='utf-8'><title>Reporte de Eventos de Crédito</title>{style}</head><body>"
    )

    html_parts.append("<h1>Reporte de Eventos de Crédito</h1>")
    html_parts.append("<div class='meta'><span class='pill'>Gold → Reporte</span> Documento de lectura ejecutiva (entendible por negocio)</div>")

    html_parts.append("<div class='card'><h2>Resumen ejecutivo</h2>")
    html_parts.append(pd.DataFrame([resumen]).to_html(index=False))
    html_parts.append("<div class='muted'>Nota: Los nombres técnicos se mantienen en las capas de datos; el reporte los traduce a lenguaje de negocio.</div>")
    html_parts.append("</div>")

    html_parts.append("<h2>Gráficos</h2>")
    html_parts.append("<div class='grid'>")
    html_parts.append(f"<div class='card'><h3>Créditos únicos por mes</h3><img src='data:image/png;base64,{img1}'/></div>")
    if img2:
        html_parts.append(f"<div class='card'><h3>Promedio de días en mora por mes</h3><img src='data:image/png;base64,{img2}'/></div>")
    html_parts.append("</div>")

    html_parts.append("<div class='card'><h2>Muestra – Estado actual de los créditos (primeras 20 filas)</h2>")
    html_parts.append(credit_state_preview.to_html(index=False))
    html_parts.append("</div>")

    html_parts.append("<div class='card'><h2>Métricas por cohorte (mensual)</h2>")
    html_parts.append(cohort_rep.to_html(index=False))
    html_parts.append("</div>")

    html_parts.append("</body></html>")

    out_html.write_text("\n".join(html_parts), encoding="utf-8")
    print(f"[OK] Reporte generado: {out_html}")


if __name__ == "__main__":
    main()
