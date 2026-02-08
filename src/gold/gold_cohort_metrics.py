import argparse
from pathlib import Path

import pandas as pd


def main():
    parser = argparse.ArgumentParser(description="Gold: métricas por cohorte (mes).")
    parser.add_argument("--ruta_silver_validos", required=True)
    parser.add_argument("--ruta_gold", required=True)
    args = parser.parse_args()

    in_path = Path(args.ruta_silver_validos)
    out_dir = Path(args.ruta_gold)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(in_path)

    # Timestamp de evento
    if "event_time_ts" in df.columns:
        df["event_time_ts"] = pd.to_datetime(df["event_time_ts"], errors="coerce")
    elif "event_time" in df.columns:
        df["event_time_ts"] = pd.to_datetime(df["event_time"], errors="coerce")

    if "event_time_ts" not in df.columns:
        raise ValueError("No existe event_time_ts/event_time para calcular cohorte.")

    df["event_month"] = df["event_time_ts"].dt.to_period("M").astype(str)

    # Métricas básicas
    metrics = df.groupby("event_month").agg(
        events=("event_id", "count") if "event_id" in df.columns else ("loan_id", "count"),
        unique_loans=("loan_id", "nunique"),
        unique_customers=("customer_id", "nunique") if "customer_id" in df.columns else ("loan_id", "nunique"),
        avg_outstanding_balance=("outstanding_balance", "mean") if "outstanding_balance" in df.columns else ("loan_id", "size"),
        avg_days_past_due=("days_past_due", "mean") if "days_past_due" in df.columns else ("loan_id", "size"),
    ).reset_index()

    # Si algunas columnas no existían, quedaron con agregación dummy; límpialas si aplica
    for col in ["avg_outstanding_balance", "avg_days_past_due"]:
        if col in metrics.columns:
            metrics[col] = pd.to_numeric(metrics[col], errors="coerce")

    metrics.to_csv(out_dir / "cohort_metrics.csv", index=False)
    print(f"[OK] Gold cohort_metrics generado: {out_dir / 'cohort_metrics.csv'} | filas={len(metrics)}")


if __name__ == "__main__":
    main()
