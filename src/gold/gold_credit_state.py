import argparse
from pathlib import Path

import pandas as pd


def main():
    parser = argparse.ArgumentParser(description="Gold: estado actual por loan_id (ultimo evento).")
    parser.add_argument("--ruta_silver_validos", required=True, help="CSV de validos (Silver)")
    parser.add_argument("--ruta_gold", required=True, help="Carpeta salida Gold")
    args = parser.parse_args()

    in_path = Path(args.ruta_silver_validos)
    out_dir = Path(args.ruta_gold)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(in_path)

    # Normaliza timestamp si existe
    if "event_time_ts" in df.columns:
        df["event_time_ts"] = pd.to_datetime(df["event_time_ts"], errors="coerce")
    elif "event_time" in df.columns:
        df["event_time_ts"] = pd.to_datetime(df["event_time"], errors="coerce")

    required = {"loan_id", "event_time_ts"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas para Gold credit_state: {sorted(missing)}")

    # Ultimo evento por loan_id
    df = df.sort_values(["loan_id", "event_time_ts"], ascending=[True, False])
    latest = df.drop_duplicates(subset=["loan_id"], keep="first")

    cols = [c for c in ["loan_id", "customer_id", "event_time_ts", "event_type", "loan_status",
                       "outstanding_balance", "days_past_due", "region", "channel", "product_type"]
            if c in latest.columns]

    latest[cols].to_csv(out_dir / "credit_state.csv", index=False)
    print(f"[OK] Gold credit_state generado: {out_dir / 'credit_state.csv'} | filas={len(latest)}")


if __name__ == "__main__":
    main()
