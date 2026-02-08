import argparse
from pathlib import Path

import pandas as pd


def main():
    parser = argparse.ArgumentParser(description="Convierte Bronze (CSV) a Parquet.")
    parser.add_argument("--ruta_bronze_csv", required=True, help="Carpeta con CSVs de Bronze")
    parser.add_argument("--ruta_bronze_parquet", required=True, help="Carpeta destino Parquet")
    args = parser.parse_args()

    in_dir = Path(args.ruta_bronze_csv)
    out_dir = Path(args.ruta_bronze_parquet)
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(in_dir.glob("*.csv"))
    if not csv_files:
        raise ValueError(f"No se encontraron CSV en {in_dir}")

    df = pd.concat((pd.read_csv(f) for f in csv_files), ignore_index=True)

    # Si ingestion_ts viene como texto, intenta parsearlo
    if "ingestion_ts" in df.columns:
        df["ingestion_ts"] = pd.to_datetime(df["ingestion_ts"], errors="coerce")

    # Particionado sencillo por ingestion_date si existe; si no, por fecha derivada de ingestion_ts
    if "ingestion_date" in df.columns:
        df["ingestion_date_part"] = df["ingestion_date"].astype(str)
    elif "ingestion_ts" in df.columns:
        df["ingestion_date_part"] = df["ingestion_ts"].dt.date.astype(str)
    else:
        df["ingestion_date_part"] = "unknown"

    for part, g in df.groupby("ingestion_date_part"):
        part_dir = out_dir / f"ingestion_date={part}"
        part_dir.mkdir(parents=True, exist_ok=True)
        g.drop(columns=["ingestion_date_part"], errors="ignore").to_parquet(
            part_dir / "data.parquet",
            index=False
        )

    print(f"[OK] Parquet generado en: {out_dir} | archivos={len(csv_files)} | filas={len(df)}")


if __name__ == "__main__":
    main()
