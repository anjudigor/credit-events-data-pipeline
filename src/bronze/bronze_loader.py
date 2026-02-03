import argparse
from datetime import datetime
import pandas as pd
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_csv", required=True)
    parser.add_argument("--output_path", required=True)
    args = parser.parse_args()

    batch_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    df = pd.read_csv(args.input_csv)

    df["batch_id"] = batch_id
    df["ingestion_ts"] = datetime.utcnow()

    output_dir = Path(args.output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"credit_events_{batch_id}.csv"
    df.to_csv(output_file, index=False)

    print(f"[OK] Bronze generado: {output_file}")
    print(f"[INFO] Rows: {len(df)} | batch_id={batch_id}")


if __name__ == "__main__":
    main()
