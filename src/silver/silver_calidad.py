import argparse
import json
import os
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F

import pandas as pd
from pathlib import Path


def crear_spark(nombre_app: str = "silver_transform") -> SparkSession:
    return (
        SparkSession.builder
        .appName(nombre_app)
        .master("local[*]")
        .getOrCreate()
    )


def normalizar_texto(col: F.Column) -> F.Column:
    return F.upper(F.trim(col))


def col_segura(df: DataFrame, nombre: str) -> F.Column:
    # Si no existe la columna, devolvemos NULL y lo mandamos a cuarentena por reglas de calidad
    return F.col(nombre) if nombre in df.columns else F.lit(None)


def castear_campos(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("event_time_ts", F.to_timestamp(col_segura(df, "event_time")))
        .withColumn("ingestion_date_dt", F.to_date(col_segura(df, "ingestion_date")))
        .withColumn("event_id", col_segura(df, "event_id").cast("string"))
        .withColumn("loan_id", col_segura(df, "loan_id").cast("string"))
        .withColumn("customer_id", col_segura(df, "customer_id").cast("string"))
        .withColumn("event_type", normalizar_texto(col_segura(df, "event_type").cast("string")))
        .withColumn("loan_status", normalizar_texto(col_segura(df, "loan_status").cast("string")))
        .withColumn("region", normalizar_texto(col_segura(df, "region").cast("string")))
        .withColumn("channel", normalizar_texto(col_segura(df, "channel").cast("string")))
        .withColumn("product_type", normalizar_texto(col_segura(df, "product_type").cast("string")))
        .withColumn("installment_number", col_segura(df, "installment_number").cast("int"))
        .withColumn("term_months", col_segura(df, "term_months").cast("int"))
        .withColumn("days_past_due", col_segura(df, "days_past_due").cast("int"))
        .withColumn("installment_amount", col_segura(df, "installment_amount").cast("double"))
        .withColumn("principal_amount", col_segura(df, "principal_amount").cast("double"))
        .withColumn("outstanding_balance", col_segura(df, "outstanding_balance").cast("double"))
        .withColumn("interest_rate", col_segura(df, "interest_rate").cast("double"))
        .withColumn("event_date", F.to_date(F.col("event_time_ts")))
        .withColumn("ingestion_date_filled", F.coalesce(F.col("ingestion_date_dt"), F.col("event_date")))
    )


def quitar_duplicados(df: DataFrame) -> (DataFrame, int):
    if "event_id" not in df.columns:
        return df, 0

    orden = []
    if "ingestion_ts" in df.columns:
        orden.append(F.col("ingestion_ts").desc())
    if "batch_id" in df.columns:
        orden.append(F.col("batch_id").desc())
    if not orden:
        orden = [F.col("event_time_ts").desc_nulls_last()]

    w = Window.partitionBy("event_id").orderBy(*orden)

    df_rankeado = df.withColumn("_rn", F.row_number().over(w))
    duplicados = df_rankeado.filter(F.col("_rn") > 1).count()

    df_sin_dupes = df_rankeado.filter(F.col("_rn") == 1).drop("_rn")
    return df_sin_dupes, duplicados


def reglas_calidad(df: DataFrame) -> dict:
    reglas = {}

    # TODO: cuando veamos los valores únicos reales, afinamos catálogos de channel/product_type si hace falta.
    tipos_evento = ["PAYMENT", "DISBURSEMENT", "DELINQUENCY", "CHARGEOFF", "CLOSE"]
    estados = ["ACTIVE", "DELINQUENT", "CHARGEOFF", "CLOSED"]

    reglas["null_event_id"] = col_segura(df, "event_id").isNull()
    reglas["null_event_time"] = F.col("event_time_ts").isNull()
    reglas["null_loan_id"] = col_segura(df, "loan_id").isNull()
    reglas["null_customer_id"] = col_segura(df, "customer_id").isNull()
    reglas["null_region"] = col_segura(df, "region").isNull()
    reglas["null_installment_amount"] = col_segura(df, "installment_amount").isNull()

    reglas["invalid_event_type"] = (
        col_segura(df, "event_type").isNotNull() & (~col_segura(df, "event_type").isin(tipos_evento))
    )
    reglas["invalid_loan_status"] = (
        col_segura(df, "loan_status").isNotNull() & (~col_segura(df, "loan_status").isin(estados))
    )

    reglas["rate_out_of_range"] = (
        col_segura(df, "interest_rate").isNotNull()
        & ((col_segura(df, "interest_rate") < F.lit(0.0)) | (col_segura(df, "interest_rate") > F.lit(1.0)))
    )
    reglas["negative_days_past_due"] = (
        col_segura(df, "days_past_due").isNotNull() & (col_segura(df, "days_past_due") < F.lit(0))
    )

    reglas["disbursement_installment_number_should_be_0"] = (
        (col_segura(df, "event_type") == F.lit("DISBURSEMENT"))
        & col_segura(df, "installment_number").isNotNull()
        & (col_segura(df, "installment_number") != F.lit(0))
    )

    reglas["payment_installment_number_should_be_gt_0"] = (
        (col_segura(df, "event_type") == F.lit("PAYMENT"))
        & col_segura(df, "installment_number").isNotNull()
        & (col_segura(df, "installment_number") <= F.lit(0))
    )

    return reglas


def aplicar_reglas(df: DataFrame, reglas: dict) -> DataFrame:
    banderas = []
    for nombre_regla, falla_cuando in reglas.items():
        banderas.append(F.when(falla_cuando, F.lit(nombre_regla)).otherwise(F.lit(None)))

    df2 = df.withColumn("dq_failed_rules_raw", F.array(*banderas))
    df2 = df2.withColumn("dq_failed_rules", F.expr("filter(dq_failed_rules_raw, x -> x is not null)")) \
             .drop("dq_failed_rules_raw")

    df2 = df2.withColumn("dq_is_valid", F.size(F.col("dq_failed_rules")) == 0)
    df2 = df2.withColumn("dq_reason_primary", F.expr("element_at(dq_failed_rules, 1)"))
    return df2


def escribir_json(ruta: str, payload: dict) -> None:
    os.makedirs(os.path.dirname(ruta), exist_ok=True)
    with open(ruta, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def main():
    parser = argparse.ArgumentParser(description="Construye Silver (validos + cuarentena) desde Bronze.")
    parser.add_argument("--ruta_bronze", required=True)
    parser.add_argument("--ruta_validos", required=True)
    parser.add_argument("--ruta_cuarentena", required=True)
    parser.add_argument("--ruta_metricas", required=True)
    args = parser.parse_args()

    spark = crear_spark()

    inicio = datetime.utcnow().isoformat() + "Z"

    # --- LECTURA BRONZE (pandas -> spark) ---
    archivos_csv = list(Path(args.ruta_bronze).glob("*.csv"))
    if not archivos_csv:
        raise ValueError(f"No se encontraron CSV en {args.ruta_bronze}")

    df_bronze_pd = pd.concat(
        [pd.read_csv(f) for f in archivos_csv],
        ignore_index=True
    )

    df_bronze = spark.createDataFrame(df_bronze_pd)


    conteo_bronze = df_bronze.count()

    df_tipado = castear_campos(df_bronze)

    df_sin_dupes, duplicados_detectados = quitar_duplicados(df_tipado)

    reglas = reglas_calidad(df_sin_dupes)
    df_con_calidad = aplicar_reglas(df_sin_dupes, reglas)

    validos = df_con_calidad.filter(F.col("dq_is_valid") == F.lit(True))
    cuarentena = df_con_calidad.filter(F.col("dq_is_valid") == F.lit(False))

    conteo_validos = validos.count()
    conteo_cuarentena = cuarentena.count()

    invalidos_por_regla = {}
    for nombre_regla in reglas.keys():
        invalidos_por_regla[nombre_regla] = cuarentena.filter(
            F.array_contains(F.col("dq_failed_rules"), nombre_regla)
        ).count()

    metricas = {
        "run_started_at_utc": inicio,
        "rutas": {
            "bronze": args.ruta_bronze,
            "silver_validos": args.ruta_validos,
            "silver_cuarentena": args.ruta_cuarentena
        },
        "conteos": {
            "bronze": conteo_bronze,
            "silver_validos": conteo_validos,
            "silver_cuarentena": conteo_cuarentena,
            "duplicados_detectados": duplicados_detectados
        },
        "invalidos_por_regla": invalidos_por_regla
    }

    def escribir_csv(df_spark: DataFrame, ruta: str, nombre: str):
        Path(ruta).mkdir(parents=True, exist_ok=True)
        df_spark.toPandas().to_csv(
            Path(ruta) / nombre,
            index=False
        )

    escribir_csv(validos, args.ruta_validos, "credit_events_valid.csv")
    escribir_csv(cuarentena, args.ruta_cuarentena, "credit_events_quarantine.csv")


    escribir_json(args.ruta_metricas, metricas)

    spark.stop()


if __name__ == "__main__":
    main()
