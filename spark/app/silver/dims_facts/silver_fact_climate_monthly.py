"""
silver_fact_climate_monthly.py

Job de modelo Silver para construir la tabla de hechos climate_monthly
a partir de datos de clima + dimensiones
de provincia y fecha.

Tabla generada: climate_monthly

Columnas finales:
  - climate_monthly_id       PK surrogate (BIGINT)
  - province_key             FK INT
  - date_key                 FK INT      (último día del mes)
  - precip_total_mm          DECIMAL(8,2)
  - temp_max_avg_c           DECIMAL(5,2)
  - temp_min_avg_c           DECIMAL(5,2)
  - et0_total_mm             DECIMAL(8,2)
  - et0_avg_mm               DECIMAL(8,2)
  - dry_month_flag           BOOLEAN
  - heavy_rain_month_flag    BOOLEAN

Escribe con overwrite dinámico por partición (province_key, date_key).
"""

import os
from typing import Optional

from pyspark.sql import SparkSession, DataFrame, functions as F

from base_silver_model_job import (
    DFMap,
    run_silver_model_job,
    create_spark_session,
)

# ==========================
# CONFIG: BUCKET Y PATHS
# ==========================

# Bucket base 
BASE_BUCKET = os.getenv("BASE_BUCKET", "henry-pf-g2-huella-hidrica")
S3_BASE = f"s3a://{BASE_BUCKET}"

# Datos de clima en Silver
CLIMATE_DAILY_PATH = f"{S3_BASE}/silver/climate_monthly/"

# Ruta base del modelo Silver (dims + facts)
SILVER_MODEL_BASE_PATH = f"{S3_BASE}/silver/model"

# ==========================
# UMBRALES PARA LOS FLAGS
# ==========================
DRY_MONTH_TOTAL_MM_THRESHOLD = 10.0          # mm/mes → mes seco si llueve menos que esto
HEAVY_RAIN_MONTH_TOTAL_MM_THRESHOLD = 150.0  # mm/mes → mes de lluvia fuerte

# ==========================
# RANGO PARA RUNS INCREMENTALES
# ==========================
PROCESS_YEAR_ENV = os.getenv("PROCESS_YEAR")    # ej. "2024"
PROCESS_MONTH_ENV = os.getenv("PROCESS_MONTH")  # ej. "11"

process_year: Optional[int] = int(PROCESS_YEAR_ENV) if PROCESS_YEAR_ENV else None
process_month: Optional[int] = int(PROCESS_MONTH_ENV) if PROCESS_MONTH_ENV else None

print("============================================================")
print(f"[FACT CLIMATE MONTHLY CONFIG] BASE_BUCKET   = {BASE_BUCKET}")
print(f"[FACT CLIMATE MONTHLY CONFIG] S3_BASE       = {S3_BASE}")
print(f"[FACT CLIMATE MONTHLY CONFIG] PROCESS_YEAR  = {process_year}")
print(f"[FACT CLIMATE MONTHLY CONFIG] PROCESS_MONTH = {process_month}")
print("============================================================")


# =============================================================
# 1) READ_SOURCES
# =============================================================
def read_sources(spark: SparkSession) -> DFMap:
    """
    Lee:
      - climate_daily: clima base 
      - province_dim: dimensión de provincia
      - date_dim: dimensión de tiempo

    """

    # Dataset de clima en Silver
    print(f"[READ] Climate base desde: {CLIMATE_DAILY_PATH}")
    climate_daily = spark.read.parquet(CLIMATE_DAILY_PATH)

    if process_year is not None:
        print(f"[READ] Filtrando climate_daily a year = {process_year}")
        climate_daily = climate_daily.filter(F.col("year") == process_year)

    if process_month is not None:
        print(f"[READ] Filtrando climate_daily a month = {process_month}")
        climate_daily = climate_daily.filter(F.col("month") == process_month)

    # Dimensiones del modelo Silver
    province_dim = spark.read.parquet(f"{SILVER_MODEL_BASE_PATH}/province")
    date_dim = spark.read.parquet(f"{SILVER_MODEL_BASE_PATH}/date")

    return {
        "climate_daily": climate_daily,
        "province_dim": province_dim,
        "date_dim": date_dim,
    }


# =============================================================
# 2) FUNCIÓN AUXILIAR PARA CONSTRUIR LA FACT MENSUAL
# =============================================================
def build_climate_monthly_fact(
    df_climate_daily: DataFrame,
    df_province: DataFrame,
    df_date: DataFrame,
) -> DataFrame:
    """
    Construye la tabla de hechos climate_monthly a partir de datos base.

    Grano final: una fila por (province_key, month).
    """

    # 0) Agregación BASE - MENSUAL por país/provincia/año/mes
    monthly = (
        df_climate_daily
        .groupBy("country_iso3", "province_name", "year", "month")
        .agg(
            # lluvia total en el mes
            F.sum("precip_total_mm").alias("precip_total_mm"),
            # promedio de las temperaturas (sobre los registros del mes)
            F.avg("temp_max_avg_c").alias("temp_max_avg_c"),
            F.avg("temp_min_avg_c").alias("temp_min_avg_c"),
            # ET0 total y promedio mensual
            F.sum("et0_total_mm").alias("et0_total_mm"),
            F.avg("et0_avg_mm").alias("et0_avg_mm"),
        )
    )

    # 0.1) Flags de mes seco / mes de lluvia fuerte basados en la lluvia total mensual
    monthly = (
        monthly
        .withColumn(
            "dry_month_flag",
            F.col("precip_total_mm") < F.lit(DRY_MONTH_TOTAL_MM_THRESHOLD),
        )
        .withColumn(
            "heavy_rain_month_flag",
            F.col("precip_total_mm") > F.lit(HEAVY_RAIN_MONTH_TOTAL_MM_THRESHOLD),
        )
    )

    # 1) Mapear province_key usando country_iso3 + province_name
    base = (
        monthly.alias("m")
        .join(
            df_province.select(
                "province_key",
                "country_iso3",
                "province_name",
            ).alias("p"),
            on=["country_iso3", "province_name"],
            how="left",
        )
    )

    # 2) Obtener date_key del último día de cada mes desde dim_date
    #    (max(date_key) por (year, month))
    end_of_month_dates = (
        df_date
        .groupBy("year", "month")
        .agg(F.max("date_key").alias("date_key"))
        .distinct()
    )

    fact = (
        base.alias("b")
        .join(
            end_of_month_dates.alias("d"),
            on=["year", "month"],
            how="left",
        )
    )

    # 3) Casts finales a DECIMAL/BOOLEAN y surrogate key
    fact = (
        fact
        .withColumn(
            "climate_monthly_id",
            F.monotonically_increasing_id().cast("bigint"),
        )
        .withColumn(
            "precip_total_mm",
            F.col("precip_total_mm").cast("decimal(8, 2)"),
        )
        .withColumn(
            "temp_max_avg_c",
            F.col("temp_max_avg_c").cast("decimal(5, 2)"),
        )
        .withColumn(
            "temp_min_avg_c",
            F.col("temp_min_avg_c").cast("decimal(5, 2)"),
        )
        .withColumn(
            "et0_total_mm",
            F.col("et0_total_mm").cast("decimal(8, 2)"),
        )
        .withColumn(
            "et0_avg_mm",
            F.col("et0_avg_mm").cast("decimal(8, 2)"),
        )
        .withColumn(
            "dry_month_flag",
            F.col("dry_month_flag").cast("boolean"),
        )
        .withColumn(
            "heavy_rain_month_flag",
            F.col("heavy_rain_month_flag").cast("boolean"),
        )
    )

    # 4) Seleccionar columnas finales
    fact = fact.select(
        "climate_monthly_id",
        "province_key",
        "date_key",
        "precip_total_mm",
        "temp_max_avg_c",
        "temp_min_avg_c",
        "et0_total_mm",
        "et0_avg_mm",
        "dry_month_flag",
        "heavy_rain_month_flag",
    )

    return fact


# =============================================================
# 3) BUILD_DIMS y BUILD_FACTS
# =============================================================
def build_dims(sources: DFMap) -> DFMap:
    """
    Este job está dedicado solo a hechos (no crea dimensiones nuevas).
    """
    return {}


def build_facts(sources: DFMap, dims: DFMap) -> DFMap:
    """
    Construye la fact climate_monthly a partir de clima base + dims.
    """

    df_climate_daily = sources["climate_daily"]
    df_province = sources["province_dim"]
    df_date = sources["date_dim"]

    climate_monthly = build_climate_monthly_fact(
        df_climate_daily,
        df_province,
        df_date,
    )

    return {
        "climate_monthly": climate_monthly
    }


# =============================================================
# 4) WRITE_TABLES (OVERWRITE DINÁMICO)
# =============================================================
def write_tables(dims: DFMap, facts: DFMap, spark: SparkSession) -> None:
    """
    Escribe la tabla climate_monthly en Silver/model.

    """

    fact = facts.get("climate_monthly")
    if fact is None or fact.rdd.isEmpty():
        print("No se construyó la tabla climate_monthly o no hay filas. Nada que escribir.")
        return

    output_path = f"{SILVER_MODEL_BASE_PATH}/climate_monthly"
    print(f"[WRITE] Guardando climate_monthly en {output_path} ...")

    (
        fact.write
        .mode("overwrite")  # overwrite dinámico por partición
        .partitionBy("province_key", "date_key")
        .format("parquet")
        .save(output_path)
    )

    print("[WRITE] climate_monthly escrito (overwrite dinámico por partición).")


# =============================================================
# 5) RUN
# =============================================================
def run(
    spark: SparkSession,
    climate_daily_path: str = CLIMATE_DAILY_PATH,
    silver_model_base_path: str = SILVER_MODEL_BASE_PATH,
    process_year_param: Optional[int] = None,
    process_month_param: Optional[int] = None,
) -> None:
    """
    Entry point del job: permite sobreescribir rutas (tests) y
    recibir process_year / process_month desde el main.
    """
    global CLIMATE_DAILY_PATH, SILVER_MODEL_BASE_PATH, process_year, process_month

    # Rutas globales (por si se overridean en tests)
    CLIMATE_DAILY_PATH = climate_daily_path
    SILVER_MODEL_BASE_PATH = silver_model_base_path

    # Actualizar los parámetros de año/mes si vienen desde el main
    if process_year_param is not None:
        process_year = process_year_param

    if process_month_param is not None:
        process_month = process_month_param

    print(f"[FACT CLIMATE MONTHLY RUN] process_year={process_year}, process_month={process_month}")

    # Ejecutar el template genérico
    run_silver_model_job(
        spark_session=spark,
        read_sources_func=read_sources,
        build_dims_func=build_dims,
        build_facts_func=build_facts,
        write_tables_func=write_tables,
    )


if __name__ == "__main__":
    spark = create_spark_session(app_name="silver_fact_climate_monthly")
    run(spark)
    spark.stop()

