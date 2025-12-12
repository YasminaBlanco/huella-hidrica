"""
silver_fact_climate_annual.py

Job de modelo Silver para construir la tabla de hechos climate_annual
a partir de datos limpios de Clima Mensual + dimensiones del modelo Silver.

Tabla generada: climate_annual

Grain:
  - country_key
  - date_key (representa el año, usando el 31/12)

Columnas:
  - climate_annual_id    PK surrogate (BIGINT)
  - country_key          FK INT
  - date_key             FK INT  (YYYYMMDD, 31 de diciembre del año)
  - precip_total_mm_year DECIMAL(10,2)
  - precip_avg_mm_year   DECIMAL(10,2)
  - temp_max_avg_year    DECIMAL(5,2)
  - temp_min_avg_year    DECIMAL(5,2)
  - et0_total_mm_year    DECIMAL(8,2)
  - et0_avg_mm_year      DECIMAL(8,2)
  - dry_months           DECIMAL(10,2)
  - heavy_rain_months    DECIMAL(10,2)
  - drought_index        DECIMAL(6,3)
  - heavy_rain_index     DECIMAL(6,3)

Soporta dos modos:

- FULL:  si PROCESS_YEAR no está definido  recalcula todos los años.
- INCREMENTAL: si PROCESS_YEAR=YYYY solo recalcula ese año
  y sobrescribe solo las particiones de ese año (overwrite dinámico).
"""

import os
from typing import Optional

from pyspark.sql import SparkSession, DataFrame, functions as F, types as T

from base_silver_model_job import (
    DFMap,
    run_silver_model_job,
    create_spark_session,
)

# ==========================
# CONFIG: BUCKET Y RANGOS
# ==========================

# Bucket base
BASE_BUCKET = os.getenv("BASE_BUCKET", "henry-pf-g2-huella-hidrica")
S3_BASE = f"s3a://{BASE_BUCKET}"

# Dataset limpio de Clima Mensual (Silver clean)
SILVER_CLIMATE_CLEAN_PATH = f"{S3_BASE}/silver/climate_monthly/"

# Ruta base del modelo Silver (dims + facts)
SILVER_MODEL_BASE_PATH = f"{S3_BASE}/silver/model"

# Parámetro de año (opcional, para runs incrementales)
PROCESS_YEAR_ENV = os.getenv("PROCESS_YEAR")  # ej. "2024"
process_year: Optional[int] = int(PROCESS_YEAR_ENV) if PROCESS_YEAR_ENV else None

print("============================================================")
print(f"[FACT CLIMATE ANNUAL CONFIG] BASE_BUCKET  = {BASE_BUCKET}")
print(f"[FACT CLIMATE ANNUAL CONFIG] S3_BASE      = {S3_BASE}")
print(f"[FACT CLIMATE ANNUAL CONFIG] PROCESS_YEAR = {process_year}")
print("============================================================")


# =============================================================
# 1) READ_SOURCES
# =============================================================
def read_sources(spark: SparkSession) -> DFMap:
    """
    Lee:
      - climate_clean: datos de clima mensual.
      - country, date: dimensiones ya creadas por silver_dims_job.py.
    """

    print(f"[READ] Climate monthly clean desde: {SILVER_CLIMATE_CLEAN_PATH}")
    climate_clean = spark.read.parquet(SILVER_CLIMATE_CLEAN_PATH)

    if process_year is not None:
        print(f"[READ] Filtrando climate_clean a year = {process_year}")
        climate_clean = climate_clean.filter(F.col("year") == process_year)

    # Dimensiones del modelo Silver
    country_dim = spark.read.parquet(f"{SILVER_MODEL_BASE_PATH}/country")
    date_dim = spark.read.parquet(f"{SILVER_MODEL_BASE_PATH}/date")

    return {
        "climate_clean": climate_clean,
        "country_dim": country_dim,
        "date_dim": date_dim,
    }


# =============================================================
# 2) FUNCIÓN AUXILIAR PARA CONSTRUIR LA FACT
# =============================================================
def build_climate_annual_fact(
    df_climate: DataFrame,
    df_country: DataFrame,
    df_date: DataFrame,
) -> DataFrame:
    """
    Construye la tabla de hechos climate_annual.

    Realiza la agregación de métricas a nivel anual (country_iso3, year)
    desde los datos mensuales/provinciales de clima.
    """

    # Castear flags booleanos a entero para que SUM funcione sin errores
    df_climate_casteado = df_climate.withColumn(
        "dry_month_flag_int",
        F.col("dry_day_flag").cast(T.IntegerType()),
    ).withColumn(
        "heavy_rain_month_flag_int",
        F.col("heavy_rain_day_flag").cast(T.IntegerType()),
    )

    # 1) Agregación anual por (country_iso3, year)
    annual_base = df_climate_casteado.groupBy("country_iso3", "year").agg(
        # Precipitación
        F.sum("precip_total_mm").alias("precip_total_mm_year"),
        (F.sum("precip_total_mm") / F.lit(12)).alias("precip_avg_mm_year"),
        # Temperatura
        F.avg("temp_max_avg_c").alias("temp_max_avg_year"),
        F.avg("temp_min_avg_c").alias("temp_min_avg_year"),
        # Evapotranspiración
        F.sum("et0_total_mm").alias("et0_total_mm_year"),
        F.avg("et0_avg_mm").alias("et0_avg_mm_year"),
        # Contar meses secos / de lluvia intensa
        F.sum("dry_month_flag_int").alias("dry_months"),
        F.sum("heavy_rain_month_flag_int").alias("heavy_rain_months"),
        # Índices (meses extremos / 12)
        (F.sum("dry_month_flag_int") / F.lit(12)).alias("drought_index"),
        (F.sum("heavy_rain_month_flag_int") / F.lit(12)).alias("heavy_rain_index"),
    )

    base = annual_base

    # 2) JOIN #1: agregar country_key usando country_iso3
    base = base.alias("c_f").join(
        df_country.select("country_key", "country_iso3").alias("c_d"),
        on="country_iso3",
        how="left",
    )

    # 3) JOIN #2: mapear year - date_key anual (31/12)
    end_of_year_dates = (
        df_date.filter(F.date_format("date", "MM-dd") == "12-31")  # 31 de diciembre
        .select("year", "date_key")
        .distinct()
    )

    base = base.alias("c_f").join(
        end_of_year_dates.alias("d"),
        on="year",
        how="left",
    )

    # 4) Crear surrogate key para la fact (PK)
    fact = base.withColumn(
        "climate_annual_id",
        F.monotonically_increasing_id().cast("bigint"),
    )

    # 5) Tipos de datos para las métricas
    fact = (
        fact.withColumn(
            "precip_total_mm_year",
            F.col("precip_total_mm_year").cast("decimal(10, 2)"),
        )
        .withColumn(
            "precip_avg_mm_year",
            F.col("precip_avg_mm_year").cast("decimal(10, 2)"),
        )
        .withColumn(
            "temp_max_avg_year",
            F.col("temp_max_avg_year").cast("decimal(5, 2)"),
        )
        .withColumn(
            "temp_min_avg_year",
            F.col("temp_min_avg_year").cast("decimal(5, 2)"),
        )
        .withColumn(
            "et0_total_mm_year",
            F.col("et0_total_mm_year").cast("decimal(8, 2)"),
        )
        .withColumn(
            "et0_avg_mm_year",
            F.col("et0_avg_mm_year").cast("decimal(8, 2)"),
        )
        .withColumn(
            "dry_months",
            F.col("dry_months").cast("decimal(10, 2)"),
        )
        .withColumn(
            "heavy_rain_months",
            F.col("heavy_rain_months").cast("decimal(10, 2)"),
        )
        .withColumn(
            "drought_index",
            F.col("drought_index").cast("decimal(6, 3)"),
        )
        .withColumn(
            "heavy_rain_index",
            F.col("heavy_rain_index").cast("decimal(6, 3)"),
        )
    )

    # 6) Seleccionar columnas finales
    fact = fact.select(
        "climate_annual_id",
        "country_key",
        "date_key",
        "precip_total_mm_year",
        "precip_avg_mm_year",
        "temp_max_avg_year",
        "temp_min_avg_year",
        "et0_total_mm_year",
        "et0_avg_mm_year",
        "dry_months",
        "heavy_rain_months",
        "drought_index",
        "heavy_rain_index",
    )

    return fact


# =============================================================
# 3) BUILD_DIMS y BUILD_FACTS
# =============================================================
def build_dims(sources: DFMap) -> DFMap:
    """
    Este job está dedicado solo a hechos.
    No construye dimensiones nuevas.
    """
    return {}


def build_facts(sources: DFMap, dims: DFMap) -> DFMap:
    """
    Construye la fact climate_annual a partir de datos limpios + dimensiones.
    """
    df_climate = sources["climate_clean"]
    df_country = sources["country_dim"]
    df_date = sources["date_dim"]

    climate_annual_fact = build_climate_annual_fact(
        df_climate,
        df_country,
        df_date,
    )

    return {"climate_annual": climate_annual_fact}


# =============================================================
# 4) WRITE_TABLES (OVERWRITE DINÁMICO POR PARTICIÓN)
# =============================================================
def write_tables(dims: DFMap, facts: DFMap, spark: SparkSession) -> None:
    """
    Escribe la tabla climate_annual en Silver/model.

    Usamos:
      - mode("overwrite") + partitionBy("country_key", "date_key")

    cuando se procesa solo un año (PROCESS_YEAR),
    se sobrescriben únicamente las particiones de ese año.
    """

    fact = facts.get("climate_annual")
    if fact is None or fact.rdd.isEmpty():
        print(
            "No se construyó la tabla climate_annual o no hay filas. Nada que escribir."
        )
        return

    output_path = f"{SILVER_MODEL_BASE_PATH}/climate_annual"
    print(f"[WRITE] Guardando climate_annual en {output_path} ...")

    (
        fact.write.mode("overwrite")  # overwrite dinámico por partición
        .partitionBy("country_key", "date_key")
        .format("parquet")
        .save(output_path)
    )

    print("[WRITE] climate_annual escrito (overwrite dinámico por partición).")


# =============================================================
# 5) RUN
# =============================================================
def run(
    spark: SparkSession,
    climate_clean_path: str = SILVER_CLIMATE_CLEAN_PATH,
    silver_model_base_path: str = SILVER_MODEL_BASE_PATH,
    process_year_param: Optional[int] = None,
) -> None:
    """
    Ejecuta el flujo estándar del job de hechos anual.

    Parámetros:
      - climate_clean_path     : ruta al parquet de clima mensual limpio.
      - silver_model_base_path : ruta base del modelo Silver.
      - process_year_param     : año a reprocesar. Si es None, se usa
        el valor leído de PROCESS_YEAR; si ambos son None, es FULL histórico.
    """
    global SILVER_CLIMATE_CLEAN_PATH, SILVER_MODEL_BASE_PATH, process_year

    SILVER_CLIMATE_CLEAN_PATH = climate_clean_path
    SILVER_MODEL_BASE_PATH = silver_model_base_path

    if process_year_param is not None:
        process_year = process_year_param

    print(f"[FACT CLIMATE ANNUAL RUN] process_year (efectivo) = {process_year}")

    run_silver_model_job(
        spark_session=spark,
        read_sources_func=read_sources,
        build_dims_func=build_dims,
        build_facts_func=build_facts,
        write_tables_func=write_tables,
    )


if __name__ == "__main__":
    spark = create_spark_session(app_name="silver_fact_climate_annual")
    run(spark)
    spark.stop()
