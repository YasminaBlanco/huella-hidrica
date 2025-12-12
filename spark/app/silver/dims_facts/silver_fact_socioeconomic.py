"""
silver_fact_socioeconomic.py

Job de modelo Silver para construir la tabla de hechos Socioeconomic
a partir de datos limpios de Indicadores Socioeconómicos + dimensiones
del modelo Silver.

Tabla generada: socioeconomic

Columnas:
  - socioeconomic_id   PK surrogate (BIGINT)
  - country_key        FK INT
  - date_key           FK INT  (representa el año, usando 31/12)
  - indicator_key      FK INT
  - indicator_value    DECIMAL(18,4)

Soporta runs:
- FULL: si PROCESS_YEAR no está definido recalcula todo el histórico.
- INCREMENTAL: si PROCESS_YEAR=YYYY sólo recalcula ese año
  y sobrescribe sólo esas particiones (overwrite dinámico).
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
# CONFIG: BUCKET Y RANGOS
# ==========================

# Bucket base del proyecto
BASE_BUCKET = os.getenv("BASE_BUCKET", "henry-pf-g2-huella-hidrica")
S3_BASE = f"s3a://{BASE_BUCKET}"

# Dataset limpio de Indicadores Socioeconómicos (Silver clean)
SILVER_SOCIOECONOMIC_CLEAN_PATH = f"{S3_BASE}/silver/socioeconomic/"

# Ruta base del modelo Silver (dims + facts)
SILVER_MODEL_BASE_PATH = f"{S3_BASE}/silver/model"

# Parámetro de rango (runs incrementales por año)
PROCESS_YEAR_ENV = os.getenv("PROCESS_YEAR")
process_year: Optional[int] = int(PROCESS_YEAR_ENV) if PROCESS_YEAR_ENV else None

print("============================================================")
print(f"[FACT SOCIO CONFIG] BASE_BUCKET  = {BASE_BUCKET}")
print(f"[FACT SOCIO CONFIG] S3_BASE      = {S3_BASE}")
print(f"[FACT SOCIO CONFIG] PROCESS_YEAR = {process_year}")
print("============================================================")


# =============================================================
# 1) READ_SOURCES
# =============================================================
def read_sources(spark: SparkSession) -> DFMap:
    """
    Lee:
      - socioeconomic_clean: datos anuales de indicadores socioeconómicos.
      - country, indicator, date: dimensiones ya creadas por silver_dims_job.py.

    Si process_year está definido, filtra socioeconomic_clean a ese año
    para soportar runs incrementales.
    """

    print(f"[READ] Socioeconomic clean desde: {SILVER_SOCIOECONOMIC_CLEAN_PATH}")
    socioeconomic_clean = spark.read.parquet(SILVER_SOCIOECONOMIC_CLEAN_PATH)

    if process_year is not None:
        print(f"[READ] Filtrando socioeconomic_clean a year = {process_year}")
        socioeconomic_clean = socioeconomic_clean.filter(F.col("year") == process_year)

    country_dim = spark.read.parquet(f"{SILVER_MODEL_BASE_PATH}/country")
    indicator_dim = spark.read.parquet(f"{SILVER_MODEL_BASE_PATH}/indicator")
    date_dim = spark.read.parquet(f"{SILVER_MODEL_BASE_PATH}/date")

    return {
        "socioeconomic_clean": socioeconomic_clean,
        "country_dim": country_dim,
        "indicator_dim": indicator_dim,
        "date_dim": date_dim,
    }


# =============================================================
# 2) FUNCIÓN AUXILIAR PARA CONSTRUIR LA FACT
# =============================================================
def build_socioeconomic_fact(
    df_socioeconomic: DataFrame,
    df_country: DataFrame,
    df_indicator: DataFrame,
    df_date: DataFrame,
) -> DataFrame:
    """
    Construye la tabla de hechos socioeconomic.

    Grain:
      - country_key
      - indicator_key
      - date_key (un registro por año, usando 31/12)
    """

    # 1) Selección de columnas de negocio
    base = df_socioeconomic.select(
        "country_iso3",
        "indicator_code",
        "year",
        "indicator_value",
    )

    # 2) JOIN country_key
    base = base.alias("s").join(
        df_country.select("country_key", "country_iso3").alias("c"),
        on="country_iso3",
        how="left",
    )

    # 3) JOIN indicator_key
    base = base.alias("s").join(
        df_indicator.select(
            "indicator_key",
            "indicator_code",
        ).alias("i"),
        on="indicator_code",
        how="left",
    )

    # 4) JOIN date_key (31 de diciembre de cada año)
    end_of_year_dates = (
        df_date.filter(F.date_format("date", "MM-dd") == "12-31")
        .select("year", "date_key")
        .distinct()
    )

    base = base.alias("s").join(
        end_of_year_dates.alias("d"),
        on="year",
        how="left",
    )

    # 5) Surrogate key
    fact = base.withColumn(
        "socioeconomic_id",
        F.monotonically_increasing_id().cast("bigint"),
    )

    # 6) Tipos de datos
    fact = fact.withColumn(
        "indicator_value",
        F.col("indicator_value").cast("decimal(18, 4)"),
    )

    # 7) Selección final
    fact = fact.select(
        "socioeconomic_id",
        "country_key",
        "date_key",
        "indicator_key",
        "indicator_value",
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
    Construye la fact socioeconomic a partir de datos limpios + dimensiones.
    """
    df_socioeconomic = sources["socioeconomic_clean"]
    df_country = sources["country_dim"]
    df_indicator = sources["indicator_dim"]
    df_date = sources["date_dim"]

    socioeconomic_fact = build_socioeconomic_fact(
        df_socioeconomic,
        df_country,
        df_indicator,
        df_date,
    )

    return {"socioeconomic": socioeconomic_fact}


# =============================================================
# 4) WRITE_TABLES (OVERWRITE DINÁMICO POR PARTICIÓN)
# =============================================================
def write_tables(dims: DFMap, facts: DFMap, spark: SparkSession) -> None:
    """
    Escribe la tabla socioeconomic en Silver/model.

    - mode("overwrite") + partitionBy("country_key", "date_key")
      Cuando process_year está definido, sólo se sobrescriben
      las particiones de ese año.
    """

    fact = facts.get("socioeconomic")
    if fact is None or fact.rdd.isEmpty():
        print(
            "No se construyó la tabla socioeconomic o no hay filas. Nada que escribir."
        )
        return

    output_path = f"{SILVER_MODEL_BASE_PATH}/socioeconomic"
    print(f"[WRITE] Guardando socioeconomic en {output_path} ...")

    (
        fact.write.mode("overwrite")  # overwrite dinámico por partición
        .partitionBy("country_key", "date_key")
        .format("parquet")
        .save(output_path)
    )

    print("[WRITE] socioeconomic escrito (overwrite dinámico por partición).")


# =============================================================
# 5) RUN
# =============================================================
def run(
    spark: SparkSession,
    socioeconomic_clean_path: str = SILVER_SOCIOECONOMIC_CLEAN_PATH,
    silver_model_base_path: str = SILVER_MODEL_BASE_PATH,
    process_year_param: Optional[int] = None,
) -> None:
    """
    Ejecuta el flujo estándar del job de hechos socioeconomic.

    Parámetros:
      - socioeconomic_clean_path: ruta al parquet limpio de indicadores.
      - silver_model_base_path  : base de dims/facts Silver.
      - process_year_param      : año a reprocesar. Si es None, se usa
        el valor leído de PROCESS_YEAR; si ambos son None, es FULL histórico.
    """
    global SILVER_SOCIOECONOMIC_CLEAN_PATH, SILVER_MODEL_BASE_PATH, process_year

    SILVER_SOCIOECONOMIC_CLEAN_PATH = socioeconomic_clean_path
    SILVER_MODEL_BASE_PATH = silver_model_base_path

    if process_year_param is not None:
        process_year = process_year_param

    print(f"[FACT SOCIO RUN] process_year (efectivo) = {process_year}")

    run_silver_model_job(
        spark_session=spark,
        read_sources_func=read_sources,
        build_dims_func=build_dims,
        build_facts_func=build_facts,
        write_tables_func=write_tables,
    )


if __name__ == "__main__":
    spark = create_spark_session(app_name="silver_fact_socioeconomic")
    run(spark)
    spark.stop()
