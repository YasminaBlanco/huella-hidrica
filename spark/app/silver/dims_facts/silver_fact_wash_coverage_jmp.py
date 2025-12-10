"""
silver_fact_wash_coverage_jmp.py

Job de modelo Silver para construir la tabla de hechos WashCoverage
a partir de JMP limpio + dimensiones del modelo Silver.

Tabla generada: wash_coverage

Columnas:
  - wash_coverage_id   PK surrogate (BIGINT)
  - country_key        FK INT
  - date_key           FK INT   (representa el año, usando 31/12)
  - residence_type_key FK INT
  - service_type_key   FK INT
  - service_level_key  FK INT
  - population_total   BIGINT
  - coverage_pct       DECIMAL(5,2)

Modos de ejecución:

- FULL:
    si PROCESS_YEAR no está definido, recalcula todo el histórico JMP.
- INCREMENTAL:
    si PROCESS_YEAR=YYYY, sólo procesa ese año y
    sólo sobrescribe ese año en la tabla de hechos (overwrite dinámico).
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

BASE_BUCKET = os.getenv("BASE_BUCKET", "henry-pf-g2-huella-hidrica")
S3_BASE = f"s3a://{BASE_BUCKET}"

# Dataset limpio de JMP (Silver clean)
SILVER_JMP_CLEAN_PATH = f"{S3_BASE}/silver/jmp/"

# Ruta base del modelo Silver (dims + facts)
SILVER_MODEL_BASE_PATH = f"{S3_BASE}/silver/model"

# Parámetro de rango (runs incrementales anuales)
PROCESS_YEAR_ENV = os.getenv("PROCESS_YEAR")  # ej. "2020"
process_year: Optional[int] = int(PROCESS_YEAR_ENV) if PROCESS_YEAR_ENV else None

print("============================================================")
print(f"[FACT WASH CONFIG] BASE_BUCKET  = {BASE_BUCKET}")
print(f"[FACT WASH CONFIG] S3_BASE      = {S3_BASE}")
print(f"[FACT WASH CONFIG] PROCESS_YEAR = {process_year}")
print("============================================================")


# =============================================================
# 1) READ_SOURCES lee jmp_clean + dimensiones
# =============================================================
def read_sources(spark: SparkSession) -> DFMap:
    """
    Lee:
      - jmp_clean: datos anuales de cobertura WASH
      - country, residence_type, service_type, service_level, date:
        dimensiones ya creadas por silver_dims_job.py

    Aplica filtro por año si process_year está configurado
    (modo incremental).
    """

    print(f"[READ] JMP clean desde: {SILVER_JMP_CLEAN_PATH}")
    jmp_clean = spark.read.parquet(SILVER_JMP_CLEAN_PATH)

    if process_year is not None:
        print(f"[READ] Filtrando JMP a year = {process_year}")
        jmp_clean = jmp_clean.filter(F.col("year") == process_year)

    # Dimensiones del modelo Silver
    country_dim = spark.read.parquet(f"{SILVER_MODEL_BASE_PATH}/country")
    residence_type_dim = spark.read.parquet(
        f"{SILVER_MODEL_BASE_PATH}/residence_type"
    )
    service_type_dim = spark.read.parquet(
        f"{SILVER_MODEL_BASE_PATH}/service_type"
    )
    service_level_dim = spark.read.parquet(
        f"{SILVER_MODEL_BASE_PATH}/service_level"
    )
    date_dim = spark.read.parquet(f"{SILVER_MODEL_BASE_PATH}/date")

    return {
        "jmp_clean": jmp_clean,
        "country_dim": country_dim,
        "residence_type_dim": residence_type_dim,
        "service_type_dim": service_type_dim,
        "service_level_dim": service_level_dim,
        "date_dim": date_dim,
    }


# =============================================================
# 2) FUNCIÓN AUXILIAR PARA CONSTRUIR LA FACT
# =============================================================
def build_wash_coverage_fact(
    df_jmp: DataFrame,
    df_country: DataFrame,
    df_residence_type: DataFrame,
    df_service_type: DataFrame,
    df_service_level: DataFrame,
    df_date: DataFrame,
) -> DataFrame:
    """
    Construye la tabla de hechos wash_coverage.

    Grain:
      - country_key
      - residence_type_key
      - service_type_key
      - service_level_key
      - date_key (un registro por año, usando 31/12 de cada año)
    """

    # 1) Columnas de negocio + métricas
    base = df_jmp.select(
        "country_iso3",
        "residence_type_code",
        "service_type_desc",
        "service_level_desc",
        "year",
        "population_total",
        "coverage_pct",
    )

    # 2) JOIN country_key (por country_iso3)
    base = (
        base.alias("j")
        .join(
            df_country.select("country_key", "country_iso3").alias("c"),
            on="country_iso3",
            how="left",
        )
    )

    # 3) JOIN residence_type_key (por residence_type_code)
    base = (
        base.alias("j")
        .join(
            df_residence_type.select(
                "residence_type_key",
                "residence_type_code",
            ).alias("r"),
            on="residence_type_code",
            how="left",
        )
    )

    # 4) JOIN service_type_key (por service_type_desc)
    base = (
        base.alias("j")
        .join(
            df_service_type.select(
                "service_type_key",
                "service_type_desc",
            ).alias("st"),
            on="service_type_desc",
            how="left",
        )
    )

    # 5) JOIN service_level_key (por service_level_desc)
    base = (
        base.alias("j")
        .join(
            df_service_level.select(
                "service_level_key",
                "service_level_desc",
            ).alias("sl"),
            on="service_level_desc",
            how="left",
        )
    )

    # 6) JOIN date_key (año - 31 de diciembre de ese año en dim_date)
    end_of_year_dates = (
        df_date
        .filter(F.date_format("date", "MM-dd") == "12-31")
        .select("year", "date_key")
        .distinct()
    )

    base = (
        base.alias("j")
        .join(
            end_of_year_dates.alias("d"),
            on="year",
            how="left",
        )
    )

    # 7) Surrogate key
    fact = base.withColumn(
        "wash_coverage_id",
        F.monotonically_increasing_id().cast("bigint"),
    )

    # 8) Tipos de datos de métricas
    fact = (
        fact
        .withColumn("population_total", F.col("population_total").cast("bigint"))
        .withColumn("coverage_pct", F.col("coverage_pct").cast("decimal(5, 2)"))
    )

    # 9) Selección final
    fact = fact.select(
        "wash_coverage_id",
        "country_key",
        "date_key",
        "residence_type_key",
        "service_type_key",
        "service_level_key",
        "population_total",
        "coverage_pct",
    )

    return fact


# =============================================================
# 3) BUILD_DIMS / BUILD_FACTS para plantilla
# =============================================================
def build_dims(sources: DFMap) -> DFMap:
    """
    Este job no construye nuevas dimensiones; sólo consume
    las dimensiones ya generadas por silver_dims_job.py.
    """
    return {}


def build_facts(sources: DFMap, dims: DFMap) -> DFMap:
    """
    Construye el diccionario de facts para el Template Method.
    """
    df_jmp = sources["jmp_clean"]
    df_country = sources["country_dim"]
    df_residence_type = sources["residence_type_dim"]
    df_service_type = sources["service_type_dim"]
    df_service_level = sources["service_level_dim"]
    df_date = sources["date_dim"]

    wash_coverage = build_wash_coverage_fact(
        df_jmp,
        df_country,
        df_residence_type,
        df_service_type,
        df_service_level,
        df_date,
    )

    return {
        "wash_coverage": wash_coverage
    }


# =============================================================
# 4) WRITE_TABLES (OVERWRITE DINÁMICO)
# =============================================================
def write_tables(dims: DFMap, facts: DFMap, spark: SparkSession) -> None:
    """
    Escribe la tabla wash_coverage en Silver/model.
    """

    fact = facts.get("wash_coverage")
    if fact is None or fact.rdd.isEmpty():
        print("No se construyó la tabla wash_coverage o no hay filas. Nada que escribir.")
        return

    output_path = f"{SILVER_MODEL_BASE_PATH}/wash_coverage"
    print(f"[WRITE] Guardando wash_coverage en {output_path} ...")

    (
        fact.write
        .mode("overwrite")                # overwrite dinámico por partición (Spark config)
        .partitionBy("country_key", "date_key")
        .format("parquet")
        .save(output_path)
    )

    print("[WRITE] wash_coverage escrito (overwrite dinámico por partición).")


# =============================================================
# 5) RUN DEL JOB (usado desde main_silver_model.py)
# =============================================================
def run(
    spark: SparkSession,
    jmp_clean_path: str = SILVER_JMP_CLEAN_PATH,
    silver_model_base_path: str = SILVER_MODEL_BASE_PATH,
    process_year_param: Optional[int] = None,
) -> None:
    """
    Ejecuta el flujo estándar del job de hechos WASH.

    Parámetros:
      - jmp_clean_path:
          ruta al parquet limpio de JMP (Silver).
      - silver_model_base_path:
          base donde viven las dimensiones y facts del modelo Silver.
      - process_year_param:
          año a reprocesar (modo incremental). Si es None, se usa
          el valor de la variable de entorno PROCESS_YEAR; si ambos
          son None, se asume FULL histórico.
    """
    global SILVER_JMP_CLEAN_PATH, SILVER_MODEL_BASE_PATH, process_year

    # Actualiza rutas globales 
    SILVER_JMP_CLEAN_PATH = jmp_clean_path
    SILVER_MODEL_BASE_PATH = silver_model_base_path

    # Si el main nos pasa un año explícito, lo usamos
    if process_year_param is not None:
        process_year = process_year_param

    print(f"[FACT WASH RUN] process_year (efectivo) = {process_year}")

    # Ejecutar el pipeline genérico del modelo Silver
    run_silver_model_job(
        spark_session=spark,
        read_sources_func=read_sources,
        build_dims_func=build_dims,
        build_facts_func=build_facts,
        write_tables_func=write_tables,
    )


if __name__ == "__main__":
    spark = create_spark_session(app_name="silver_fact_wash_coverage_jmp")
    run(spark)
    spark.stop()