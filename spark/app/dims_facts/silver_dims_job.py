"""
silver_dims_job.py

Job para construir las DIMENSIONES del modelo Silver a partir de las tablas
Silver limpias (JMP, World Bank / socioeconómico, clima mensual).

Dimensiones creadas físicamente (en Parquet):

- country
- residence_type
- service_type
- service_level
- date
- indicator
- province

Cada tabla tiene una clave surrogate *_key, alineada con el modelo dimensional.

Este job NO crea facts; solo dimensiones, y siempre hace FULL REFRESH.
"""

import os
from pyspark.sql import SparkSession, DataFrame, functions as F

from base_silver_model_job import (
    DFMap,
    run_silver_model_job,
    create_spark_session,
)

# ============================================================================
# 1. CONFIGURACIÓN DE RUTAS (CONTROLADAS POR BASE_BUCKET)
# ============================================================================

BASE_BUCKET = os.getenv("BASE_BUCKET", "henry-pf-g2-huella-hidrica")
S3_BASE = f"s3a://{BASE_BUCKET}"

# Entradas Silver limpias (ajustables vía la función run())
SILVER_JMP_CLEAN_PATH = f"{S3_BASE}/silver/jmp/"
DIM_INDICATOR_PATH    = f"{S3_BASE}/silver/socioeconomic/"
DIM_CLIMA_PATH        = f"{S3_BASE}/silver/climate_monthly/"

# Salida del modelo Silver (dims + facts)
SILVER_MODEL_BASE_PATH = f"{S3_BASE}/silver/model"


# ============================================================================
# 2. LECTURA DE FUENTES
# ============================================================================

def read_sources(spark: SparkSession) -> DFMap:
    """
    Lee las tablas Silver limpias necesarias para construir las dimensiones.

    Fuentes:
      - JMP limpio (cobertura WASH, etc.)
      - Socioeconómico (indicadores tipo World Bank)
      - Clima mensual (climate_monthly)

    Retorna:
      dict con:
        {
          "jmp_clean":  df_jmp,
          "world_bank": df_world,
          "climate":    df_climate,
        }
    """
    print(f"[READ] JMP clean      desde: {SILVER_JMP_CLEAN_PATH}")
    print(f"[READ] World Bank dim desde: {DIM_INDICATOR_PATH}")
    print(f"[READ] Climate        desde: {DIM_CLIMA_PATH}")

    df_jmp = spark.read.parquet(SILVER_JMP_CLEAN_PATH)
    df_world = spark.read.parquet(DIM_INDICATOR_PATH)
    df_climate = spark.read.parquet(DIM_CLIMA_PATH)

    return {
        "jmp_clean": df_jmp,
        "world_bank": df_world,
        "climate": df_climate,
    }


# ============================================================================
# 3. CONSTRUCTORES DE CADA DIMENSIÓN
# ============================================================================

# ------------------ COUNTRY ------------------

def build_country(df_jmp: DataFrame) -> DataFrame:
    """
    Construye la dimensión de país (country).

    Esquema:
      - country_key    (PK surrogate)
      - country_iso3
      - country_name
      - region_name    (por ahora fijo: 'Latin America and Caribbean')
    """
    dim = (
        df_jmp
        .select("country_iso3", "country_name")
        .dropna(subset=["country_name"])
        .distinct()
        .withColumn("country_key", F.monotonically_increasing_id())
        .withColumn("region_name", F.lit("Latin America and Caribbean"))
    )

    dim = dim.select(
        "country_key",
        "country_iso3",
        "country_name",
        "region_name",
    )
    return dim


# ------------------ RESIDENCE_TYPE ------------------

def build_residence_type(df_jmp: DataFrame) -> DataFrame:
    """
    Construye la dimensión residence_type (tipo de residencia).

    Esquema:
      - residence_type_key   (PK surrogate)
      - residence_type_code  (TOT / URB / RUR / UNK)
      - residence_type_desc  (total / urban / rural / unknown)
    """
    dim = (
        df_jmp
        .select("residence_type_code", "residence_type_desc")
        .dropna(subset=["residence_type_code"])
        .distinct()
        .withColumn("residence_type_key", F.monotonically_increasing_id())
    )

    dim = dim.select(
        "residence_type_key",
        "residence_type_code",
        "residence_type_desc",
    )
    return dim


# ------------------ SERVICE_TYPE ------------------

def build_service_type(df_jmp: DataFrame) -> DataFrame:
    """
    Construye la dimensión service_type (tipo de servicio).

    Ejemplos de service_type_desc según JMP:
      - 'water'
      - 'sanitation'
      - 'hygiene'

    Esquema:
      - service_type_key   (PK surrogate)
      - service_type_desc
    """
    dim = (
        df_jmp
        .select("service_type_desc")
        .dropna(subset=["service_type_desc"])
        .distinct()
        .withColumn("service_type_key", F.monotonically_increasing_id())
    )

    dim = dim.select(
        "service_type_key",
        "service_type_desc",
    )
    return dim


# ------------------ SERVICE_LEVEL ------------------

def build_service_level(df_jmp: DataFrame) -> DataFrame:
    """
    Construye la dimensión service_level (nivel de servicio).

    Esquema:
      - service_level_key   (PK surrogate)
      - service_level_desc
    """
    dim = (
        df_jmp
        .select("service_level_desc")
        .dropna(subset=["service_level_desc"])
        .distinct()
        .withColumn("service_level_key", F.monotonically_increasing_id())
    )

    dim = dim.select(
        "service_level_key",
        "service_level_desc",
    )
    return dim


# ------------------ DATE (dim_date) ------------------

def build_date_dim(df_jmp: DataFrame, df_climate: DataFrame) -> DataFrame:
    """
    Construye la dimensión de fecha (date) usando el rango de años combinado
    de JMP y CLIMA.

    Razón:
    - JMP aporta años históricos de encuestas.
    - CLIMA es la fuente que se seguirá actualizando (2025, 2026, ...).
    Tomamos:
      - min_year = mínimo entre (min year JMP, min year CLIMA)
      - max_year = máximo entre (max year JMP, max year CLIMA)

    Esquema:
      - date_key   (INT, formato YYYYMMDD)
      - date       (DATE)
      - year       (INT)
      - month      (INT)
      - month_name (STRING)
      - quarter    (INT: 1, 2, 3, 4)
    """
    spark = df_jmp.sql_ctx.sparkSession

    # Bounds JMP
    jmp_bounds = df_jmp.select(
        F.min("year").alias("min_year"),
        F.max("year").alias("max_year"),
    ).first()

    # Bounds CLIMA
    clima_bounds = df_climate.select(
        F.min("year").alias("min_year"),
        F.max("year").alias("max_year"),
    ).first()

    candidates_min = [
        jmp_bounds["min_year"],
        clima_bounds["min_year"],
    ]
    candidates_max = [
        jmp_bounds["max_year"],
        clima_bounds["max_year"],
    ]

    # Quitamos None en caso de que alguna fuente venga vacía
    candidates_min = [c for c in candidates_min if c is not None]
    candidates_max = [c for c in candidates_max if c is not None]

    if not candidates_min or not candidates_max:
        raise ValueError(
            "No se encontraron años válidos en JMP ni en CLIMA "
            "para construir dim_date."
        )

    min_year = min(candidates_min)
    max_year = max(candidates_max)

    start_date = f"{min_year}-01-01"
    end_date = f"{max_year}-12-31"

    # Generamos un rango diario entre start_date y end_date
    df_seq = spark.sql(
        f"""
        SELECT sequence(
            to_date('{start_date}'),
            to_date('{end_date}'),
            interval 1 day
        ) AS date_seq
        """
    )

    dim_date = df_seq.select(F.explode("date_seq").alias("date"))

    dim_date = (
        dim_date
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("month_name", F.date_format("date", "MMMM"))
        .withColumn("quarter", F.quarter("date"))
        .withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
    )

    dim_date = dim_date.select(
        "date_key",
        "date",
        "year",
        "month",
        "month_name",
        "quarter",
    )
    return dim_date


# ------------------ INDICATOR ------------------

def build_indicator(df_indicator: DataFrame) -> DataFrame:
    """
    Construye la dimensión de indicadores (indicator), derivada de datos
    tipo World Bank / socioeconómico.

    Esquema:
      - indicator_key   (PK surrogate)
      - indicator_code
      - indicator_name
    """
    dim_indicator = (
        df_indicator
        .select("indicator_code", "indicator_name")
        .dropna(subset=["indicator_code", "indicator_name"])
        .distinct()
        .withColumn("indicator_key", F.monotonically_increasing_id())
    )

    dim_indicator = dim_indicator.select(
        "indicator_key",
        "indicator_code",
        "indicator_name",
    )
    return dim_indicator


# ------------------ PROVINCE ------------------

def build_province(df_climate: DataFrame) -> DataFrame:
    """
    Construye la dimensión de provincias (province), usando datos de clima.

    Esquema:
      - province_key   (PK surrogate)
      - country_iso3
      - province_name
    """
    dim_province = (
        df_climate
        .select("country_iso3", "province_name")
        .dropna(subset=["country_iso3", "province_name"])
        .distinct()
        .withColumn("province_key", F.monotonically_increasing_id())
    )

    dim_province = dim_province.select(
        "province_key",
        "country_iso3",
        "province_name",
    )
    return dim_province


# ============================================================================
# 4. BUILD_DIMS / BUILD_FACTS PARA LA PLANTILLA
# ============================================================================

def build_dims(sources: DFMap) -> DFMap:
    """
    Construye todas las dimensiones del modelo Silver a partir de:

      - JMP limpio      -> country, residence_type, service_type, service_level
      - World Bank/Socio-> indicator
      - Clima mensual   -> province, date 
    """
    df_jmp = sources["jmp_clean"]
    df_world = sources["world_bank"]
    df_climate = sources["climate"]

    dims: DFMap = {
        "country":        build_country(df_jmp),
        "residence_type": build_residence_type(df_jmp),
        "service_type":   build_service_type(df_jmp),
        "service_level":  build_service_level(df_jmp),
        "date":           build_date_dim(df_jmp, df_climate),
        "indicator":      build_indicator(df_world),
        "province":       build_province(df_climate),
    }

    return dims


def build_facts(sources: DFMap, dims: DFMap) -> DFMap:
    """
    Este job se dedica exclusivamente a dimensiones.
    No construye tablas de hechos, por lo que retornamos un dict vacío.
    """
    return {}


# ============================================================================
# 5. ESCRITURA DE DIMENSIONES
# ============================================================================

def write_tables(dims: DFMap, facts: DFMap, spark: SparkSession) -> None:
    """
    Escribe cada dimensión en formato Parquet bajo SILVER_MODEL_BASE_PATH.

    Notas:
    - Usamos overwrite completo para dimensiones (full refresh).
    """
    for table_name, df in dims.items():
        output_path = f"{SILVER_MODEL_BASE_PATH}/{table_name}"
        print(f"[WRITE] Guardando dimensión {table_name} en {output_path} ...")

        (
            df.write
            .mode("overwrite")  # full refresh de la dimensión
            .format("parquet")
            .save(output_path)
        )

    # facts está vacío para este job; no se escribe nada más aquí.


# ============================================================================
# 6. FUNCIÓN RUN (USADA DESDE main_silver_model.py)
# ============================================================================

def run(
    spark: SparkSession,
    silver_jmp_clean_path: str = SILVER_JMP_CLEAN_PATH,
    dim_indicator_path: str = DIM_INDICATOR_PATH,
    dim_clima_path: str = DIM_CLIMA_PATH,
    silver_model_base_path: str = SILVER_MODEL_BASE_PATH,
) -> None:
    """
    Ejecuta el job de dimensiones permitiendo sobreescribir rutas desde fuera.

    Si no se pasan parámetros, se usan las rutas por defecto definidas arriba.
    """
    global SILVER_JMP_CLEAN_PATH, DIM_INDICATOR_PATH, DIM_CLIMA_PATH, SILVER_MODEL_BASE_PATH

    SILVER_JMP_CLEAN_PATH = silver_jmp_clean_path
    DIM_INDICATOR_PATH = dim_indicator_path
    DIM_CLIMA_PATH = dim_clima_path
    SILVER_MODEL_BASE_PATH = silver_model_base_path

    run_silver_model_job(
        spark_session=spark,
        read_sources_func=read_sources,
        build_dims_func=build_dims,
        build_facts_func=build_facts,
        write_tables_func=write_tables,
    )


# ============================================================================
# 7. ENTRYPOINT DIRECTO 
# ============================================================================

if __name__ == "__main__":
    spark = create_spark_session(app_name="silver_dimensions_job")
    run(spark)
    spark.stop()

