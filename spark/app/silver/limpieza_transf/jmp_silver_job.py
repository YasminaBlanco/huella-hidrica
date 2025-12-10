# jmp_silver_job.py
#
# Job de limpieza para la fuente JMP (Wash Coverage)

import os
from pyspark.sql import functions as F, types as T
from pyspark.sql import DataFrame


# =====================================================
# CONFIGURACIÓN: bucket base y rutas en S3 para JMP
# =====================================================

BASE_BUCKET = os.getenv("BASE_BUCKET", "henry-pf-g2-huella-hidrica")

BRONZE_JMP_PATH = f"s3a://{BASE_BUCKET}/bronze/jmp/country=*/year=*"
SILVER_JMP_CLEAN_PATH = f"s3a://{BASE_BUCKET}/silver/jmp/"

# Opcional para procesar solo un año concreto 
PROCESS_YEAR_ENV = os.getenv("PROCESS_YEAR")
PROCESS_YEAR = int(PROCESS_YEAR_ENV) if PROCESS_YEAR_ENV else None


# ==========================
# 1) LECTURA DESDE BRONZE
# ==========================

def read_jmp_func(spark_session) -> DataFrame:
    """
    Lee el bronze de JMP desde S3.
    Si se define PROCESS_YEAR, filtra por ese año en la columna 'Year'.
    """
    print(f"[JMP] Leyendo desde: {BRONZE_JMP_PATH}")
    df = (
        spark_session.read
        .option("mergeSchema", "true")
        .parquet(BRONZE_JMP_PATH)
    )

    if PROCESS_YEAR is not None:
        df = df.filter(F.col("Year") == PROCESS_YEAR)

    return df


# ==========================
# 2) ESTANDARIZAR COLUMNAS Y TIPOS
# ==========================

def standardize_jmp_func(df: DataFrame) -> DataFrame:
    """
    Estandariza columnas:
      - Selecciona solo las columnas necesarias
      - Normaliza nombres (alias)
      - Castea tipos según el modelo lógico
    """
    # --- Residence Type: desc (minúsculas) ---
    res_desc = F.lower(F.col("Residence Type"))

    # --- Residence Type: code ---
    res_code = (
        F.when(res_desc == "total", "TOT")
         .when(res_desc == "urban", "URB")
         .when(res_desc == "rural", "RUR")
         .otherwise("UNK")
    )

    return df.select(
        # Dimensión país
        F.col("ISO3").alias("country_iso3"),
        F.col("Country").alias("country_name"),

        # Año (nivel anual para JMP)
        F.col("Year").cast(T.IntegerType()).alias("year"),

        # Residence Type (code + desc)
        res_code.alias("residence_type_code"),

        # Dimensiones de residencia / servicio
        res_desc.alias("residence_type_desc"),
        F.lower(F.col("Service Type")).alias("service_type_desc"),
        F.lower(F.col("Service Level")).alias("service_level_desc"),

        # Medidas (indicadores)
        F.col("Coverage").cast(T.DecimalType(5, 2)).alias("coverage_pct"),
        F.col("Population").cast(T.LongType()).alias("population_total"),
    )


# ==========================
# 3) VALIDACIONES DE CALIDAD
# ==========================

def dq_jmp_func(df: DataFrame):
    """
    Validaciones básicas en memoria:
      - Nulls por columna
      - Negativos en indicadores
      - Filas duplicadas
    """
    print("=== Data Quality Checks JMP ===")

    # Nulls por columna
    nulls = {c: df.filter(F.col(c).isNull()).count() for c in df.columns}
    print("Null counts:", nulls)

    # Revisar negativos SOLO en indicadores
    indicator_cols = ["coverage_pct", "population_total"]
    for c in indicator_cols:
        if c in df.columns:
            neg = df.filter(F.col(c) < 0).count()
            if neg > 0:
                print(f"[WARN] {c} tiene {neg} valores negativos.")

    # Duplicados
    total = df.count()
    uniques = df.dropDuplicates().count()
    dups = total - uniques
    if dups > 0:
        print(f"[WARN] Hay {dups} filas duplicadas.")

    print("=== End DQ JMP ===")


# ==========================
# 4) LIMPIEZA
# ==========================

def clean_jmp_func(df: DataFrame) -> DataFrame:
    """
    Reglas de limpieza específicas para JMP:
      - Eliminar negativos en indicadores (coverage_pct, population_total)
      - Validar dominios de residence_type_desc (total/urban/rural)
      - Rellenar nulos (numéricos -> 0, string -> 'UNKNOWN')
      - Eliminar duplicados
    """

    # 1) Quitar negativos SOLO en indicadores de JMP
    indicator_cols = ["coverage_pct", "population_total"]
    for c in indicator_cols:
        if c in df.columns:
            df = df.filter((F.col(c) >= 0) | F.col(c).isNull())

    # 2) Validar dominios de residence_type_desc
    valid_residence = ["total", "urban", "rural"]
    if "residence_type_desc" in df.columns:
        df = df.filter(F.col("residence_type_desc").isin(valid_residence))

    # 3) Rellenar nulos
    numeric_types = ("int", "bigint", "double", "float", "decimal")
    numeric_cols = [c for c, t in df.dtypes if any(nt in t for nt in numeric_types)]
    string_cols = [c for c, t in df.dtypes if t == "string"]

    if numeric_cols:
        df = df.fillna(0, subset=numeric_cols)
    if string_cols:
        df = df.fillna("UNKNOWN", subset=string_cols)

    # 4) Eliminar duplicados
    df = df.dropDuplicates()

    return df


# ==========================
# 5) ESCRITURA A SILVER
# ==========================

def write_clean_jmp_func(df_clean: DataFrame):
    """
    Escribe el dataset limpio en Silver:
      - Formato Parquet
      - Compresión Snappy
      - Particionado por country_iso3 y year
    """
    (
        df_clean.write
        .mode("overwrite")
        .option("compression", "snappy")
        .partitionBy("country_iso3", "year")
        .parquet(SILVER_JMP_CLEAN_PATH)
    )
    print("Dataset limpio escrito en:", SILVER_JMP_CLEAN_PATH)
