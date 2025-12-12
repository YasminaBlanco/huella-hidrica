"""
Plantilla base para jobs de limpieza (Bronze to Silver).

Patrones de diseño aplicados: Template Method y Strategy.
Define el flujo general (read -> standardize -> data quality -> clean -> write).
"""

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from typing import Callable, Any

# Alias para simplificar las firmas de función
DFProcessor = Callable[[DataFrame], DataFrame]
DFValidator = Callable[[DataFrame], Any]
DFWriter = Callable[[DataFrame], None]
DFReader = Callable[[SparkSession], DataFrame]


def create_spark_session(app_name: str = "clean_etl_job") -> SparkSession:
    """
    Crea y devuelve una SparkSession.

    En la instancia EC2 con Docker:
      - La configuración de acceso a S3 (S3A, timeouts, provider de credenciales, etc.)
        se toma desde spark-defaults.conf y del IAM Role adjunto a la instancia.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        # Overwrite dinámico por partición
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Claves correctas para Spark 4.x
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.enableNestedColumnProjections", "true")
        .getOrCreate()
    )

    return spark


def run_clean_etl(
    spark_session: SparkSession,
    read_func: DFReader,
    standardize_func: DFProcessor,
    dq_func: DFValidator,
    clean_func: DFProcessor,
    write_clean_func: DFWriter,
):
    """
    EJECUTA EL FLUJO ESTÁNDAR DE LIMPIEZA (Template Method).
    """
    print("\n[STEP 1/5] Iniciando lectura de Bronze...")
    df_raw = read_func(spark_session)

    if df_raw is None or df_raw.count() == 0:
        print(
            "ADVERTENCIA: El DataFrame leído está vacío o no se encontró data. Terminando ETL."
        )
        return

    print("[STEP 2/5] Estandarizando datos...")
    df_std = standardize_func(df_raw)

    print("[STEP 3/5] Ejecutando chequeos de Calidad de Datos (DQ)...")
    dq_func(df_std)

    print("[STEP 4/5] Aplicando reglas de limpieza y transformación...")
    df_clean = clean_func(df_std)

    print("[STEP 5/5] Guardando dataset limpio en Silver...")
    write_clean_func(df_clean)
    print("\n ETL completado con éxito para este origen.")
