"""
Plantilla base para jobs de modelado Silver (Dimensiones y Hechos).

Patrones de diseño aplicados:
- Template Method:
    run_silver_model_job define el flujo general del job de modelo.
- Strategy:
    cada modelo implementa sus propias funciones:
      * read_sources_func
      * build_dims_func
      * build_facts_func
      * write_tables_func

Soporta dos modos de ejecución según el contexto:

  - Full histórico

  - Incremental
"""

from typing import Any, Callable, Dict, Optional

from pyspark.sql import DataFrame, SparkSession

# Alias para mapear nombres de tablas
DFMap = Dict[str, DataFrame]


# ============================================================
# Creación de SparkSession para el modelo Silver
# ============================================================
def create_spark_session(app_name: str = "Huella_Hidrica_Silver_Model") -> SparkSession:
    """
    Crea una SparkSession con configuraciones pensadas para:
      - Escritura en Parquet con compresión snappy.
      - Overwrite dinámico por partición.
    """
    spark = (
        SparkSession.builder.appName(app_name)
        # Overwrite dinámico de particiones
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # Compresión por defecto para Parquet
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Zona horaria consistente
        .config("spark.sql.session.timeZone", "UTC").getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# Template Method para jobs de modelo Silver
# ============================================================
def run_silver_model_job(
    spark_session: SparkSession,
    read_sources_func: Callable[..., DFMap],
    build_dims_func: Callable[[DFMap], DFMap],
    build_facts_func: Callable[[DFMap, DFMap], DFMap],
    write_tables_func: Callable[[DFMap, DFMap, SparkSession], None],
    context: Optional[Dict[str, Any]] = None,
) -> None:
    """
    Ejecuta el flujo estándar de un job de modelo Silver:

      1) read_sources_func

      2) build_dims_func

      3) build_facts_func

      4) write_tables_func

    Parámetro context:
      - Diccionario con parámetros de ejecución, por ejemplo:
            {
                "process_year": 2025,
                "process_month": 11
            }
      - Si no se pasa o es None, se asume full histórico.
    """
    if context is None:
        context = {}

    print("[STEP 1/4] Leyendo datasets de entrada (limpios / dims previas)...")

    # --------------------------------------------
    try:
        source_dfs: DFMap = read_sources_func(spark_session, context)
    except TypeError:
        source_dfs = read_sources_func(spark_session)

    if not source_dfs:
        print("ADVERTENCIA: No se leyeron fuentes para este job. Terminando.")
        return

    print("[STEP 2/4] Construyendo dimensiones...")
    dims: DFMap = build_dims_func(source_dfs)

    print("[STEP 3/4] Construyendo tablas de hechos...")
    facts: DFMap = build_facts_func(source_dfs, dims)

    print("[STEP 4/4] Escribiendo dimensiones y hechos en Silver...")
    write_tables_func(dims, facts, spark_session)

    print("\nSilver model job completado con éxito.\n")
