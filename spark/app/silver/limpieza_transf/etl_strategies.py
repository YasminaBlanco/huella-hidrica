# etl_strategies.py
#
# Estrategias de ETL para:
#   - Open Meteo (clima diario -> mensual)
#   - World Bank (indicadores socioeconómicos)


from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, StringType,
                               StructField, StructType)

# ==============================================================================
# ESTRATEGIA: LECTURA UNIFICADA (Bronze -> Silver)
# ==============================================================================


def read_data(spark: SparkSession, source_type: str, base_path: str) -> DataFrame:
    """
    Lectura unificada para:
      - source_type == "open_meteo"
      - source_type == "world_bank"

    base_path se espera con esquema s3a://, por ejemplo:
      s3a://<bucket>/bronze/open_meteo/
      s3a://<bucket>/bronze/world_bank/

    """

    # Definición de valor literal 0.0 como DOUBLE
    DOUBLE_ZERO = F.lit(0.0).cast(DoubleType())

    # Columnas finales comunes para la salida Silver de clima
    common_cols = [
        "country_iso3",
        "province",
        "temperature_2m_max",
        "temperature_2m_min",
        "precipitation_sum",
        "et0_fao_evapotranspiration",
        "year",
        "month",
    ]

    # Esquema objetivo para Open Meteo (para manejar DF vacío)
    target_schema_open_meteo = StructType(
        [
            StructField("country_iso3", StringType(), True),
            StructField("province", StringType(), True),
            StructField("temperature_2m_max", DoubleType(), True),
            StructField("temperature_2m_min", DoubleType(), True),
            StructField("precipitation_sum", DoubleType(), True),
            StructField("et0_fao_evapotranspiration", DoubleType(), True),
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
        ]
    )

    if source_type == "open_meteo":
        # FIX TEMPORAL PARA TIMESTAMP(NANOS), útil para ciertos parquet
        spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")

        print(f"[INFO] Open Meteo: base_path = {base_path}")

        # Actualmente solo usamos ARG/MEX, pero el código está listo para extender
        sources_config = [
            {
                # 1. ARG/MEX: Leen 'year' y 'month' directamente del archivo
                "paths": [f"{base_path}ARG/", f"{base_path}MEX/"],
                "has_province_col": True,
                "read_cols": [
                    "province",
                    "temperature_2m_max",
                    "temperature_2m_min",
                    "precipitation_sum",
                    "et0_fao_evapotranspiration",
                    "year",
                    "month",
                ],
            },
        ]

        dfs = []

        for config in sources_config:
            paths_to_read = config["paths"]
            cols_to_read = config["read_cols"]

            try:
                print(
                    f"[INFO] Open Meteo: Lectura selectiva para paths: {paths_to_read}. "
                    f"Columnas: {cols_to_read}"
                )

                # columnas relevantes del parquet
                df = (
                    spark.read.format("parquet")
                    .option("recursiveFileLookup", "true")
                    .load(paths_to_read, columns=cols_to_read)
                )

                total_rows = df.count()
                print(
                    f"[INFO] Open Meteo: Lectura exitosa para {paths_to_read}. "
                    f"Total filas: {total_rows}"
                )

            except Exception as e:
                print(
                    f"[ERROR] Falló la lectura recursiva y selectiva para {paths_to_read}. "
                    f"Ignorando este grupo."
                )
                print(f"  Motivo del fallo: {str(e)[:200]}...")
                continue

            if df is None or df.isEmpty():
                print(
                    f"[INFO] No se encontró data válida en los paths: {paths_to_read}. "
                    f"Continuando."
                )
                continue

            # 1. ESTANDARIZAR country_iso3 desde el path
            #    Ejemplo de filename: .../open_meteo/ARG/...
            df = df.withColumn(
                "country_iso3",
                F.regexp_extract(F.input_file_name(), r"open_meteo/([A-Z]{3})/", 1),
            )

            # 2. FILTRADO CRÍTICO: Limpieza final de NULLs en las columnas de partición
            initial_count = df.count()
            df = df.filter(F.col("year").isNotNull() & F.col("month").isNotNull())

            filtered_count = df.count()
            if filtered_count < initial_count:
                print(
                    f"[WARNING] Se filtraron {initial_count - filtered_count} filas de "
                    f"data de {paths_to_read} porque 'year' o 'month' eran NULL."
                )

            if df.isEmpty():
                print(
                    f"[WARNING] Después de filtrar year/month NULL, el conjunto "
                    f"{paths_to_read} quedó vacío. Continuando."
                )
                continue

            # 3. columna 'province'
            if not config["has_province_col"]:
                df = df.withColumn("province", F.lit("Unknown").cast(StringType()))

            # 4. Unificación de columnas métricas: si faltan, se agregan como 0.0
            for col_name in ["temperature_2m_max", "temperature_2m_min"]:
                if col_name not in df.columns:
                    df = df.withColumn(col_name, DOUBLE_ZERO)

            dfs.append(df)

        if not dfs:
            # Devuelve DF vacío con el esquema target explícito
            print(
                "[INFO] La lectura de Open Meteo no produjo resultados válidos. "
                "Creando DataFrame vacío con esquema Silver."
            )
            return spark.createDataFrame([], schema=target_schema_open_meteo)

        # Selección final para forzar orden y unificación
        dfs_selected = [df.select(common_cols) for df in dfs]

        # Unificamos los DF con unionByName
        return reduce(
            lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs_selected
        )

    elif source_type == "world_bank":
        # Lectura simple para World Bank
        # base_path ya viene con s3a:// desde main_silver
        wb_cols_to_read = [
            "country_code",
            "date",
            "indicator_code",
            "indicator_name",
            "value",
        ]

        print(
            f"[INFO] World Bank: Leyendo desde {base_path} "
            f"solo columnas esenciales: {wb_cols_to_read}"
        )

        return spark.read.option("recursiveFileLookup", "true").load(
            base_path, columns=wb_cols_to_read
        )

    else:
        raise ValueError(f"Fuente desconocida en read_data: {source_type}")


# ==============================================================================
# ESTRATEGIA: ESTANDARIZACIÓN
# ==============================================================================


def standardize_data(df: DataFrame) -> DataFrame:
    """
    Aplica estandarización de columnas/tipos según la estructura del DF.
    """

    cols = df.columns

    # Clima (Open Meteo)
    if "temperature_2m_max" in cols and "precipitation_sum" in cols:
        # Solo renombramos 'province' a 'province_name' para Silver
        return df.withColumnRenamed("province", "province_name")

    # Socioeconómico (World Bank)
    elif "indicator_code" in cols and "country_code" in cols:
        # Extrae 'year' del campo 'date' y castea valor a Double
        return df.select(
            F.col("country_code").alias("country_iso3"),
            F.col("date").cast(IntegerType()).alias("year"),
            F.col("indicator_code"),
            F.col("indicator_name"),
            F.col("value").cast(DoubleType()).alias("indicator_value"),
        )

    print("WARNING: Dataset no reconocido en standardize_data. Se retorna sin cambios.")
    return df


# ==============================================================================
# ESTRATEGIA: DATA QUALITY
# ==============================================================================


def dq_check(df: DataFrame):
    cols = df.columns
    total = df.count()
    print(f"--- DQ REPORT (Total Rows: {total}) ---")

    if "temperature_2m_max" in cols:
        nulls = df.filter(F.col("temperature_2m_max").isNull()).count()
        print(f"[Clima] Nulos en temperature_2m_max: {nulls}")

    if "indicator_value" in cols:
        negatives = df.filter(F.col("indicator_value") < 0).count()
        print(f"[Socio] Valores negativos en indicator_value: {negatives}")


# ==============================================================================
# ESTRATEGIA: TRANSFORMACIÓN
# ==============================================================================


def transform_data(df: DataFrame) -> DataFrame:
    cols = df.columns

    # Clima
    if "precipitation_sum" in cols and "et0_fao_evapotranspiration" in cols:
        # Aseguramos no nulos en las métricas de acumulado
        df = df.na.fill(0.0, subset=["precipitation_sum", "et0_fao_evapotranspiration"])

        # Agregación mensual: nivel (country, province, year, month)
        df_monthly = df.groupBy("country_iso3", "province_name", "year", "month").agg(
            F.round(F.sum("precipitation_sum"), 2).alias("precip_total_mm"),
            F.round(F.avg("temperature_2m_max"), 2).alias("temp_max_avg_c"),
            F.round(F.avg("temperature_2m_min"), 2).alias("temp_min_avg_c"),
            F.round(F.sum("et0_fao_evapotranspiration"), 2).alias("et0_total_mm"),
            F.round(F.avg("et0_fao_evapotranspiration"), 2).alias("et0_avg_mm"),
        )

        # Flags climáticos por dia
        return df_monthly.withColumn(
            "dry_day_flag", F.col("precip_total_mm") < 10
        ).withColumn("heavy_rain_day_flag", F.col("precip_total_mm") > 150)

    # Socioeconómico
    elif "indicator_code" in cols and "indicator_value" in cols:
        # Filtra registros sin valor de indicador
        return df.filter(F.col("indicator_value").isNotNull())

    # Fallback
    return df


# ==============================================================================
# ESTRATEGIA: ESCRITURA (Silver)
# ==============================================================================


def write_data(df: DataFrame, output_path: str):
    """
    Guarda el DataFrame aplicando la estrategia de partición:
      - Open Meteo: partición por country_iso3, year, month
      - World Bank: partición por country_iso3, year

    IMPORTANTE:
      - Con spark.sql.sources.partitionOverwriteMode=dynamic,
        usar .mode("overwrite").partitionBy(...) hace overwrite
        solo de las particiones afectadas, no de toda la tabla.
    """
    cols = df.columns

    # 1. Open Meteo (Clima) - partición por country_iso3, year, month
    if (
        "precip_total_mm" in cols
        and "country_iso3" in cols
        and "year" in cols
        and "month" in cols
    ):
        group1_countries = ["ARG", "MEX"]
        df_group1 = df.filter(F.col("country_iso3").isin(group1_countries))

        print(
            "[INFO] Open Meteo: Escritura particionada por country_iso3, year, month."
        )

        if not df_group1.isEmpty():
            print(
                "[INFO] GRP 1 (ARG/MEX): overwrite dinámico por "
                "country_iso3, year, month en path:",
                output_path,
            )
            (
                df_group1.write.mode("overwrite")
                .partitionBy("country_iso3", "year", "month")
                .parquet(output_path)
            )
        else:
            print("[WARNING] GRP 1 (ARG/MEX) vacío. No se escribe nada para clima.")

    # 2. World Bank (Socioeconómico) - partición por country_iso3, year
    elif "indicator_code" in cols and "country_iso3" in cols and "year" in cols:
        print(
            "[INFO] World Bank: overwrite dinámico por country_iso3, year en path:",
            output_path,
        )
        (
            df.write.mode("overwrite")
            .partitionBy("country_iso3", "year")
            .parquet(output_path)
        )

    # 3. Fallback para estructuras inesperadas
    else:
        print(
            "[WARNING] write_data: estructura de DF desconocida, "
            "guardando sin partición en modo overwrite. Path:",
            output_path,
        )
        df.write.mode("overwrite").parquet(output_path)
