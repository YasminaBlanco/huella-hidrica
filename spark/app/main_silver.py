"""
Orquestador principal del ETL de limpieza para ejecutar el flujo de limpieza para múltiples fuentes
de datos de manera unificada.
"""

import os
from pyspark.sql import functions as F

# Importamos la plantilla base con el flujo principal
from base_silver_job import create_spark_session, run_clean_etl

# Importamos todas las funciones de Estrategia (Lectura, Estandarización, Limpieza, etc.)
from silver.limpieza_transf import etl_strategies as strategies
from silver.limpieza_transf import jmp_silver_job as jmp


def main():
    """
    Función principal que orquesta la ejecución del ETL.
    Pensada para correr en la instancia EC2 con Spark en Docker.
    """

    # ======================================================
    # 1. CONFIGURACIÓN: bucket base y periodo a procesar
    # ======================================================
    BASE_BUCKET = os.getenv("BASE_BUCKET", "henry-pf-g2-huella-hidrica")

    bronze_root = f"s3a://{BASE_BUCKET}/bronze"
    silver_root = f"s3a://{BASE_BUCKET}/silver"

    # Rutas de Lectura (Bronze)
    BASE_BRONZE_METEO = f"{bronze_root}/open_meteo/"
    BASE_BRONZE_WB = f"{bronze_root}/world_bank/"
    BASE_BRONZE_JMP = f"{bronze_root}/jmp/country=*/year=*"

    # Rutas de Escritura (Silver)
    OUTPUT_SILVER_CLIMATE = f"{silver_root}/climate_monthly/"
    OUTPUT_SILVER_SOCIO = f"{silver_root}/socioeconomic/"
    OUTPUT_SILVER_JMP = f"{silver_root}/jmp/"

    # Periodo a procesar para runs incrementales
    process_year_env = os.getenv("PROCESS_YEAR")  # ej. "2024"
    process_month_env = os.getenv("PROCESS_MONTH")  # ej. "11"

    process_year = int(process_year_env) if process_year_env else None
    process_month = int(process_month_env) if process_month_env else None

    print("=" * 60)
    print(f"[CONFIG] BASE_BUCKET       = {BASE_BUCKET}")
    print(f"[CONFIG] PROCESS_YEAR      = {process_year}")
    print(f"[CONFIG] PROCESS_MONTH     = {process_month}")
    print("=" * 60)

    # ======================================================
    # 2. CREACIÓN DE LA SESIÓN DE SPARK
    # ======================================================

    spark = create_spark_session("Huella_Hidrica_Unified_ETL")
    spark.sparkContext.setLogLevel("WARN")
    print("SparkSession creada con éxito.")

    # ======================================================
    # 3. LECTURAS FILTRADAS (para usar PROCESS_YEAR/MONTH)
    # ======================================================

    # --- OPEN METEO (clima diario/mensual) ---
    def read_open_meteo_filtered(sp):
        df = strategies.read_data(sp, "open_meteo", BASE_BRONZE_METEO)
        # Filtrado incremental por año/mes si se especifica
        if process_year is not None:
            df = df.filter(F.col("year") == process_year)
        if process_month is not None:
            df = df.filter(F.col("month") == process_month)
        return df

    # --- WORLD BANK (socioeconómico anual) ---
    def read_world_bank_filtered(sp):
        df = strategies.read_data(sp, "world_bank", BASE_BRONZE_WB)
        # En bronze la columna 'date' representa el año
        if process_year is not None:
            df = df.filter(F.col("date") == process_year)
        return df

    # =================================================================
    # 4. EJECUCIÓN 1: DATOS CLIMÁTICOS (OPEN METEO)
    # =================================================================
    print("\n" + "=" * 50)
    print(">>> INICIANDO ETL PARA OPEN METEO (CLIMA) <<<")
    print("=" * 50)

    run_clean_etl(
        spark_session=spark,
        # Lectura filtrada por año/mes si se definieron PROCESS_YEAR/MONTH
        read_func=read_open_meteo_filtered,
        standardize_func=strategies.standardize_data,
        dq_func=strategies.dq_check,
        clean_func=strategies.transform_data,
        # Escritura: write_data usa .mode('overwrite') + partitionBy(...)
        # y se apoya en spark.sql.sources.partitionOverwriteMode=dynamic
        write_clean_func=lambda df: strategies.write_data(df, OUTPUT_SILVER_CLIMATE),
    )

    # =================================================================
    # 5. EJECUCIÓN 2: DATOS SOCIOECONÓMICOS (WORLD BANK)
    # =================================================================
    print("\n" + "=" * 50)
    print(">>> INICIANDO ETL PARA WORLD BANK (SOCIOECONÓMICO) <<<")
    print("=" * 50)

    run_clean_etl(
        spark_session=spark,
        read_func=read_world_bank_filtered,
        standardize_func=strategies.standardize_data,
        dq_func=strategies.dq_check,
        clean_func=strategies.transform_data,
        write_clean_func=lambda df: strategies.write_data(df, OUTPUT_SILVER_SOCIO),
    )

    # =================================================================
    # 6. EJECUCIÓN 3: DATOS JMP (WASH COVERAGE)
    # =================================================================
    print("\n" + "=" * 50)
    print(">>> INICIANDO ETL PARA JMP <<<")
    print("=" * 50)

    # Para JMP usamos las funciones específicas de jmp_silver_job.py
    run_clean_etl(
        spark_session=spark,
        read_func=jmp.read_jmp_func,
        standardize_func=jmp.standardize_jmp_func,
        dq_func=jmp.dq_jmp_func,
        clean_func=jmp.clean_jmp_func,
        write_clean_func=jmp.write_clean_jmp_func,
    )

    print("\n>>> PROCESO ETL UNIFICADO FINALIZADO CON ÉXITO. <<<")
    spark.stop()


if __name__ == "__main__":
    main()
