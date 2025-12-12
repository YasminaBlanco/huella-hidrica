# main_gold_model.py
"""
Orquestador principal del MODELO GOLD (KPIs) - Huella Hídrica.

Responsabilidades:
- Definir el bucket/base S3 .
- Construir rutas de entrada (silver/model) y salida (gold/model).
- Validar que las tablas Silver necesarias existan y tengan datos.
- Ejecutar en orden los jobs de KPIs.
"""

import os
from typing import List

from pyspark.sql import SparkSession

from base_gold_model_job import create_spark_session
from gold import (
    gold_kpi01_climate_water,
    gold_kpi02_water_mobility,
    gold_kpi03_critical_zones,
    gold_kpi04_health_risk_population,
    gold_kpi05_urban_rural_gap_water,
    gold_kpi06_water_gdp_corr,
    gold_kpi07_water_sanitation_gap,
)


# ==========================
# 0. CONFIG RUTAS S3
# ==========================

# variable BASE_BUCKET 
BASE_BUCKET = os.getenv("BASE_BUCKET", "henry-pf-g2-huella-hidrica")
S3_BASE = f"s3a://{BASE_BUCKET}"

# Silver (modelo ya construido)
SILVER_MODEL_BASE_PATH = f"{S3_BASE}/silver/model"

# Salida para el modelo GOLD (KPIs)
GOLD_MODEL_BASE_PATH = f"{S3_BASE}/gold/model"


# ============================================================
# 1. Helpers de validación sobre Silver
# ============================================================

def validate_silver_tables(
    spark: SparkSession,
    silver_base: str,
    required_tables: List[str],
) -> None:
    """
    Verifica que las tablas necesarias en silver/model:
      - Se puedan leer correctamente.
      - No estén vacías.

    Lanza RuntimeError si alguna tabla no existe o está vacía.
    """
    print("============================================================")
    print("[GOLD MAIN] Validando tablas Silver requeridas...")
    for table in required_tables:
        path = f"{silver_base.rstrip('/')}/{table}"
        print(f"[GOLD MAIN] Checando tabla Silver '{table}' en {path}")
        try:
            df = spark.read.parquet(path)
        except Exception as e:
            raise RuntimeError(
                f"[GOLD MAIN] ERROR: No se pudo leer la tabla Silver '{table}' "
                f"desde {path}. Detalle: {e}"
            ) from e

        if df.rdd.isEmpty():
            raise RuntimeError(
                f"[GOLD MAIN] ERROR: La tabla Silver '{table}' está vacía en {path}."
            )

        count = df.count()
        print(f"[GOLD MAIN] OK: '{table}' tiene {count} filas.")

    print("[GOLD MAIN] Validación de tablas Silver completada.")
    print("============================================================")


# ============================================================
# 2. main()
# ============================================================

def main() -> None:
    print("============================================================")
    print("[GOLD MODEL CONFIG] BASE_BUCKET   =", BASE_BUCKET)
    print("[GOLD MODEL CONFIG] S3_BASE       =", S3_BASE)
    print("[GOLD MODEL CONFIG] SILVER_MODEL  =", SILVER_MODEL_BASE_PATH)
    print("[GOLD MODEL CONFIG] GOLD_MODEL    =", GOLD_MODEL_BASE_PATH)
    print("============================================================")

    spark = create_spark_session("Huella_Hidrica_Gold_Model")
    spark.sparkContext.setLogLevel("WARN")
    print(">>> SparkSession MODELO GOLD (KPIs) creada.")

    try:
        # ----------------- Validaciones globales -----------------
        required_tables = [
            # Facts principales
            "wash_coverage",
            "climate_annual",
            "climate_monthly",
            "socioeconomic",   

            # Dimensiones clave
            "country",
            "province",        
            "residence_type",
            "indicator",       
        ]
        validate_silver_tables(spark, SILVER_MODEL_BASE_PATH, required_tables)

        # ================== KPI 01 ==================
        print("\n" + "=" * 80)
        print(">>> CREANDO TABLA GOLD KPI 01 - CLIMA vs AGUA SEGURA <<<")
        print("=" * 80)
        print("Granularidad: country + residence_type + year")
        print(
            "Incluye: precip_total_mm_year, safe_water_pct (Drinking water + At least basic), "
            "deltas, correlación, nivel de riesgo y dirección del impacto.\n"
        )

        gold_kpi01_climate_water.run(
            spark,
            silver_model_base_path=SILVER_MODEL_BASE_PATH,
            gold_model_base_path=GOLD_MODEL_BASE_PATH,
        )
        print("KPI 01 creado correctamente en GOLD.\n")

        # ================== KPI 02 ==================
        print("\n" + "=" * 80)
        print(">>> CREANDO TABLA GOLD KPI 02 - MOVILIDAD POR AGUA (>30 MIN) <<<")
        print("=" * 80)
        print(
            "Granularidad: country + residence_type + year\n"
            "Incluye: pct_over_30min, delta vs año anterior, tendencia "
            "(improved/worsened/stable), años observados y semáforo.\n"
        )

        gold_kpi02_water_mobility.run(
            spark,
            silver_model_base_path=SILVER_MODEL_BASE_PATH,
            gold_model_base_path=GOLD_MODEL_BASE_PATH,
        )
        print("KPI 02 creado correctamente en GOLD.\n")

        # ================== KPI 03 ==================
        print("\n" + "=" * 80)
        print(">>> CREANDO TABLA GOLD KPI 03 - ZONAS CRÍTICAS CLIMA + SANEAMIENTO <<<")
        print("=" * 80)
        print(
            "Granularidad esperada: country + province + year\n"
            "Incluye: saneamiento básico %, tendencia climática de precipitación, "
            "bandera de zona crítica, conteos por país/año y semáforo.\n"
        )

        gold_kpi03_critical_zones.run(
            spark,
            silver_model_base_path=SILVER_MODEL_BASE_PATH,
            gold_model_base_path=GOLD_MODEL_BASE_PATH,
        )
        print("KPI 03 creado correctamente en GOLD.\n")

        # ================== KPI 04 ==================
        print("\n" + "=" * 80)
        print(">>> CREANDO TABLA GOLD KPI 04 - ÍNDICE PONDERADO DE RIESGO SANITARIO <<<")
        print("=" * 80)
        print(
            "Granularidad: country + year\n"
            "Incluye: riesgo por saneamiento, mortalidad infantil, pobreza, "
            "población total, índice compuesto 0-100 y semáforo.\n"
        )

        gold_kpi04_health_risk_population.run(
            spark,
            silver_model_base_path=SILVER_MODEL_BASE_PATH,
            gold_model_base_path=GOLD_MODEL_BASE_PATH,
        )
        print("KPI 04 creado correctamente en GOLD.\n")

        # ================== KPI 05 ==================
        print("\n" + "=" * 80)
        print(">>> CREANDO TABLA GOLD KPI 05 - BRECHA URBANO/RURAL EN AGUA SEGURA <<<")
        print("=" * 80)
        print(
            "Granularidad: country + year\n"
            "Incluye: agua segura urbana %, agua segura rural %, "
            "brecha en p.p. y semáforo.\n"
        )

        gold_kpi05_urban_rural_gap_water.run(
            spark,
            silver_model_base_path=SILVER_MODEL_BASE_PATH,
            gold_model_base_path=GOLD_MODEL_BASE_PATH,
        )
        print("KPI 05 creado correctamente en GOLD.\n")

        # ================== KPI 06 ==================
        print("\n" + "=" * 80)
        print(">>> CREANDO TABLA GOLD KPI 06 - CORR AGUA SEGURA vs PIB PER CÁPITA <<<")
        print("=" * 80)
        print(
            "Granularidad: region_name + year\n"
            "Incluye: número de países, correlación agua vs PIB, valor absoluto, "
            "promedios de agua segura y PIB y semáforo.\n"
        )

        gold_kpi06_water_gdp_corr.run(
            spark,
            silver_model_base_path=SILVER_MODEL_BASE_PATH,
            gold_model_base_path=GOLD_MODEL_BASE_PATH,
        )
        print("KPI 06 creado correctamente en GOLD.\n")

        # ================== KPI 07 ==================
        print("\n" + "=" * 80)
        print(">>> CREANDO TABLA GOLD KPI 07 - BRECHA AGUA vs SANEAMIENTO SEGURO <<<")
        print("=" * 80)
        print(
            "Granularidad: country + year\n"
            "Incluye: % agua segura total, % saneamiento seguro total, "
            "brecha en p.p. y semáforo.\n"
        )

        gold_kpi07_water_sanitation_gap.run(
            spark,
            silver_model_base_path=SILVER_MODEL_BASE_PATH,
            gold_model_base_path=GOLD_MODEL_BASE_PATH,
        )
        print("KPI 07 creado correctamente en GOLD.\n")

        print("\n>>> ORQUESTACIÓN MODELO GOLD FINALIZADA OK. <<<")

    finally:
        print("\nCerrando SparkSession GOLD...")
        spark.stop()
        print("SparkSession GOLD cerrada.")


if __name__ == "__main__":
    main()