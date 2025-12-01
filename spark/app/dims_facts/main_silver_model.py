"""
main_silver_model.py

Orquestador principal del MODELO SILVER (Dimensiones + Hechos).

Lee configuración de variables de entorno:
- BASE_BUCKET  : bucket base en S3
- PROCESS_YEAR : año a reprocesar 
- PROCESS_MONTH: mes a reprocesar 

Comportamiento:
- Sin PROCESS_YEAR / PROCESS_MONTH  -> FULL (todo el histórico)
- Solo PROCESS_YEAR                 -> anual (solo ese año)
- PROCESS_YEAR + PROCESS_MONTH      -> clima mensual solo ese año-mes
"""

import os
from typing import Optional

from base_silver_model_job import create_spark_session

import silver_dims_job
import silver_fact_climate_monthly
import silver_fact_climate_annual
import silver_fact_wash_coverage_jmp
import silver_fact_socioeconomic

# ==========================
# CONFIG GLOBAL DESDE ENV
# ==========================

BASE_BUCKET = os.getenv("BASE_BUCKET", "henry-pf-g2-huella-hidrica")
S3_BASE = f"s3a://{BASE_BUCKET}"

PROCESS_YEAR_ENV = os.getenv("PROCESS_YEAR")   # ej. "2020"
PROCESS_MONTH_ENV = os.getenv("PROCESS_MONTH") # ej. "5"

process_year: Optional[int] = int(PROCESS_YEAR_ENV) if PROCESS_YEAR_ENV else None
process_month: Optional[int] = int(PROCESS_MONTH_ENV) if PROCESS_MONTH_ENV else None

print("================================ SILVER MODEL CONFIG ================================")
print(f"BASE_BUCKET   = {BASE_BUCKET}")
print(f"S3_BASE       = {S3_BASE}")
print(f"PROCESS_YEAR  = {process_year}")
print(f"PROCESS_MONTH = {process_month}")
print("====================================================================================")


def main() -> None:
    """
    Orquesta la ejecución de:
      - Dimensiones (full siempre)
      - Facts (full o incremental según process_year / process_month).
    """
    spark = create_spark_session("Huella_Hidrica_Silver_Model")

    try:
        # =====================================================
        # 1) DIMENSIONES (FULL REFRESH)
        # =====================================================
        print("================================================================================")
        print(">>> CREANDO TABLAS DE DIMENSIONES (full refresh) <<<")
        print("================================================================================")

        silver_dims_job.run(spark)

        print("DIMs creadas/actualizadas correctamente.\n")

        # =====================================================
        # 2) FACT climate_monthly
        #    - process_year: filtra año
        #    - process_month: filtra mes 
        # =====================================================
        print("================================================================================")
        print(f">>> CREANDO FACT climate_monthly (year={process_year}, month={process_month}) <<<")
        print("================================================================================")

        silver_fact_climate_monthly.run(
            spark,
            process_year_param=process_year,
            process_month_param=process_month,
        )

        print("Fact climate_monthly creada/actualizada correctamente.\n")

        # =====================================================
        # 3) FACT climate_annual (granularidad: año)
        # =====================================================
        print("================================================================================")
        print(f">>> CREANDO FACT climate_annual (year={process_year}) <<<")
        print("================================================================================")

        silver_fact_climate_annual.run(
            spark,
            process_year_param=process_year,
        )

        print("Fact climate_annual creada/actualizada correctamente.\n")

        # =====================================================
        # 4) FACT wash_coverage (JMP) (granularidad: año)
        # =====================================================
        print("================================================================================")
        print(f">>> CREANDO FACT wash_coverage (JMP) (year={process_year}) <<<")
        print("================================================================================")

        silver_fact_wash_coverage_jmp.run(
            spark,
            process_year_param=process_year,
        )

        print("Fact wash_coverage creada/actualizada correctamente.\n")

        # =====================================================
        # 5) FACT socioeconomic (World Bank) (granularidad: año)
        # =====================================================
        print("================================================================================")
        print(f">>> CREANDO FACT socioeconomic (World Bank) (year={process_year}) <<<")
        print("================================================================================")

        silver_fact_socioeconomic.run(
            spark,
            process_year_param=process_year,
        )

        print("Fact socioeconomic creada/actualizada correctamente.\n")

        print(">>> ORQUESTACIÓN MODELO SILVER FINALIZADA OK. <<<")

    finally:
        print("\nCerrando SparkSession...")
        spark.stop()
        print("SparkSession cerrada.")


if __name__ == "__main__":
    main()


