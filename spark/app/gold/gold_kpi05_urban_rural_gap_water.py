from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from base_gold_model_job import BaseGoldKPIJob

# Claves de la dimensión WASH (ACTUALIZADAS con el último mapeo de residencia)

WASH_KEYS = {
    "water_type_key": 2,  # 2 = 'drinking water'
    # 3 = 'basic service', 7 = 'safely managed service'
    "safe_levels": [3, 7],
    # 1 = 'urban', 2 = 'rural'. (0 = 'total' es excluido)
    "residence_types": [1, 2],
}


# Umbrales para el semáforo (ajustar según negocio)

UMBRALES = {
    "verde_max_abs_gap": 10.0,  # ABS(gap) < 10 p.p.
    "amarillo_max_abs_gap": 20.0,  # ABS(gap) < 20 p.p. (Según especificación del semáforo)
}


class GoldKPI05(BaseGoldKPIJob):
    """KPI 5: Brecha urbano–rural en agua segura.
    La brecha se calcula como: (Agua Urbana %) - (Agua Rural %)
    """

    def kpi_name(self) -> str:
        return "kpi05_urban_rural_gap_water"

    def output_path(self) -> str:
        return f"{self.gold_base}/{self.kpi_name()}"

    def build(self) -> DataFrame:
        self.log(
            "Calculando % agua segura (básico/seguro) por país/año y residencia..."
        )

        # 1. Leer Fact y Dimensiones necesarias
        wash_df = self.read_silver_table("wash_coverage")

        # 2. FILTRO, CASTING Y CREACIÓN DE COLUMNA 'year' (CRÍTICO)
        water_safe_df = (
            wash_df.filter(
                # FILTRO: service_type_key = 2 ('drinking water')
                (F.col("service_type_key") == WASH_KEYS["water_type_key"])
                &
                # FILTRO: service_level_key IN (3, 7) (Niveles básico y seguro)
                (F.col("service_level_key").isin(WASH_KEYS["safe_levels"]))
                &
                # residence_type_key IN (1, 2) (urban y rural)
                (F.col("residence_type_key").isin(WASH_KEYS["residence_types"]))
            )
            .withColumn(
                # Se crea la columna 'year'
                "year",
                F.substring(F.col("date_key").cast("string"), 1, 4).cast("int"),
            )
            .withColumn(
                # Castear la columna de porcentaje a decimal para permitir F.sum()
                "coverage_pct_num",
                F.col("coverage_pct").cast("decimal(10, 4)"),
            )
            .select(
                # Selección explícita de columnas clave
                "country_key",
                "year",
                "residence_type_key",
                "coverage_pct_num",
            )
            .filter(
                # Eliminamos filas donde el porcentaje no pudo ser parseado
                F.col("coverage_pct_num").isNotNull()
            )
        )

        # 3. Leer dimensiones
        dim_country = self.read_silver_table("country").select(
            "country_key", "country_name"
        )

        # Leemos la dimensión de residencia. Las claves 1 y 2 corresponden a 'urban' y 'rural'
        dim_residence = self.read_silver_table("residence_type").select(
            "residence_type_key", "residence_type_desc"
        )

        # 4. Unir con dim_residence_type y normalizar 'residence_type_desc'
        water_safe_df = water_safe_df.join(
            dim_residence, on="residence_type_key", how="inner"
        ).withColumn(
            # Normalizamos la descripción para asegurar un pivot correcto,
            # ya que las descripciones son 'urban' y 'rural'.
            "residence_type_clean",
            F.lower(F.trim(F.col("residence_type_desc"))),
        )

        # 5. Agregar a nivel país/año/residencia limpia
        # Sumamos las coberturas de 'Basic' y 'Safely managed' para obtener el total.
        water_pct_df = water_safe_df.groupBy(
            "country_key", "year", "residence_type_clean"
        ).agg(F.sum("coverage_pct_num").alias("safe_water_pct"))

        # ====================================================================
        self.log(
            f"PUNTO DE CONTROL: Filas agregadas (country/year/residence) antes del pivot: {water_pct_df.count()}"
        )
        # ====================================================================

        # 6. Pivotar usando la columna limpia y los valores exactos 'urban' y 'rural'
        pivoted_df = (
            water_pct_df.groupBy("country_key", "year")
            .pivot("residence_type_clean", ["urban", "rural"])
            .agg(F.first("safe_water_pct"))
        )

        # Renombrar columnas pivotadas
        pivoted_df = pivoted_df.withColumnsRenamed(
            {
                "urban": "water_urban_pct",
                "rural": "water_rural_pct",
            }
        )

        # 7. Calcular la brecha (Gap)
        # Usamos coalesce para tratar los NULLs y permitir el cálculo de la brecha
        final_df = pivoted_df.withColumn(
            "gap_urban_rural_pp",
            F.coalesce(F.col("water_urban_pct"), F.lit(0))
            - F.coalesce(F.col("water_rural_pct"), F.lit(0)),
        )

        # 8. Unir con dim_country
        final_df = final_df.join(dim_country, on="country_key", how="inner").na.drop(
            subset=["water_urban_pct", "water_rural_pct"], how="all"
        )  # Eliminar si ambos son NULL

        # 9. Aplicar Semáforo
        final_df = final_df.withColumn(
            "risk_level",
            F.when(
                F.abs(F.col("gap_urban_rural_pp")) >= UMBRALES["amarillo_max_abs_gap"],
                "Rojo",
            )
            .when(
                F.abs(F.col("gap_urban_rural_pp")) >= UMBRALES["verde_max_abs_gap"],
                "Amarillo",
            )
            .otherwise("Verde"),
        )

        # 10. Seleccionar y ordenar columnas
        return final_df.select(
            F.col("country_key").cast("int"),
            F.col("country_name").cast("string"),
            F.col("year").cast("int"),
            F.col("water_urban_pct").cast("decimal(5,2)"),
            F.col("water_rural_pct").cast("decimal(5,2)"),
            F.col("gap_urban_rural_pp").cast("decimal(6,3)"),
            F.col("risk_level").cast("string"),
        )


def run(
    spark: SparkSession,
    silver_model_base_path: str,
    gold_model_base_path: str,
    write_mode: str = "overwrite",
) -> None:
    """Función de conveniencia para ejecutar el job desde el orquestador."""
    job = GoldKPI05(
        spark=spark,
        silver_model_base_path=silver_model_base_path,
        gold_model_base_path=gold_model_base_path,
        write_mode=write_mode,
    )
    job.run()
