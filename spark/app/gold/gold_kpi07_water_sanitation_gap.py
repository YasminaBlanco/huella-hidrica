from base_gold_model_job import BaseGoldKPIJob
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Claves de la dimensión WASH
WASH_KEYS = {
    "water_type_key": 2,  # 2 = 'drinking water'
    "sanitation_type_key": 0,  # 0 = 'sanitation'
    "safe_levels": [0, 3],  # 0 = 'basic service', 3 = 'safely managed service'
}

# Umbrales para el semáforo (ajustar según negocio)
UMBRALES = {
    "verde_max_abs_gap": 10.0,  # ABS(gap) < 10 p.p.
    "amarillo_max_abs_gap": 15.0,  # ABS(gap) < 15 p.p.
}


class GoldKPI07(BaseGoldKPIJob):
    """
    KPI 7: Brecha agua segura vs saneamiento seguro (país/año).
    La brecha se calcula como: (Agua Segura %) - (Saneamiento Seguro %)
    """

    def kpi_name(self) -> str:
        return "kpi07_water_sanitation_gap"

    def output_path(self) -> str:
        return f"{self.gold_base}/{self.kpi_name()}"

    def _get_safe_coverage_df(self, service_type_key: int, alias: str) -> DataFrame:
        """
        Calcula el % de cobertura básico/seguro para un service_type específico
        (Agua o Saneamiento) a nivel país/año.
        """
        self.log(
            f"Calculando % de cobertura segura para {alias} (Service Type Key: {service_type_key})..."
        )

        wash_df = self.read_silver_table("wash_coverage")

        # 1. Filtrar, castear cobertura y calcular el año en pasos secuenciales
        coverage_df = (
            wash_df.filter(
                (F.col("service_type_key") == service_type_key)
                & (F.col("service_level_key").isin(WASH_KEYS["safe_levels"]))
            )
            .withColumn(
                # Asegurar que la columna de cobertura es numérica
                "coverage_pct_num",
                F.col("coverage_pct").cast("decimal(10, 4)"),
            )
            .filter(F.col("coverage_pct_num").isNotNull())
            .withColumn(
                # Crear la columna 'year' antes del GROUP BY
                "year",
                F.substring(F.col("date_key").cast("string"), 1, 4).cast("int"),
            )
        )

        # 2. Agregación total país/año (Sumar la cobertura de los niveles 'Basic' y 'Safely Managed')
        agg_df = coverage_df.groupBy(
            "country_key", "year"  # 'year' ahora es una columna válida para agrupar
        ).agg(
            # Sumar la columna numérica casteada
            F.sum("coverage_pct_num").alias(alias)
        )

        return agg_df

    def build(self) -> DataFrame:
        # 1. Calcular % agua segura total país/año (Key 2: Drinking Water)
        water_df = self._get_safe_coverage_df(
            service_type_key=WASH_KEYS["water_type_key"], alias="water_basic_safe_pct"
        )

        # 2. Calcular % saneamiento seguro total país/año (Key 0: Sanitation)
        sanitation_df = self._get_safe_coverage_df(
            service_type_key=WASH_KEYS["sanitation_type_key"],
            alias="sanitation_basic_safe_pct",
        )

        # 3. Unir ambas series por país/año
        joined_df = water_df.join(
            sanitation_df, on=["country_key", "year"], how="inner"
        ).na.drop(subset=["water_basic_safe_pct", "sanitation_basic_safe_pct"])

        if joined_df.count() == 0:
            self.log(
                "WARNING: La unión no produjo resultados. Revisa los datos de 'country_key' y 'year' en ambas fuentes."
            )
            return self.spark.createDataFrame(
                [],
                "country_key: int, country_name: string, year: int, water_basic_safe_pct: decimal(5,2), sanitation_basic_safe_pct: decimal(5,2), gap_water_sanitation_pp: decimal(6,3), risk_level: string",
            )

        # 4. Calcular la brecha (Gap) en puntos porcentuales (p.p.)
        final_df = joined_df.withColumn(
            "gap_water_sanitation_pp",
            F.col("water_basic_safe_pct") - F.col("sanitation_basic_safe_pct"),
        )

        # 5. Unir con dim_country
        dim_country = self.read_silver_table("country").select(
            "country_key", "country_name"
        )
        final_df = final_df.join(dim_country, on="country_key", how="inner")

        # 6. Aplicar Semáforo
        # Semáforo del KPI: Verde < 10, Amarillo 10-15, Rojo >=15 (p.p.)
        final_df = final_df.withColumn(
            "risk_level",
            F.when(
                F.abs(F.col("gap_water_sanitation_pp"))
                >= UMBRALES["amarillo_max_abs_gap"],
                "Rojo",
            )
            .when(
                F.abs(F.col("gap_water_sanitation_pp"))
                >= UMBRALES["verde_max_abs_gap"],
                "Amarillo",
            )
            .otherwise("Verde"),
        )

        # 7. Seleccionar y ordenar columnas
        return final_df.select(
            F.col("country_key").cast("int"),
            F.col("country_name").cast("string"),
            F.col("year").cast("int"),
            F.col("water_basic_safe_pct").cast("decimal(5,2)"),
            F.col("sanitation_basic_safe_pct").cast("decimal(5,2)"),
            F.col("gap_water_sanitation_pp").cast("decimal(6,3)"),
            F.col("risk_level").cast("string"),
        )


def run(
    spark: SparkSession,
    silver_model_base_path: str,
    gold_model_base_path: str,
    write_mode: str = "overwrite",
) -> None:
    """Función de conveniencia para ejecutar el job desde el orquestador."""
    job = GoldKPI07(
        spark=spark,
        silver_model_base_path=silver_model_base_path,
        gold_model_base_path=gold_model_base_path,
        write_mode=write_mode,
    )
    job.run()
