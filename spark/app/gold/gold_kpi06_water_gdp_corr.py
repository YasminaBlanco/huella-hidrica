from base_gold_model_job import BaseGoldKPIJob
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# Indicadores de WDI
WDI_INDICATORS = {
    "gdp_per_capita_key": 0,  # Clave confirmada para 'NY.GDP.PCAP.CD'.
}

# Claves de la dimensión WASH
WASH_KEYS = {
    # CLAVE: 2 es 'drinking water' (Tipo de Servicio)
    "water_type_key": 2,
    # CLAVES: 0 = basic service, 3 = safely managed service (Nivel de Servicio)
    "safe_levels": [0, 3],
}

# Umbrales para el semáforo
UMBRALES = {
    "verde_max_abs_corr": 0.3,
    "amarillo_max_abs_corr": 0.6,
}


class GoldKPI06(BaseGoldKPIJob):
    """
    KPI 6: Correlación agua segura vs PIB per cápita (región).
    """

    def kpi_name(self) -> str:
        return "kpi06_water_gdp_corr"

    def output_path(self) -> str:
        return f"{self.gold_base}/{self.kpi_name()}"

    def _get_safe_water_pct(self) -> DataFrame:
        """Calcula el % de agua segura (total país/año)."""
        self.log("Calculando % agua segura total país/año desde wash_coverage...")

        wash_df = self.read_silver_table("wash_coverage")

        water_safe_df = (
            wash_df.filter(
                # Usando clave 2 para 'drinking water'
                (F.col("service_type_key") == WASH_KEYS["water_type_key"])
                &
                # Usando claves 0 y 3 para niveles seguros/básicos
                (F.col("service_level_key").isin(WASH_KEYS["safe_levels"]))
            )
            .withColumn(
                # Crear la columna 'year' a partir de 'date_key'
                "year",
                F.substring(F.col("date_key").cast("string"), 1, 4).cast("int"),
            )
            .withColumn(
                # Castear la cobertura a un tipo numérico (decimal)
                "coverage_pct_num",
                F.col("coverage_pct").cast("decimal(10, 4)"),
            )
            .filter(
                # Eliminar filas donde el casting falló
                F.col("coverage_pct_num").isNotNull()
            )
        )

        # --- Lógica de Debugging para 0 filas ---
        if water_safe_df.count() == 0:
            unique_service_levels = [
                row["service_level_key"]
                for row in wash_df.select("service_level_key")
                .distinct()
                .limit(20)
                .collect()
            ]
            unique_service_types = [
                row["service_type_key"]
                for row in wash_df.select("service_type_key")
                .distinct()
                .limit(20)
                .collect()
            ]
            print("unique_service_levels", unique_service_levels.count())
            print("unique_service_types", unique_service_types.count())
        # ---------------------------------------

        # Agregación total país/año
        # Sumar la cobertura neta por niveles de servicio (0 y 3) para obtener el total 'safe water'
        water_pct_df = water_safe_df.groupBy("country_key", "year").agg(
            # Usar la columna numérica casteada
            F.sum("coverage_pct_num").alias("safe_water_pct_total")
        )

        return water_pct_df

    def _get_gdp_per_capita(self) -> DataFrame:
        """Extrae el PIB per cápita desde socioeconomic."""
        self.log("Extrayendo PIB per cápita desde socioeconomic...")

        socio_df = self.read_silver_table("socioeconomic")

        gdp_df = (
            socio_df.filter(
                # USANDO CLAVE NUMÉRICA DE LA DIMENSIÓN
                F.col("indicator_key")
                == WDI_INDICATORS["gdp_per_capita_key"]
            )
            .withColumn(
                # Crear la columna 'year'
                "year",
                F.substring(F.col("date_key").cast("string"), 1, 4).cast("int"),
            )
            .select(
                "country_key", "year", F.col("indicator_value").alias("gdp_per_capita")
            )
        )

        # --- Lógica de Debugging para 0 filas ---
        if gdp_df.count() == 0:
            unique_indicators = [
                row["indicator_key"]
                for row in socio_df.select("indicator_key")
                .distinct()
                .limit(20)
                .collect()
            ]
            print("unique_indicators", unique_indicators.count())
        # ---------------------------------------

        # Aseguramos el casting de la columna valor para cálculos
        gdp_df = gdp_df.withColumn(
            "gdp_per_capita", F.col("gdp_per_capita").cast("decimal(18,2)")
        ).filter(
            # Eliminar filas donde el casting falló
            F.col("gdp_per_capita").isNotNull()
        )

        return gdp_df

    def build(self) -> DataFrame:
        # 1. Obtener métricas a nivel país/año
        water_df = self._get_safe_water_pct()

        gdp_df = self._get_gdp_per_capita()

        # 2. Unir ambas métricas para crear la tabla base country_year_metrics
        base_metrics_df = water_df.join(
            gdp_df, on=["country_key", "year"], how="inner"
        ).na.drop(subset=["safe_water_pct_total", "gdp_per_capita"])

        if base_metrics_df.count() == 0:
            self.log(
                "WARNING: La unión no produjo resultados. Revisa los datos de 'country_key' y 'year' en ambas fuentes."
            )
            # Si no hay datos, devolvemos un DataFrame vacío
            return self.spark.createDataFrame(
                [],
                (
                    "region_name: string, "
                    "year: int, "
                    "n_countries: int, "
                    "corr_water_vs_gdp: decimal(6,3), "
                    "corr_abs_value: decimal(6,3), "
                    "avg_safe_water_pct: decimal(5,2), "
                    "avg_gdp_per_capita: decimal(18,2), "
                    "risk_level: string"
                ),
            )

        # 3. Unir con dim_country para obtener la región
        dim_country = self.read_silver_table("country").select(
            "country_key", "region_name"
        )
        country_year_metrics = base_metrics_df.join(
            dim_country, on="country_key", how="inner"
        )

        # 4. Agrupar por región/año y calcular la correlación y promedios
        self.log("Agrupando por región/año y calculando la correlación de Pearson...")
        final_df = (
            country_year_metrics.groupBy("region_name", "year")
            .agg(
                F.count("country_key").alias("n_countries"),
                # Correlación de Pearson
                F.corr("safe_water_pct_total", "gdp_per_capita").alias(
                    "corr_water_vs_gdp"
                ),
                F.avg("safe_water_pct_total").alias("avg_safe_water_pct"),
                # 'gdp_per_capita' de la tabla base, no 'avg_gdp_per_capita'
                F.avg("gdp_per_capita").alias("avg_gdp_per_capita"),
            )
            .filter(F.col("n_countries") >= 2)
        )  # Solo regiones con 2+ países para correlación

        # 5. Calcular valor absoluto de la correlación
        final_df = final_df.withColumn(
            "corr_abs_value", F.abs(F.col("corr_water_vs_gdp"))
        )

        # 6. Aplicar Semáforo
        # Semáforo del KPI: Verde < 0.3, Amarillo 0.3-0.6, Rojo >=0.6
        final_df = final_df.withColumn(
            "risk_level",
            F.when(F.col("corr_abs_value") >= UMBRALES["amarillo_max_abs_corr"], "Rojo")
            .when(F.col("corr_abs_value") >= UMBRALES["verde_max_abs_corr"], "Amarillo")
            .otherwise("Verde"),
        )

        # 7. Seleccionar y ordenar columnas
        return final_df.select(
            F.col("region_name").cast("string"),
            F.col("year").cast("int"),
            F.col("n_countries").cast("int"),
            F.col("corr_water_vs_gdp").cast("decimal(6,3)"),
            F.col("corr_abs_value").cast("decimal(6,3)"),
            F.col("avg_safe_water_pct").cast("decimal(5,2)"),
            F.col("avg_gdp_per_capita").cast("decimal(18,2)"),
            F.col("risk_level").cast("string"),
        )


def run(
    spark: SparkSession,
    silver_model_base_path: str,
    gold_model_base_path: str,
    write_mode: str = "overwrite",
) -> None:
    """Función de conveniencia para ejecutar el job desde el orquestador."""
    job = GoldKPI06(
        spark=spark,
        silver_model_base_path=silver_model_base_path,
        gold_model_base_path=gold_model_base_path,
        write_mode=write_mode,
    )
    job.run()
