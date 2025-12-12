# gold_kpi01_climate_water.py
#
# Correlación Clima vs Acceso al Agua (MEX/ARG).

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame
from base_gold_model_job import BaseGoldKPIJob


class GoldKPI01ClimateWater(BaseGoldKPIJob):
    """
    KPI 01 - Impacto del clima en el acceso al agua (México y Argentina).

    Correlación entre la variación anual de precipitación y la variación anual
    de cobertura de agua segura, por país (MEX/ARG) y tipo de área (urbano/rural).
    """

    # Tablas SILVER
    CLIMATE_TABLE = "climate_annual"
    WASH_TABLE = "wash_coverage"
    COUNTRY_TABLE = "country"
    RES_TYPE_TABLE = "residence_type"

    # Filtros de negocio
    COUNTRIES_ISO3_FILTER = ["MEX", "ARG"]
    SAFE_SERVICE_TYPE_KEYS = [2]  # drinking water
    SAFE_SERVICE_LEVEL_KEYS = [5]  # at least basic
    RESIDENCE_TYPE_KEYS = [1, 2]  # urban, rural

    def kpi_name(self) -> str:
        return "KPI01_Climate_Water"

    def output_path(self) -> str:
        return f"{self.gold_base}/kpi01_climate_water"

    # ---------------- Helper de Fecha  ----------------

    def _year_from_date_key(self, col_name: str):
        """
        Deriva el año a partir de date_key (YYYYMMDD) usando aritmética
        """
        return (F.col(col_name).cast("bigint") / F.lit(10000)).cast("int")

    # ---------------- Lógica principal OPTIMIZADA ----------------

    def build(self) -> DataFrame:
        # ======================
        # 1) Leer y Broadcast Dimensiones
        # ======================
        self.log("Leyendo y preparando dimensiones para Broadcast...")

        country_df = self.read_silver_table(self.COUNTRY_TABLE).select(
            "country_key",
            "country_iso3",
            "country_name",
        )
        # PREPARACIÓN BROADCAST: Dimensión País
        B_country_df = F.broadcast(country_df)

        res_type_df = self.read_silver_table(self.RES_TYPE_TABLE).select(
            "residence_type_key",
            "residence_type_desc",
        )
        # PREPARACIÓN BROADCAST: Dimensión Tipo de Residencia
        B_res_type_df = F.broadcast(res_type_df)

        # Leer Tablas de Hechos
        climate_raw = self.read_silver_table(self.CLIMATE_TABLE).select(
            "country_key",
            "date_key",
            "precip_total_mm_year",
        )
        wash_raw = self.read_silver_table(self.WASH_TABLE).select(
            "country_key",
            "date_key",
            "residence_type_key",
            "service_type_key",
            "service_level_key",
            F.col("coverage_pct").cast("double").alias("coverage_pct"),
        )

        # ============================================================
        # 2) Derivar año en hechos
        # ============================================================
        self.log("Derivando columna 'year' a partir de date_key...")

        climate_with_year = climate_raw.withColumn(
            "year", self._year_from_date_key("date_key")
        )
        wash_with_year = wash_raw.withColumn(
            "year", self._year_from_date_key("date_key")
        )

        # ===============================================
        # 3) Filtrar WASH a agua segura y países MEX/ARG
        # ===============================================
        self.log(
            "Aplicando filtros de agua segura y usando Broadcast para enriquecimiento..."
        )

        wash_enriched = (
            wash_with_year
            # USO DE BROADCAST: Join con dimensiones pequeñas
            .join(B_country_df, on="country_key", how="left").join(
                B_res_type_df, on="residence_type_key", how="left"
            )
        )

        wash_safe = (
            wash_enriched.filter(F.col("country_iso3").isin(self.COUNTRIES_ISO3_FILTER))
            .filter(F.col("service_type_key").isin(self.SAFE_SERVICE_TYPE_KEYS))
            .filter(F.col("service_level_key").isin(self.SAFE_SERVICE_LEVEL_KEYS))
            .filter(F.col("residence_type_key").isin(self.RESIDENCE_TYPE_KEYS))
        )

        safe_count = wash_safe.count()
        if safe_count == 0:
            self.log("No se encontraron filas de agua segura. Devolviendo DF vacío.")
            # Definición del esquema para retorno seguro (omitiendo por brevedad)
            schema = (
                "country_key INT, country_name STRING, "
                "residence_type_key INT, residence_type_desc STRING, "
                "year INT, "
                "precip_total_mm_year DOUBLE, delta_precip_mm DOUBLE, "
                "safe_water_pct DOUBLE, delta_safe_water_pp DOUBLE, "
                "start_year INT, end_year INT, years_observed BIGINT, "
                "corr_precip_vs_water DOUBLE, corr_abs_value DOUBLE, "
                "risk_level STRING, impact_direction STRING"
            )
            return self.spark.createDataFrame([], schema)

        # Agregamos por país + tipo de residencia + año
        wash_safe_yearly = wash_safe.groupBy(
            "country_key",
            "country_iso3",
            "country_name",
            "residence_type_key",
            "residence_type_desc",
            "year",
        ).agg(F.max("coverage_pct").alias("safe_water_pct"))

        # ======================================================
        # 4) Rango dinámico de años basado solo en wash_coverage
        # ======================================================
        self.log("Calculando rango dinámico de años desde wash_coverage filtrado...")

        stats = wash_safe_yearly.agg(
            F.min("year").alias("min_year"), F.max("year").alias("max_year")
        ).collect()[0]
        min_year = stats["min_year"]
        max_year = stats["max_year"]

        self.log(f"Rango de años detectado: {min_year} - {max_year}")

        # ============================
        # 5) Preparar clima anual MEX/ARG
        # ============================
        self.log("Filtrando clima anual por países y rango de años...")

        climate_for_countries = (
            climate_with_year
            # USO DE BROADCAST: Join con dimensión país
            .join(B_country_df, on="country_key", how="left")
            .filter(F.col("country_iso3").isin(self.COUNTRIES_ISO3_FILTER))
            .filter(
                (F.col("year") >= F.lit(min_year)) & (F.col("year") <= F.lit(max_year))
            )
            .select(
                "country_key",
                "country_iso3",
                "country_name",
                "year",
                F.col("precip_total_mm_year")
                .cast("double")
                .alias("precip_total_mm_year"),
            )
        )

        # ============================
        # 6) Unir clima + agua segura
        # ============================
        self.log("Uniendo clima anual con agua segura por (country, year)...")

        series_df = climate_for_countries.join(
            wash_safe_yearly,
            on=["country_key", "country_iso3", "country_name", "year"],
            how="inner",
        ).select(
            "country_key",
            "country_name",
            "country_iso3",
            "residence_type_key",
            "residence_type_desc",
            "year",
            "precip_total_mm_year",
            "safe_water_pct",
        )

        if series_df.rdd.isEmpty():
            self.log(
                "Después de unir clima anual con agua segura, no quedaron filas. Devolviendo DataFrame vacío."
            )
            schema = (
                "country_key INT, country_name STRING, "
                "residence_type_key INT, residence_type_desc STRING, "
                "year INT, "
                "precip_total_mm_year DOUBLE, delta_precip_mm DOUBLE, "
                "safe_water_pct DOUBLE, delta_safe_water_pp DOUBLE, "
                "start_year INT, end_year INT, years_observed BIGINT, "
                "corr_precip_vs_water DOUBLE, corr_abs_value DOUBLE, "
                "risk_level STRING, impact_direction STRING"
            )
            return self.spark.createDataFrame([], schema)

        # ==========================================
        # 7) Calcular variaciones anuales (deltas)
        # ==========================================
        self.log("Calculando variaciones anuales (deltas)...")

        w = Window.partitionBy(
            "country_key",
            "country_name",
            "residence_type_key",
            "residence_type_desc",
        ).orderBy("year")

        deltas = (
            series_df.withColumn("prev_precip", F.lag("precip_total_mm_year").over(w))
            .withColumn("prev_safe", F.lag("safe_water_pct").over(w))
            .withColumn(
                "delta_precip_mm", F.col("precip_total_mm_year") - F.col("prev_precip")
            )
            .withColumn(
                "delta_safe_water_pp", F.col("safe_water_pct") - F.col("prev_safe")
            )
            .filter(F.col("prev_precip").isNotNull() & F.col("prev_safe").isNotNull())
        )

        # ==========================================
        # 8) Calcular correlación por país + residencia
        # ==========================================
        self.log("Calculando correlación entre deltas clima vs agua segura...")

        group_cols = [
            "country_key",
            "country_name",
            "residence_type_key",
            "residence_type_desc",
        ]

        agg_corr = deltas.groupBy(group_cols).agg(
            F.count("*").alias("years_observed"),
            F.min("year").alias("start_year"),
            F.max("year").alias("end_year"),
            F.corr("delta_precip_mm", "delta_safe_water_pp").alias(
                "corr_precip_vs_water"
            ),
        )

        # Lógica de Semáforo y Dirección
        agg_corr = agg_corr.withColumn(
            "corr_abs_value", F.abs(F.col("corr_precip_vs_water"))
        )
        agg_corr = agg_corr.withColumn(
            "risk_level",
            F.when(F.col("corr_precip_vs_water").isNull(), F.lit("gray"))
            .when(F.col("corr_abs_value") < 0.3, F.lit("green"))
            .when(F.col("corr_abs_value") < 0.6, F.lit("yellow"))
            .otherwise(F.lit("red")),
        ).withColumn(
            "impact_direction",
            F.when(F.col("corr_precip_vs_water").isNull(), F.lit("uncertain"))
            .when(F.col("corr_precip_vs_water") >= 0.2, F.lit("direct"))
            .when(F.col("corr_precip_vs_water") <= -0.2, F.lit("inverse"))
            .otherwise(F.lit("uncertain")),
        )

        # PREPARACIÓN BROADCAST
        B_agg_corr = F.broadcast(agg_corr)

        # ==========================================
        # 9) Combinar series anuales + métricas agregadas
        # ==========================================
        self.log("Combinando series anuales con correlación, riesgo y dirección...")

        final_df = (
            deltas
            # USO DE BROADCAST: Join de series con el DF de correlación
            .join(B_agg_corr, on=group_cols, how="left").select(
                "country_key",
                "country_name",
                "residence_type_key",
                "residence_type_desc",
                "year",
                "precip_total_mm_year",
                "delta_precip_mm",
                "safe_water_pct",
                "delta_safe_water_pp",
                "start_year",
                "end_year",
                "years_observed",
                "corr_precip_vs_water",
                "corr_abs_value",
                "risk_level",
                "impact_direction",
            )
        )

        return final_df


def run(
    spark,
    silver_model_base_path: str,
    gold_model_base_path: str,
) -> None:
    job = GoldKPI01ClimateWater(
        spark=spark,
        silver_model_base_path=silver_model_base_path,
        gold_model_base_path=gold_model_base_path,
    )
    job.run()
