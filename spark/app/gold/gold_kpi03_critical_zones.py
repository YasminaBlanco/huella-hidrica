# gold_kpi03_critical_zones.py
#
# Zonas Críticas (Clima + Saneamiento) para México y Argentina.

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from base_gold_model_job import BaseGoldKPIJob


class GoldKPI03CriticalZones(BaseGoldKPIJob):
    """
    KPI 03 - Zonas críticas para inversión (clima + saneamiento, México y Argentina)

    Pregunta de negocio:
      En México y Argentina, ¿podemos localizar zonas donde coinciden baja
      cobertura de saneamiento y una tendencia climática de disminución de lluvias?

    """

    # -------- Tablas SILVER --------

    CLIMATE_TABLE = "climate_monthly"
    WASH_TABLE = "wash_coverage"
    COUNTRY_TABLE = "country"
    PROVINCE_TABLE = "province"

    PROVINCE_KEY_COL = "province_key"
    PROVINCE_NAME_COL = "province_name"

    # -------- Config constantes --------
    SANITATION_SERVICE_TYPE_KEY = 0
    SANITATION_BAD_LEVEL_KEYS = [1, 2, 4]
    SANITATION_RESIDENCE_KEYS = [1]  # Urbano
    SANITATION_THRESHOLD = 80.0

    TARGET_COUNTRIES_ISO3 = ["MEX", "ARG"]

    CLIMATE_DECREASING_CORR_THRESHOLD = -0.3
    CLIMATE_INCREASING_CORR_THRESHOLD = 0.3
    MIN_YEARS_FOR_TREND = 3

    def kpi_name(self) -> str:
        return "KPI03_Critical_Zones_Climate_Sanitation"

    def output_path(self) -> str:
        return f"{self.gold_base}/kpi03_critical_zones"

    # ----------------- Helper de Fecha  -----------------

    def _year_from_date_key(self, col_name: str):
        """
        Deriva el año a partir de date_key (YYYYMMDD)usando aritmética
        """
        # (YYYYMMDD / 10000) -> YYYY.MMDD... -> YYYY
        return (F.col(col_name).cast("bigint") / F.lit(10000)).cast("int")

    # ----------------- Lógica principal OPTIMIZADA -----------------

    def build(self) -> DataFrame:
        self.log("Leyendo dimensiones country y province...")

        # Dim país
        country_df = self.read_silver_table(self.COUNTRY_TABLE).select(
            "country_key",
            "country_iso3",
            "country_name",
        )
        # PREPARACIÓN BROADCAST
        B_country_df = F.broadcast(country_df)

        # Dim provincia
        province_df = self.read_silver_table(self.PROVINCE_TABLE).select(
            self.PROVINCE_KEY_COL,
            "country_iso3",
            self.PROVINCE_NAME_COL,
        )

        province_with_country = province_df.join(
            country_df, on="country_iso3", how="left"
        )
        # PREPARACIÓN BROADCAST
        B_province_with_country = F.broadcast(province_with_country)

        # =====================================================
        # 1) Clima mensual- clima anual
        # =====================================================
        self.log("Preparando clima mensual (derivando year y aplicando filtros)...")

        climate_raw = self.read_silver_table(self.CLIMATE_TABLE).select(
            self.PROVINCE_KEY_COL,
            "date_key",
            "precip_total_mm",
        )

        climate_with_year = climate_raw.withColumn(
            "year", self._year_from_date_key("date_key")
        )

        climate_enriched = (
            climate_with_year
            # USO DE BROADCAST
            .join(B_province_with_country, on=self.PROVINCE_KEY_COL, how="left").filter(
                F.col("country_iso3").isin(self.TARGET_COUNTRIES_ISO3)
            )
        )

        self.log("Agregando clima anual (suma de precipitación mensual)...")

        climate_yearly = climate_enriched.groupBy(
            "country_key",
            "country_name",
            "country_iso3",
            self.PROVINCE_KEY_COL,
            self.PROVINCE_NAME_COL,
            "year",
        ).agg(
            # Cast a double dentro de F.sum()
            F.sum(F.col("precip_total_mm").cast("double")).alias("precip_total_mm_year")
        )

        # =====================================================
        # 2) Tendencia climática
        # =====================================================
        self.log("Calculando tendencia climática por provincia...")

        climate_trend_stats = (
            climate_yearly.groupBy(
                "country_key",
                "country_name",
                "country_iso3",
                self.PROVINCE_KEY_COL,
                self.PROVINCE_NAME_COL,
            )
            .agg(
                F.countDistinct("year").alias("years_climate_observed"),
                F.corr("year", "precip_total_mm_year").alias("corr_year_precip"),
            )
            .withColumn(
                "climate_trend",
                F.when(
                    F.col("years_climate_observed") < self.MIN_YEARS_FOR_TREND,
                    F.lit("uncertain"),
                )
                .when(
                    F.col("corr_year_precip") <= self.CLIMATE_DECREASING_CORR_THRESHOLD,
                    F.lit("decreasing"),
                )
                .when(
                    F.col("corr_year_precip") >= self.CLIMATE_INCREASING_CORR_THRESHOLD,
                    F.lit("increasing"),
                )
                .otherwise(F.lit("stable")),
            )
            .withColumn(
                "is_climate_neg_trend", F.col("climate_trend") == F.lit("decreasing")
            )
        )

        # PREPARACIÓN BROADCAST
        B_climate_trend_stats = F.broadcast(
            climate_trend_stats.select(
                "country_key",
                "country_name",
                "country_iso3",
                self.PROVINCE_KEY_COL,
                self.PROVINCE_NAME_COL,
                "climate_trend",
                "is_climate_neg_trend",
            )
        )

        # =====================================================
        # 3) Saneamiento anual
        # =====================================================
        self.log("Preparando saneamiento urbano por país / año...")

        wash_raw = self.read_silver_table(self.WASH_TABLE).select(
            "country_key",
            "date_key",
            "residence_type_key",
            "service_type_key",
            "service_level_key",
            F.col("coverage_pct").cast("double").alias("coverage_pct"),
        )
        wash_with_year = wash_raw.withColumn(
            "year", self._year_from_date_key("date_key")
        )

        wash_sanitation = (
            wash_with_year.filter(
                F.col("service_type_key") == self.SANITATION_SERVICE_TYPE_KEY
            )
            .filter(F.col("residence_type_key").isin(self.SANITATION_RESIDENCE_KEYS))
            # USO DE BROADCAST
            .join(B_country_df, on="country_key", how="left")
            .filter(F.col("country_iso3").isin(self.TARGET_COUNTRIES_ISO3))
        )

        sanitation_yearly = (
            wash_sanitation.groupBy(
                "country_key", "country_name", "country_iso3", "year"
            )
            .agg(
                F.sum(
                    F.when(
                        F.col("service_level_key").isin(self.SANITATION_BAD_LEVEL_KEYS),
                        F.col("coverage_pct"),
                    ).otherwise(F.lit(0.0))
                ).alias("pct_bad_sanitation")
            )
            .withColumn(
                "sanitation_basic_pct", F.lit(100.0) - F.col("pct_bad_sanitation")
            )
            .withColumn(
                "is_low_sanitation",
                F.col("sanitation_basic_pct") < self.SANITATION_THRESHOLD,
            )
        )

        # PREPARACIÓN BROADCAST
        B_sanitation_yearly = F.broadcast(
            sanitation_yearly.select(
                "country_key",
                "year",
                "sanitation_basic_pct",
                "is_low_sanitation",
            )
        )

        # =====================================================
        # 4) Combinar clima + saneamiento
        # =====================================================

        self.log("Combinando clima por provincia/año con saneamiento por país/año...")

        zones = (
            climate_yearly
            # USO DE BROADCAST
            .join(
                B_climate_trend_stats,
                on=[
                    "country_key",
                    "country_name",
                    "country_iso3",
                    self.PROVINCE_KEY_COL,
                    self.PROVINCE_NAME_COL,
                ],
                how="left",
            )
            # USO DE BROADCAST
            .join(B_sanitation_yearly, on=["country_key", "year"], how="left")
        )

        # =====================================================
        # 5-8) Filtrar, marcar crítica y calcular stats
        # =====================================================

        zones_complete = zones.filter(F.col("sanitation_basic_pct").isNotNull())
        zones_complete = zones_complete.withColumn(
            "is_critical_zone",
            F.col("is_low_sanitation") & F.col("is_climate_neg_trend"),
        )

        critical_counts = (
            zones_complete.groupBy("country_key", "country_name", "year")
            .agg(
                F.sum(F.when(F.col("is_critical_zone"), F.lit(1)).otherwise(F.lit(0)))
                .cast("int")
                .alias("critical_zones_count")
            )
            .withColumn(
                "risk_level",
                F.when(F.col("critical_zones_count") <= 5, F.lit("green"))
                .when(F.col("critical_zones_count") <= 20, F.lit("yellow"))
                .otherwise(F.lit("red")),
            )
        )
        # PREPARACIÓN BROADCAST
        B_critical_counts = F.broadcast(
            critical_counts.select(
                "country_key",
                "country_name",
                "year",
                "critical_zones_count",
                "risk_level",
            )
        )

        coverage_stats = zones_complete.groupBy(
            "country_key", "country_name", self.PROVINCE_KEY_COL, self.PROVINCE_NAME_COL
        ).agg(
            F.countDistinct("year").alias("years_observed"),
            F.min("year").alias("start_year"),
            F.max("year").alias("end_year"),
        )
        # PREPARACIÓN BROADCAST
        B_coverage_stats = F.broadcast(
            coverage_stats.select(
                "country_key",
                "country_name",
                self.PROVINCE_KEY_COL,
                self.PROVINCE_NAME_COL,
                "start_year",
                "end_year",
                "years_observed",
            )
        )

        # =====================================================
        # 9) Select final
        # =====================================================

        final_df = (
            zones_complete
            # USO DE BROADCAST
            .join(
                B_critical_counts,
                on=["country_key", "country_name", "year"],
                how="left",
            )
            # USO DE BROADCAST
            .join(
                B_coverage_stats,
                on=[
                    "country_key",
                    "country_name",
                    self.PROVINCE_KEY_COL,
                    self.PROVINCE_NAME_COL,
                ],
                how="left",
            ).select(
                "country_key",
                "country_name",
                self.PROVINCE_KEY_COL,
                self.PROVINCE_NAME_COL,
                "year",
                "sanitation_basic_pct",
                "is_low_sanitation",
                "precip_total_mm_year",
                "climate_trend",
                "is_climate_neg_trend",
                "is_critical_zone",
                "critical_zones_count",
                "risk_level",
                "start_year",
                "end_year",
                "years_observed",
            )
        )

        return final_df


def run(
    spark,
    silver_model_base_path: str,
    gold_model_base_path: str,
) -> None:
    job = GoldKPI03CriticalZones(
        spark=spark,
        silver_model_base_path=silver_model_base_path,
        gold_model_base_path=gold_model_base_path,
    )
    job.run()
