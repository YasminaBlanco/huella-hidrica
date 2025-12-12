# gold_kpi03_critical_zones.py
#
# Zonas Críticas (Clima + Saneamiento) para México y Argentina.
#


from pyspark.sql import DataFrame
from pyspark.sql import functions as F

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
    # Servicio SANITATION
    SANITATION_SERVICE_TYPE_KEY = 0
    # Niveles que consideramos "mal saneamiento"
    SANITATION_BAD_LEVEL_KEYS = [1, 2, 4]
    # Sólo urbano
    SANITATION_RESIDENCE_KEYS = [1]

    # Umbral de saneamiento básico considerado "adecuado"
    SANITATION_THRESHOLD = 80.0

    # Solo analizamos estos países
    TARGET_COUNTRIES_ISO3 = ["MEX", "ARG"]

    # Umbrales para tendencia climática (correlación año vs precipitación)
    CLIMATE_DECREASING_CORR_THRESHOLD = -0.3
    CLIMATE_INCREASING_CORR_THRESHOLD = 0.3
    MIN_YEARS_FOR_TREND = 3

    def __init__(self, spark, silver_model_base_path, gold_model_base_path):
        super().__init__(spark, silver_model_base_path, gold_model_base_path)
        
        self.spark.conf.set("spark.sql.shuffle.partitions", "16")

    # ------------------------------------------------------------------
    # Metadatos del KPI
    # ------------------------------------------------------------------
    def kpi_name(self) -> str:
        return "KPI03_Critical_Zones_Climate_Sanitation"

    def output_path(self) -> str:
        return f"{self.gold_base}/kpi03_critical_zones"

    # ------------------------------------------------------------------
    # Helper internos
    # ------------------------------------------------------------------
    def _year_from_date_key(self, col_name: str):
        """
        Deriva el año a partir de date_key (YYYYMMDD) usando aritmética:
        (YYYYMMDD / 10000) -> YYYY
        """
        return (F.col(col_name).cast("bigint") / F.lit(10000)).cast("int")

    # ------------------------------------------------------------------
    # Lógica principal
    # ------------------------------------------------------------------
    def build(self) -> DataFrame:
        self.log("Leyendo dimensiones country y province...")

        # ==========================
        # Dimensión país
        # ==========================
        country_df = self.read_silver_table(self.COUNTRY_TABLE).select(
            "country_key", "country_iso3", "country_name"
        )
        B_country_df = F.broadcast(country_df)

        # Países objetivo (MEX / ARG) -> lista de country_key
        target_countries_df = country_df.filter(
            F.col("country_iso3").isin(self.TARGET_COUNTRIES_ISO3)
        )
        target_country_keys = [
            row["country_key"]
            for row in target_countries_df.select("country_key").distinct().collect()
        ]

        # ==========================
        # Dimensión provincia
        # ==========================
        province_df = self.read_silver_table(self.PROVINCE_TABLE).select(
            self.PROVINCE_KEY_COL, "country_iso3", self.PROVINCE_NAME_COL
        )

        province_with_country = (
            province_df.join(country_df, on="country_iso3", how="left")
            .filter(F.col("country_iso3").isin(self.TARGET_COUNTRIES_ISO3))
            .select(
                "country_key",
                "country_iso3",
                "country_name",
                self.PROVINCE_KEY_COL,
                self.PROVINCE_NAME_COL,
            )
        )
        B_province_with_country = F.broadcast(province_with_country)

        # Lista de province_key objetivo (solo MEX/ARG)
        province_keys = [
            row[self.PROVINCE_KEY_COL]
            for row in province_with_country.select(self.PROVINCE_KEY_COL)
            .distinct()
            .collect()
        ]

        # =====================================================
        # 1) Saneamiento anual (urbano) por país / año
        # =====================================================
        self.log("Preparando saneamiento urbano por país/año...")

        wash_raw = (
            self.read_silver_table(self.WASH_TABLE)
            .where(F.col("country_key").isin(target_country_keys))
            .select(
                "country_key",
                "date_key",
                "residence_type_key",
                "service_type_key",
                "service_level_key",
                F.col("coverage_pct").cast("double").alias("coverage_pct"),
            )
        )

        wash_with_year = wash_raw.withColumn(
            "year", self._year_from_date_key("date_key")
        )

        wash_sanitation = wash_with_year.filter(
            (F.col("service_type_key") == self.SANITATION_SERVICE_TYPE_KEY)
            & (F.col("residence_type_key").isin(self.SANITATION_RESIDENCE_KEYS))
        )

        sanitation_yearly = (
            wash_sanitation.groupBy("country_key", "year")
            .agg(
                F.sum(
                    F.when(
                        F.col("service_level_key").isin(
                            self.SANITATION_BAD_LEVEL_KEYS
                        ),
                        F.col("coverage_pct"),
                    ).otherwise(F.lit(0.0))
                ).alias("pct_bad_sanitation")
            )
            .withColumn(
                "sanitation_basic_pct",
                F.lit(100.0) - F.col("pct_bad_sanitation"),
            )
            .withColumn(
                "is_low_sanitation",
                F.col("sanitation_basic_pct") < self.SANITATION_THRESHOLD,
            )
        )

        sanitation_yearly = sanitation_yearly.cache()

        if sanitation_yearly.rdd.isEmpty():
            self.log(
                "No hay datos de saneamiento para los países objetivo. "
                "Devolviendo DataFrame vacío."
            )
            schema = (
                "country_key INT, country_name STRING, "
                f"{self.PROVINCE_KEY_COL} INT, {self.PROVINCE_NAME_COL} STRING, "
                "year INT, "
                "sanitation_basic_pct DOUBLE, is_low_sanitation BOOLEAN, "
                "precip_total_mm_year DOUBLE, climate_trend STRING, "
                "is_climate_neg_trend BOOLEAN, is_critical_zone BOOLEAN"
            )
            return self.spark.createDataFrame([], schema)

        # Rango de años observado en saneamiento
        stats_years = sanitation_yearly.agg(
            F.min("year").alias("min_year"),
            F.max("year").alias("max_year"),
        ).collect()[0]
        min_year = stats_years["min_year"]
        max_year = stats_years["max_year"]

        self.log(
            f"Rango de años en saneamiento (para recortar clima): {min_year} - {max_year}"
        )

        # Nombres e ISO3 al DF de saneamiento
        sanitation_yearly = sanitation_yearly.join(
            B_country_df.select("country_key", "country_name", "country_iso3"),
            on="country_key",
            how="left",
        )

        B_sanitation_yearly = F.broadcast(
            sanitation_yearly.select(
                "country_key",
                "country_name",
                "country_iso3",
                "year",
                "sanitation_basic_pct",
                "is_low_sanitation",
            )
        )

        # =====================================================
        # 2) Clima mensual -> clima anual por provincia
        # =====================================================
        self.log(
            "Preparando clima mensual filtrado por provincias objetivo y rango de años..."
        )

        climate_raw = (
            self.read_silver_table(self.CLIMATE_TABLE)
            .where(F.col(self.PROVINCE_KEY_COL).isin(province_keys))
            .select(
                self.PROVINCE_KEY_COL,
                "date_key",
                F.col("precip_total_mm").cast("double").alias("precip_total_mm"),
            )
        )

        climate_with_year = (
            climate_raw.withColumn("year", self._year_from_date_key("date_key"))
            .filter(
                (F.col("year") >= F.lit(min_year))
                & (F.col("year") <= F.lit(max_year))
            )
        )

        climate_enriched = climate_with_year.join(
            B_province_with_country, on=self.PROVINCE_KEY_COL, how="inner"
        )

        self.log("Agregando clima anual (suma de precipitación mensual).")

        climate_yearly = (
            climate_enriched.groupBy(
                "country_key",
                "country_name",
                "country_iso3",
                self.PROVINCE_KEY_COL,
                self.PROVINCE_NAME_COL,
                "year",
            )
            .agg(
                F.sum("precip_total_mm").alias("precip_total_mm_year"),
            )
        )

        # =====================================================
        # 3) Tendencia climática por provincia
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
                    F.col("corr_year_precip")
                    <= self.CLIMATE_DECREASING_CORR_THRESHOLD,
                    F.lit("decreasing"),
                )
                .when(
                    F.col("corr_year_precip")
                    >= self.CLIMATE_INCREASING_CORR_THRESHOLD,
                    F.lit("increasing"),
                )
                .otherwise(F.lit("stable")),
            )
            .withColumn(
                "is_climate_neg_trend",
                F.col("climate_trend") == F.lit("decreasing"),
            )
        )

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
        # 4) Combinar clima (provincia/año) + saneamiento (país/año)
        # =====================================================
        self.log(
            "Combinando clima por provincia/año con saneamiento por país/año..."
        )

        zones = (
            climate_yearly.join(
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
            .join(
                B_sanitation_yearly,
                on=["country_key", "country_name", "country_iso3", "year"],
                how="left",
            )
        )

        zones_complete = zones.filter(F.col("sanitation_basic_pct").isNotNull())

        zones_complete = zones_complete.withColumn(
            "is_critical_zone",
            F.col("is_low_sanitation") & F.col("is_climate_neg_trend"),
        )

        # =====================================================
        # 5) Select final
        # =====================================================
        final_df = zones_complete.select(
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
