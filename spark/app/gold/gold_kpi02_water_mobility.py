# gold_kpi02_water_mobility.py
#
# KPI 02 – Movilidad forzada para conseguir agua
# % de población cuya fuente principal de agua está a más de 30 minutos,
# por país, zona (urban/rural) y año.


from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql import DataFrame

from base_gold_model_job import BaseGoldKPIJob


class GoldKPI02WaterMobility(BaseGoldKPIJob):
    """
    KPI 02 - Movilidad forzada para conseguir agua.

    Métrica principal: porcentaje de población que tarda más de 30 minutos 
    en llegar a su fuente principal de agua, y su evolución anual.
    """

    # Tablas SILVER
    WASH_TABLE = "wash_coverage"
    COUNTRY_TABLE = "country"
    RES_TYPE_TABLE = "residence_type"

    # Filtros de negocio
    LATAM_ISO3 = []  # [] => todos los países con datos

    DRINKING_WATER_KEY = 2          # service_type_key = 2 -> drinking water
    LIMITED_SERVICE_KEY = 4         # service_level_key = 4 -> limited service (>30 min)
    RESIDENCE_TYPE_KEYS = [1, 2]    # 1 = urban, 2 = rural

    # Umbral para considerar que la tendencia empeora o mejora en puntos porcentuales
    UMBRAL_TENDENCIA_PP = 0.5

    def __init__(self, spark, silver_model_base_path, gold_model_base_path):
        super().__init__(spark, silver_model_base_path, gold_model_base_path)
        self.spark.conf.set("spark.sql.shuffle.partitions", "32")

    def kpi_name(self) -> str:
        return "KPI02_Water_Mobility"

    def output_path(self) -> str:
        return f"{self.gold_base}/kpi02_water_mobility"

    # ---------------- Helper de Fecha ----------------

    def _year_from_date_key(self, col_name: str):
        """
        Deriva el año a partir de date_key (YYYYMMDD) usando aritmética:
        (YYYYMMDD / 10000) -> YYYY.MMDD... -> YYYY
        """
        return (F.col(col_name).cast("bigint") / F.lit(10000)).cast("int")

    # --------------- Lógica principal  ---------------

    def build(self) -> DataFrame:
        self.log("Leyendo tablas SILVER necesarias para KPI 02 (wash + dims)...")

        # Fact WASH (sólo columnas necesarias)
        wash_raw = self.read_silver_table(self.WASH_TABLE).select(
            "country_key",
            "date_key",
            "residence_type_key",
            "service_type_key",
            "service_level_key",
            F.col("coverage_pct").cast("double").alias("coverage_pct"),
        )

        # Dimensión País 
        country_df = self.read_silver_table(self.COUNTRY_TABLE).select(
            "country_key",
            "country_name",
        )
        B_country_df = F.broadcast(country_df)

        # Dimensión Tipo de residencia
        res_type_df = self.read_silver_table(self.RES_TYPE_TABLE).select(
            "residence_type_key",
            "residence_type_desc",
        )
        B_res_type_df = F.broadcast(res_type_df)

        # -----------------------------
        # 1) Derivar año desde date_key
        # -----------------------------
        self.log("Derivando columna 'year' a partir de date_key...")

        wash_with_year = wash_raw.withColumn(
            "year", self._year_from_date_key("date_key")
        )

        # ------------------------------------
        # 2) Enriquecer con país y residencia
        # ------------------------------------
        self.log(
            "Uniendo wash_coverage con dim_country y dim_residence_type usando Broadcast..."
        )

        wash_enriched = (
            wash_with_year
            .join(B_country_df, on="country_key", how="left")
            .join(B_res_type_df, on="residence_type_key", how="left")
        )

        # -------------------------------------------------
        # 3) Filtrar a 'más de 30 minutos' (limited service)
        # -------------------------------------------------
        self.log(
            "Filtrando registros de 'drinking water' + 'limited service' (>30 min) "
            "y zonas urbana / rural..."
        )

        wash_limited = (
            wash_enriched
            .filter(F.col("service_type_key") == self.DRINKING_WATER_KEY)
            .filter(F.col("service_level_key") == self.LIMITED_SERVICE_KEY)
            .filter(F.col("residence_type_key").isin(self.RESIDENCE_TYPE_KEYS))
        )

        # --------------------------------------------------------
        # 4) Agregar por país + residencia + año: % población >30
        # --------------------------------------------------------
        self.log(
            "Agregando porcentaje de población cuya fuente principal está a más de 30 minutos..."
        )

        pct_yearly = (
            wash_limited
            .groupBy(
                "country_key",
                "country_name",
                "residence_type_key",
                "residence_type_desc",
                "year",
            )
            .agg(F.max("coverage_pct").alias("pct_over_30min"))
        )

        # -------------------------------------------------
        # 5) Calcular evolución anual (delta en p.p.) + tendencia
        # -------------------------------------------------
        self.log(
            "Calculando evolución anual (delta_pct_over_30min_pp) y tendencia..."
        )

        w = Window.partitionBy(
            "country_key",
            "country_name",
            "residence_type_key",
            "residence_type_desc",
        ).orderBy("year")

        pct_with_delta = (
            pct_yearly
            .withColumn("prev_pct", F.lag("pct_over_30min").over(w))
            .withColumn(
                "delta_pct_over_30min_pp",
                F.col("pct_over_30min") - F.col("prev_pct"),
            )
            .withColumn(
                "mobility_trend",
                F.when(
                    F.col("delta_pct_over_30min_pp").isNull(),
                    F.lit("no_previous_year"),
                )
                .when(
                    F.col("delta_pct_over_30min_pp") > self.UMBRAL_TENDENCIA_PP,
                    F.lit("worsened"),
                )
                .when(
                    F.col("delta_pct_over_30min_pp") < -self.UMBRAL_TENDENCIA_PP,
                    F.lit("improved"),
                )
                .otherwise(F.lit("stable")),
            )
        )

        # -------------------------------------------------
        # 6) Semáforo por nivel de % población >30 min
        # -------------------------------------------------
        self.log("Asignando semáforo según % de población >30 minutos...")

        pct_with_sem = pct_with_delta.withColumn(
            "risk_level",
            F.when(F.col("pct_over_30min").isNull(), F.lit("gray"))
            .when(F.col("pct_over_30min") <= 5, F.lit("green"))
            .when(F.col("pct_over_30min") <= 20, F.lit("yellow"))
            .otherwise(F.lit("red")),
        )

        # -------------------------------------------------
        # 7) Filtrar filas sin delta (primer año de cada grupo)
        # -------------------------------------------------
        pct_filtered = pct_with_sem.filter(
            F.col("delta_pct_over_30min_pp").isNotNull()
        )

        # -------------------------------------------------
        # 8) Seleccionar columnas finales 
        # -------------------------------------------------
        final_df = pct_filtered.select(
            "country_key",
            "country_name",
            "residence_type_key",
            "residence_type_desc",
            "year",
            "pct_over_30min",
            "delta_pct_over_30min_pp",
            "mobility_trend",
            "risk_level",
        )

        return final_df


def run(
    spark,
    silver_model_base_path: str,
    gold_model_base_path: str,
) -> None:
    """
    Punto de entrada del KPI 02 de movilidad por agua.
    """
    job = GoldKPI02WaterMobility(
        spark=spark,
        silver_model_base_path=silver_model_base_path,
        gold_model_base_path=gold_model_base_path,
    )
    job.run()
