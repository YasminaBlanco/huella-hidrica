from base_gold_model_job import BaseGoldKPIJob
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Indicadores de WDI (ejemplo, verificar códigos correctos en dim_indicator)
WDI_INDICATORS = {
    "population": "SP.POP.TOTL",
    "child_mortality": "SH.DYN.MORT",
    "poverty": "SI.POV.DDAY",
}

# --- CONFIGURACIÓN DEL ÍNDICE PONDERADO ---
# Pesos para cada componente del riesgo
WEIGHTS = {
    "sanitation_risk_pct": 0.50,  # El saneamiento fallido es el factor de riesgo principal
    "child_mortality_rate": 0.30,
    "poverty_rate": 0.20,
}

# Límites de escalamiento para normalización (ajustar para un rango lógico de datos)
# Los valores se escalarán entre [0, 1] usando (Valor - Min) / (Max - Min)
SCALING_LIMITS = {
    # Máxima falta de cobertura que puede ocurrir (100 - 0 = 100)
    "sanitation_risk_pct": {"min": 0.0, "max": 100.0},
    # Tasa de mortalidad infantil máxima esperada (ej. 150 por mil)
    "child_mortality_rate": {"min": 0.0, "max": 150.0},
    # Tasa de pobreza máxima esperada (100%)
    "poverty_rate": {"min": 0.0, "max": 100.0},
}

# Umbrales para el semáforo del Índice (0-100)
INDEX_UMBRALES = {
    "verde_max_index": 20.0,  # Índice < 20
    "amarillo_max_index": 40.0,  # Índice < 40
}


class GoldKPI04WeightedIndex(BaseGoldKPIJob):
    """
    KPI 4 Alternativo: Índice de Riesgo Sanitario Ponderado.
    Calcula un score compuesto basado en Saneamiento, Mortalidad Infantil y Pobreza.
    """

    def kpi_name(self) -> str:
        return "kpi04_weighted_health_risk_index"

    def output_path(self) -> str:
        return f"{self.gold_base}/{self.kpi_name()}"

    def _get_sanitation_coverage(self) -> DataFrame:
        """
        Calcula el % de saneamiento básico/seguro (total país/año)
        y lo transforma en una métrica de RIESGO (100 - Cobertura).
        """
        self.log(
            "Calculando % saneamiento básico/seguro total país/año y transformando a riesgo..."
        )

        wash_df = self.read_silver_table("wash_coverage")

        # FILTRO: Filtros para Saneamiento Básico/Seguro (asumiendo 0='sanitation', [1, 2, 4]='safe levels')
        sanitation_safe_df = wash_df.filter(
            (F.col("service_type_key") == 0)
            & (F.col("service_level_key").isin([1, 2, 4]))
        )

        sanitation_safe_df = sanitation_safe_df.withColumn(
            "year", F.substring(F.col("date_key").cast("string"), 1, 4).cast("int")
        )

        sanitation_pct_df = sanitation_safe_df.groupBy("country_key", "year").agg(
            F.sum("coverage_pct").alias("sanitation_coverage_pct")
        )

        # TRANSFORMACIÓN CLAVE: RIESGO DE SANEAMIENTO = 100 - Cobertura
        sanitation_risk_df = sanitation_pct_df.withColumn(
            "sanitation_risk_pct", F.lit(100.0) - F.col("sanitation_coverage_pct")
        )

        return sanitation_risk_df.select(
            "country_key", "year", "sanitation_coverage_pct", "sanitation_risk_pct"
        )

    def _get_socioeconomic_metrics(self) -> DataFrame:
        """
        Extrae y pivota las métricas socioeconómicas (Población, Mortalidad, Pobreza).
        """
        self.log("Extrayendo y pivotando métricas socioeconómicas...")

        socio_df = self.read_silver_table("socioeconomic").alias("socio")
        dim_indicator = self.read_silver_table("indicator").select(
            "indicator_key", "indicator_code"
        )

        joined_df = socio_df.join(dim_indicator, on="indicator_key", how="inner")

        filtered_df = (
            joined_df.filter(
                F.col("indicator_code").isin(list(WDI_INDICATORS.values()))
            )
            .withColumns(
                {
                    "year": F.substring(F.col("date_key").cast("string"), 1, 4).cast(
                        "int"
                    ),
                    "value": F.col("indicator_value"),
                }
            )
            .select("country_key", "year", "indicator_code", "value")
        )

        pivot_df = (
            filtered_df.groupBy("country_key", "year")
            .pivot("indicator_code", list(WDI_INDICATORS.values()))
            .agg(F.first("value"))
        )

        pivot_df = pivot_df.withColumnsRenamed(
            {
                WDI_INDICATORS["child_mortality"]: "child_mortality_rate",
                WDI_INDICATORS["poverty"]: "poverty_rate",
                WDI_INDICATORS["population"]: "population_total",
            }
        )

        # Asegura tipos de datos
        pivot_df = pivot_df.withColumns(
            {
                "child_mortality_rate": F.col("child_mortality_rate").cast(
                    "decimal(8,3)"
                ),
                "poverty_rate": F.col("poverty_rate").cast("decimal(5,2)"),
                "population_total": F.col("population_total").cast("long"),
            }
        )

        return pivot_df

    def _normalize_and_weight(self, df: DataFrame) -> DataFrame:
        """
        Normaliza (escala) cada métrica de riesgo y aplica la ponderación.

        CORRECCIÓN: Se reestructura la lógica de normalización para evitar el error
        PySparkTypeError, asegurando que solo se pasen Columnas PySpark a F.when().
        """
        self.log("Normalizando y ponderando métricas para crear el Índice de Riesgo...")

        risk_df = df

        # 1. Normalización Min-Max para cada métrica clave
        for col_name, limits in SCALING_LIMITS.items():
            min_val = limits["min"]
            max_val = limits["max"]
            range_val = max_val - min_val

            # Chequeamos range_val fuera de la expresión Spark, ya que es una constante Python
            if range_val > 0:
                # Si el rango es válido, definimos la expresión de normalización: (Valor - Min) / Rango
                normalized_value = (F.col(col_name) - F.lit(min_val)) / F.lit(range_val)

                # Cadena F.when para aplicar capping y manejar NULLs
                normalization_expr = (
                    F.when(
                        F.col(col_name).isNull(),
                        F.lit(None),  # Mantener NULL si falta dato
                    )
                    .when(
                        F.col(col_name) >= F.lit(max_val),
                        F.lit(1.0),  # Cap a 1.0 si >= Max
                    )
                    .when(
                        F.col(col_name) <= F.lit(min_val),
                        F.lit(0.0),  # Cap a 0.0 si <= Min
                    )
                    .otherwise(
                        normalized_value  # Aplicar normalización si está entre Min y Max
                    )
                )
            else:
                # Caso de rango cero (Min == Max). Se asume riesgo máximo (1.0) si el valor es not null.
                normalization_expr = (
                    F.when(F.col(col_name).isNull(), F.lit(None))
                    .when(F.col(col_name).isNotNull(), F.lit(1.0))
                    .otherwise(F.lit(None))
                )

            risk_df = risk_df.withColumn(f"{col_name}_norm", normalization_expr)

        # 2. Aplicar Ponderación y Suma para el Índice
        weighted_terms = []
        for col_name, weight in WEIGHTS.items():
            # (Valor Normalizado * Peso)
            # Utilizamos coalesce para que si el valor normalizado es NULL, el término ponderado sea 0.0,
            # permitiendo el cálculo del índice con los otros factores.
            weighted_term = F.coalesce(F.col(f"{col_name}_norm"), F.lit(0.0)) * F.lit(
                weight
            )
            weighted_terms.append(weighted_term)

        # El índice final es la suma (row-wise) de los términos ponderados, escalado de 0 a 100
        # CORRECCIÓN: Usar la función 'sum' de Python para sumar las Columnas PySpark.
        risk_df = risk_df.withColumn(
            "health_risk_index",
            sum(weighted_terms).cast("decimal(6,3)") * F.lit(100),  # Escala a 100
        )

        return risk_df

    def build(self) -> DataFrame:
        # 1. Obtener la cobertura de saneamiento y transformarla a riesgo
        sanitation_df = self._get_sanitation_coverage()

        # 2. Obtener las métricas socioeconómicas
        socio_df = self._get_socioeconomic_metrics()

        # 3. Unir las métricas (JOIN por country_key y year)
        joined_df = sanitation_df.join(
            socio_df, on=["country_key", "year"], how="inner"
        )  # NO hacemos dropna aquí, queremos ver qué países tienen datos parciales.

        # 4. Calcular, normalizar y ponderar el Índice de Riesgo
        indexed_df = self._normalize_and_weight(joined_df)

        # 5. Unir con dim_country
        dim_country = self.read_silver_table("country").select(
            "country_key", "country_name"
        )
        final_df = indexed_df.join(dim_country, on="country_key", how="inner")

        # 6. Aplicar Semáforo (Basado en el Índice de 0 a 100)
        final_df = final_df.withColumn(
            "risk_level",
            F.when(F.col("health_risk_index").isNull(), "Datos Faltantes")
            .when(
                F.col("health_risk_index") > INDEX_UMBRALES["amarillo_max_index"],
                "Rojo",
            )
            .when(
                F.col("health_risk_index") > INDEX_UMBRALES["verde_max_index"],
                "Amarillo",
            )
            .otherwise("Verde"),
        )

        # 7. Seleccionar y ordenar columnas
        return final_df.select(
            F.col("country_key").cast("int"),
            F.col("country_name").cast("string"),
            F.col("year").cast("int"),
            F.col("sanitation_coverage_pct").cast("decimal(5,2)"),
            F.col("child_mortality_rate").cast("decimal(8,3)"),
            F.col("poverty_rate").cast("decimal(5,2)"),
            F.col("population_total").cast("bigint"),
            F.col("health_risk_index").cast("decimal(6,3)").alias("health_risk_index"),
            F.col("risk_level").cast("string"),
        )


def run(
    spark: SparkSession,
    silver_model_base_path: str,
    gold_model_base_path: str,
    write_mode: str = "overwrite",
) -> None:
    """Función de conveniencia para ejecutar el job desde el orquestador."""
    job = GoldKPI04WeightedIndex(
        spark=spark,
        silver_model_base_path=silver_model_base_path,
        gold_model_base_path=gold_model_base_path,
        write_mode=write_mode,
    )
    job.run()
