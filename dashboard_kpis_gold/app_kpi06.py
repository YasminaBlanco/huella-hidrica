import streamlit as st
import pandas as pd
import altair as alt
import numpy as np
from common import S3_PATH_KPI06

# ======================================================================
# 1. LÓGICA DE DATOS
# ======================================================================

@st.cache_data
def load_kpi06_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Carga el modelo Gold de KPI06 desde S3 y prepara:
    - df_final: resumen por región (+ fila 'Regional')
    - df_raw: serie anual original (para la evolución en el tiempo)
    """
    try:
        df = pd.read_parquet(S3_PATH_KPI06)
    except Exception as e:
        st.error(f"Error al cargar datos de KPI 6 desde S3: {e}")
        return pd.DataFrame(), pd.DataFrame()

    df = df.copy()

    # Aseguramos el nombre de la columna de correlación
    if "correlation_coefficient" not in df.columns and "corr_water_vs_gdp" in df.columns:
        df = df.rename(columns={"corr_water_vs_gdp": "correlation_coefficient"})

    if "correlation_coefficient" not in df.columns:
        st.error("El dataset de KPI 6 no contiene la columna 'correlation_coefficient'.")
        return pd.DataFrame(), pd.DataFrame()

    df["correlation_coefficient"] = df["correlation_coefficient"].astype(float)

    # Función auxiliar para semáforo
    def get_risk_level(abs_r: float) -> str:
        if abs_r >= 0.6:
            return "Rojo"
        elif abs_r >= 0.3:
            return "Amarillo"
        else:
            return "Verde"

    # Marcamos riesgo por año (df_raw)
    df["risk_level"] = df["correlation_coefficient"].abs().apply(get_risk_level)

    # ---- Agregado por región (promedio de correlación en el periodo) ----
    if "region_name" not in df.columns:
        st.error("El dataset de KPI 6 no contiene la columna 'region_name'.")
        return pd.DataFrame(), pd.DataFrame()

    agg = (
        df.groupby("region_name")
        .agg(
            n_countries=("n_countries", "max"),
            correlation_coefficient=("correlation_coefficient", "mean"),  # promedio
            avg_safe_water_pct=("avg_safe_water_pct", "mean"),
            avg_gdp_per_capita=("avg_gdp_per_capita", "mean"),
        )
        .reset_index()
    )

    agg["corr_abs_value"] = agg["correlation_coefficient"].abs()
    agg["risk_level"] = agg["corr_abs_value"].apply(get_risk_level)

    # ---- Fila 'Regional' (promedio global) ----
    global_corr_mean = df["correlation_coefficient"].mean()

    global_row = {
        "region_name": "Regional",
        "n_countries": int(df["n_countries"].max()) if "n_countries" in df.columns else df["region_name"].nunique(),
        "correlation_coefficient": float(global_corr_mean),
        "avg_safe_water_pct": float(df["avg_safe_water_pct"].mean()),
        "avg_gdp_per_capita": float(df["avg_gdp_per_capita"].mean()),
        "corr_abs_value": float(abs(global_corr_mean)),
        "risk_level": get_risk_level(abs(global_corr_mean)),
    }

    df_final = pd.concat([agg, pd.DataFrame([global_row])], ignore_index=True)

    # Tipos numéricos consistentes
    for col in ["correlation_coefficient", "corr_abs_value", "avg_safe_water_pct", "avg_gdp_per_capita"]:
        df_final[col] = df_final[col].astype(float)

    return df_final, df  # df_raw = df


# ======================================================================
# 2. ANÁLISIS NARRATIVO (SERIE DE TIEMPO)
# ======================================================================

def explain_timeseries_analysis_06(df_raw: pd.DataFrame) -> None:
    """Explica en lenguaje sencillo la evolución anual de la correlación."""
    st.markdown("---")
    st.subheader("¿Cómo leer la evolución anual de la relación Agua–PIB?")

    if df_raw.empty:
        st.warning("No hay datos anuales disponibles para el análisis de tendencia.")
        return

    df_trend = df_raw.sort_values("year")

    # Métricas simples de tendencia
    corr_initial = df_trend.iloc[0]["correlation_coefficient"]
    corr_final = df_trend.iloc[-1]["correlation_coefficient"]
    abs_corr_initial = abs(corr_initial)
    abs_corr_final = abs(corr_final)
    delta_abs = abs_corr_final - abs_corr_initial
    avg_sign = np.sign(df_trend["correlation_coefficient"].mean())

    direction_text = (
        "positiva (más PIB, más agua segura)" if avg_sign > 0
        else "negativa (más PIB, menos agua segura)"
    )

    if delta_abs > 0.1:
        strength_text = (
            f"la relación se ha vuelto **más fuerte** (|r| pasó de {abs_corr_initial:.2f} a {abs_corr_final:.2f})."
        )
        impact_text = "El acceso al agua depende cada vez más de la riqueza."
    elif delta_abs < -0.1:
        strength_text = (
            f"la relación se ha vuelto **más débil** (|r| pasó de {abs_corr_initial:.2f} a {abs_corr_final:.2f})."
        )
        impact_text = "La riqueza pesa menos para explicar quién tiene agua segura."
    else:
        strength_text = "la fuerza de la relación se ha mantenido **bastante estable**."
        impact_text = "El peso de la riqueza sobre el acceso al agua casi no ha cambiado."

    st.markdown(
        f"""
- Las **barras hacia arriba** muestran años en que los países con más PIB tienden a tener **más acceso a agua segura**.  
- Las **barras hacia abajo** indican años donde ocurre lo contrario.  
- El **color** refleja qué tan fuerte es la relación (verde débil, amarillo moderada, rojo fuerte).

Entre **{int(df_trend['year'].min())}** y **{int(df_trend['year'].max())}**:
- La relación ha sido en promedio **{direction_text}**.  
- En términos de fuerza, {strength_text}  
- En resumen: **{impact_text}**
"""
    )


# ======================================================================
# 3. GRÁFICOS
# ======================================================================

def plot_kpi06_chart(df_final: pd.DataFrame) -> None:
    """Gráfico principal: fuerza de la correlación promedio por región."""
    df_chart = df_final[df_final["region_name"] != "Regional"].sort_values(
        "corr_abs_value", ascending=False
    )

    base = alt.Chart(df_chart).encode(
        x=alt.X(
            "corr_abs_value:Q",
            title="Fuerza de la relación (|r|)",
            axis=alt.Axis(format=".2f"),
        ),
        y=alt.Y("region_name:N", sort="-x", title="Región"),
        color=alt.Color(
            "risk_level:N",
            scale=alt.Scale(
                domain=["Rojo", "Amarillo", "Verde"],
                range=["#E53935", "#F2C94C", "#27AE60"],
            ),
            legend=alt.Legend(title="Semáforo de riesgo"),
        ),
        tooltip=[
            alt.Tooltip("region_name", title="Región"),
            alt.Tooltip("correlation_coefficient", title="r promedio", format=".3f"),
            alt.Tooltip("n_countries", title="Países considerados"),
            alt.Tooltip("avg_safe_water_pct", title="Agua segura promedio (%)", format=".1f"),
            alt.Tooltip("avg_gdp_per_capita", title="PIB per cápita promedio", format="$,.0f"),
        ],
    ).properties(height=alt.Step(26))

    chart = base.mark_bar()
    text = base.mark_text(
        align="left",
        baseline="middle",
        dx=3,
        color="white",
    ).encode(text=alt.Text("correlation_coefficient", format=".2f"))

    st.altair_chart(chart + text, use_container_width=True)


def plot_kpi06_timeseries(df_raw: pd.DataFrame) -> None:
    """Gráfico de series de tiempo del coeficiente de correlación anual."""
    if df_raw.empty:
        st.warning("No hay datos para la serie de tiempo.")
        return

    region_name = (
        df_raw["region_name"].iloc[0] if "region_name" in df_raw.columns else "Regional"
    )

    base = alt.Chart(df_raw).encode(
        x=alt.X("year:O", title="Año"),
        y=alt.Y(
            "correlation_coefficient:Q",
            title="Coeficiente de correlación (r)",
            scale=alt.Scale(domain=[-1, 1]),
            axis=alt.Axis(format=".1f"),
        ),
        color=alt.Color(
            "risk_level:N",
            scale=alt.Scale(
                domain=["Rojo", "Amarillo", "Verde"],
                range=["#E53935", "#F2C94C", "#27AE60"],
            ),
            legend=alt.Legend(title="Fuerza de la relación"),
        ),
        tooltip=[
            alt.Tooltip("year:O", title="Año"),
            alt.Tooltip("correlation_coefficient:Q", title="r", format=".3f"),
            alt.Tooltip("risk_level:N", title="Semáforo"),
        ],
    ).properties(title=f"Evolución anual de la correlación – {region_name}", height=320)

    bars = base.mark_bar()

    zero_line = alt.Chart(pd.DataFrame({"r": [0]})).mark_rule(
        color="gray", strokeDash=[3, 3]
    ).encode(y="r:Q")

    text = base.mark_text(
        align="center",
        dy=-8,
        color="black",
    ).encode(text=alt.Text("correlation_coefficient", format=".2f"))

    st.altair_chart(bars + zero_line + text, use_container_width=True)


# ======================================================================
# 4. LAYOUT PRINCIPAL 
# ======================================================================

def layout_kpi06() -> None:
    """Pantalla principal del KPI 6."""
    df_final, df_raw = load_kpi06_data()

    if df_final.empty:
        st.error("No se pudieron cargar los datos del KPI 6.")
        return

    # ====== Estilos CSS ======
    st.markdown(
        """
        <style>
        .kpi-hero {
            display: flex;
            flex-wrap: wrap;
            gap: 1.5rem;
            padding: 1.6rem 1.8rem;
            border-radius: 1.2rem;
            background: linear-gradient(135deg, #0F4C75, #14597A);
            color: #FFFFFF;
            box-shadow: 0 10px 30px rgba(15, 76, 117, 0.4);
            margin-bottom: 1.4rem;
        }
        .kpi-hero-left {
            flex: 2;
            min-width: 260px;
        }
        .kpi-hero-right {
            flex: 1;
            min-width: 220px;
            border-left: 1px solid rgba(255, 255, 255, 0.4);
            padding-left: 1.2rem;
            font-size: 0.9rem;
        }
        .kpi-hero-title {
            font-size: 2.0rem;
            font-weight: 750;
            margin-bottom: 0.3rem;
        }
        .kpi-hero-subtitle {
            font-size: 1.05rem;
            opacity: 0.98;
            margin-bottom: 0.7rem;
        }
        .kpi-hero-tags {
            display: flex;
            flex-wrap: wrap;
            gap: 0.4rem;
            margin-bottom: 0.6rem;
        }
        .kpi-tag {
            padding: 0.2rem 0.7rem;
            border-radius: 999px;
            font-size: 0.72rem;
            text-transform: uppercase;
            letter-spacing: 0.06em;
            font-weight: 600;
            border: 1px solid rgba(255, 255, 255, 0.65);
            backdrop-filter: blur(6px);
            background-color: rgba(0, 0, 0, 0.10);
        }
        .kpi-tag.green { background-color: rgba(39, 174, 96, 0.25); }
        .kpi-tag.blue  { background-color: rgba(52, 152, 219, 0.25); }
        .kpi-tag.gold  { background-color: rgba(241, 196, 15, 0.25); }
        .kpi-hero-small {
            font-size: 0.9rem;
            opacity: 0.95;
        }
        .kpi-legend-item { margin-bottom: 0.25rem; }
        .kpi-metrics-card {
            background: linear-gradient(135deg, #0F4C75, #14597A);
            border-radius: 1rem;
            padding: 1rem 1.2rem;
            box-shadow: 0 8px 22px rgba(15, 76, 117, 0.45);
            display: flex;
            flex-direction: column;
            gap: 0.35rem;
            color: #F9FAFB;
            border: 1px solid #1FA2FF;
        }
        .kpi-metric { margin-bottom: 0.35rem; }
        .kpi-metric-label { font-size: 0.8rem; opacity: 0.9; }
        .kpi-metric-value {
            font-size: 1.9rem;
            font-weight: 700;
            line-height: 1.1;
        }
        .kpi-metric-value.accent { color: #FACC15; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Métricas globales para el HERO
    df_regions = df_final[df_final["region_name"] != "Regional"]
    n_regions = df_regions["region_name"].nunique()

    regional_row = df_final[df_final["region_name"] == "Regional"]
    if not regional_row.empty:
        global_r = regional_row.iloc[0]["correlation_coefficient"]
        global_risk = regional_row.iloc[0]["risk_level"]
    else:
        global_r = df_final["correlation_coefficient"].mean()
        global_risk = df_final["risk_level"].iloc[0]

    worst_region = df_regions.sort_values("corr_abs_value", ascending=False).iloc[0]

    # ====== HERO en Z ======
    st.markdown(
        f"""
        <div class="kpi-hero">
          <div class="kpi-hero-left">
            <div class="kpi-hero-title">
              ¿La riqueza se traduce en mejor acceso al agua segura?
            </div>
            <div class="kpi-hero-subtitle">
              Miramos qué tan alineados van el <b>PIB per cápita</b> y el <b>acceso a agua segura</b>.
              Si la correlación es fuerte y está en rojo, el acceso al agua depende demasiado del dinero.
            </div>
            <div class="kpi-hero-tags">
              <span class="kpi-tag green">Agua segura</span>
              <span class="kpi-tag blue">Economía</span>
              <span class="kpi-tag gold">Equidad regional</span>
            </div>
            <div class="kpi-hero-small">
              Abajo encontrarás cómo está construido el indicador y cómo leerlo.
            </div>
          </div>

          <div class="kpi-hero-right">
            <div style="font-weight: 650; margin-bottom: 0.4rem;">
              Semáforo de dependencia económica
            </div>
            <div class="kpi-legend-item">
              🟢 <b>Verde</b>: la riqueza apenas explica quién tiene agua segura.
            </div>
            <div class="kpi-legend-item">
              🟡 <b>Amarillo</b>: la riqueza ya pesa de forma importante.
            </div>
            <div class="kpi-legend-item">
              🔴 <b>Rojo</b>: el acceso al agua está fuertemente ligado al dinero.
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # =========================
    # 1) ¿Qué mide este indicador? 
    # =========================
    left, right = st.columns([2.1, 1.2])

    with left:
        st.markdown(
            """
            ### 1. 👀 ¿Qué mide este indicador?

            - Une, para cada región, los datos de **PIB per cápita** y de **acceso promedio a agua segura** de sus países.
            - Mide **qué tan fuerte** es la relación entre estos dos valores.
            - El resultado es un número entre **0 y 1**:
              - cerca de **0**: casi no hay relación entre riqueza y agua;
              - cerca de **1**: la riqueza y el acceso al agua se mueven muy juntos (alta dependencia)..
            """
        )

        with st.expander("Ver definición técnica del indicador"):
            st.markdown(
                """
*Indicador técnico*  
Índice de **dependencia económica del acceso al agua**, medido como el **valor absoluto**
del coeficiente de correlación de Pearson (**|r|**) entre el **PIB per cápita promedio**
y el **porcentaje de población con agua segura**, por región y año.

*Cálculo básico*  

- Para cada **región** y **año** se toma:
  - el **PIB per cápita promedio** de los países con datos;
  - el **porcentaje promedio de población con agua segura**.
- Con esas dos series se calcula el coeficiente de correlación de Pearson **r**.

*Semáforo de dependencia económica (según |r|)*  

- 🟢 **Verde (|r| < 0.3)**: relación débil; la riqueza casi no explica quién tiene agua segura.  
- 🟡 **Amarillo (0.3 ≤ |r| < 0.6)**: relación moderada; la riqueza ya pesa de forma importante.  
- 🔴 **Rojo (|r| ≥ 0.6)**: relación fuerte; el acceso al agua está muy ligado al PIB.
                """
            )

    with right:
        st.markdown(
            f"""
            <div class="kpi-metrics-card">
              <div class="kpi-metric">
                <div class="kpi-metric-label">Regiones analizadas</div>
                <div class="kpi-metric-value accent">{n_regions}</div>
              </div>
              <div class="kpi-metric">
                <div class="kpi-metric-label">Región más dependiente del PIB</div>
                <div class="kpi-metric-value accent">{worst_region['region_name']}</div>
              </div>
              <div style="font-size: 0.78rem; opacity: 0.9; margin-top: 0.25rem;">
                Es la región con la <b>correlación más alta</b> entre riqueza y acceso al agua segura.
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("---")

    # =========================
    # 2) Barras por región
    # =========================
    st.subheader("2. 📊 ¿En qué regiones pesa más la riqueza para tener agua segura?")
    plot_kpi06_chart(df_final)

    st.markdown("---")

    # =========================
    # 3) Serie de tiempo + explicación
    # =========================
    st.subheader("3. ⏱️ Evolución anual de la correlación Agua–PIB")

    plot_kpi06_timeseries(df_raw)
    explain_timeseries_analysis_06(df_raw)
