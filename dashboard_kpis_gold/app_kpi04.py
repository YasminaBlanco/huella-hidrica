import streamlit as st
import pandas as pd
import pydeck as pdk
import altair as alt

from common import COUNTRY_COORDS, S3_PATH_KPI04, risk_color_rgb 

# ======================================================================
# 1. LÓGICA DE DATOS
# ======================================================================

@st.cache_data
def load_kpi04() -> pd.DataFrame:
    """Lee el modelo Gold de kpi04 desde S3 (formato Parquet)."""

    try:
        df = pd.read_parquet(S3_PATH_KPI04)
    except Exception as e:
        st.error(f"Error al cargar datos desde S3: {e}")
        return pd.DataFrame()
    return df


def determine_kpi04_risk(index_value: float):
    """Define el nivel de riesgo sanitario (semáforo) para el KPI 4 (0 a 100)."""
    if index_value <= 20.0:
        return "Verde (Riesgo Bajo)" 
    elif index_value <= 40.0:
        return "Amarillo (Riesgo Moderado)" 
    else:
        return "Rojo (Riesgo Alto)" 


def prepare_base_df_kpi04() -> pd.DataFrame:
    """Carga y enriquece el dataframe base para el KPI 4 (Riesgo Sanitario)."""
    df = load_kpi04().copy() 
    if df.empty:
        return df

    # Aseguramos que la columna del índice esté entre 0 y 100 
    df["health_risk_index"] = df["health_risk_index"].clip(lower=0, upper=100).astype(float) 

    try:
        df = df[df["country_name"].isin(COUNTRY_COORDS.keys())].copy()

        df["lat"] = df["country_name"].map(lambda c: COUNTRY_COORDS[c][0])
        df["lon"] = df["country_name"].map(lambda c: COUNTRY_COORDS[c][1])

        # 2. Aplicar lógica del semáforo
        df["risk_level"] = df["health_risk_index"].apply(determine_kpi04_risk)
        
        # 3. Preparar colores 
        df["color"] = df["risk_level"].apply(risk_color_rgb) 
        
        # 4. Etiqueta 
        df["country_zone"] = df["country_name"] 
    except NameError:
        st.warning("Faltan constantes de 'common.py'. La preparación de datos será incompleta.")
        df["risk_level"] = df["health_risk_index"].apply(determine_kpi04_risk)
        df["country_zone"] = df["country_name"]

    return df

# ======================================================================
# 2. GRÁFICOS
# ======================================================================

def plot_kpi04_ranking(df_rank: pd.DataFrame, year: int):
    """Genera el gráfico de ranking (barras horizontales) para el KPI 4."""

    risk_domain = ["Rojo (Riesgo Alto)", "Amarillo (Riesgo Moderado)", "Verde (Riesgo Bajo)"]
    risk_colors = ["#E53935", "#F2C94C", "#27AE60"]  # rojo, amarillo, verde

    chart = (
        alt.Chart(df_rank)
        .mark_bar()
        .encode(
            y=alt.Y(
                "country_name:N",
                sort=alt.EncodingSortField(
                    field="health_risk_index",
                    op="max",
                    order="descending",
                ),
                title="País",
                axis=alt.Axis(labelLimit=0),
            ),
            x=alt.X(
                "health_risk_index:Q",
                title="Índice de Riesgo Sanitario (0 a 100)",
                axis=alt.Axis(format=".1f"),
            ),
            color=alt.Color(
                "risk_level:N",
                scale=alt.Scale(domain=risk_domain, range=risk_colors),
                legend=alt.Legend(title="Semáforo de Riesgo"),
            ),
            tooltip=[
                alt.Tooltip("country_name:N", title="País"),
                alt.Tooltip("health_risk_index:Q", title="Índice de Riesgo", format=".3f"),
                alt.Tooltip("risk_level:N", title="Nivel de Riesgo"),
            ],
        )
        .properties(height=alt.Step(26))
    )

    st.altair_chart(chart, use_container_width=True)


def plot_kpi04_time_series(df_trend: pd.DataFrame, country_sel: str):
    """Genera la gráfica de serie temporal para el riesgo sanitario de un país."""
    if df_trend.empty:
        st.info(f"No hay datos históricos disponibles para {country_sel}.")
        return

    line_chart = (
        alt.Chart(df_trend)
        .mark_line(point=True)
        .encode(
            x=alt.X("year:O", title="Año"),
            y=alt.Y("health_risk_index:Q", title="Índice de Riesgo Sanitario (0 a 100)"),
            color=alt.value("#1f77b4"),
            tooltip=[
                alt.Tooltip("year:O", title="Año"),
                alt.Tooltip("health_risk_index:Q", title="Índice", format=".3f"),
                alt.Tooltip("risk_level:N", title="Nivel de riesgo"),
            ],
        )
        .properties(height=300, title=f"Evolución del Riesgo Sanitario en {country_sel}")
    )

    st.altair_chart(line_chart, use_container_width=True)

# ======================================================================
# 3. NARRATIVA
# ======================================================================

def display_kpi04_ranking_analysis(df: pd.DataFrame):
    """Análisis narrativo integrado con el ranking (Lectura rápida)."""
    
    st.markdown("## 📖 Análisis Rápido del Riesgo Sanitario")
    st.markdown("---")

    if df.empty:
        st.warning("No hay datos suficientes para generar un análisis para el año seleccionado.")
        return

    worst_case = df.sort_values(by='health_risk_index', ascending=False).iloc[0]
    
    # Recuadro 
    st.markdown(
        """
        <div style="
            border-radius: 999px;
            padding: 0.55rem 1.1rem;
            margin: 0.3rem 0 1rem;
            background: linear-gradient(90deg, rgba(15,76,117,0.85), rgba(13,110,179,0.95));
            color: #F9FAFB;
            font-size: 0.95rem;
        ">
        Se mide un <b>Índice de Riesgo Sanitario Ponderado</b> (0 a 100).
        <b>Mientras más alto, mayor riesgo.</b>
        </div>
        """,
        unsafe_allow_html=True,
    )
    
    st.markdown(f"""
    * **Prioridad Crítica ({worst_case['year']}):** El país con el índice de riesgo sanitario ponderado 
      más alto es **{worst_case['country_name']}**, con un valor de **{worst_case['health_risk_index']:.2f}**. 
      Su riesgo está clasificado como **{worst_case['risk_level']}**.
    * **Implicación:** Un índice de riesgo sanitario ponderado alto indica que la población enfrenta 
      una combinación compleja de baja cobertura de saneamiento, alta mortalidad infantil y pobreza, 
      requiriendo acción inmediata.
    """)
    st.markdown("---")


def display_kpi04_country_trend_analysis(df_trend: pd.DataFrame, country_sel: str):
    """Análisis narrativo detallado de la evolución del riesgo sanitario para un país."""
    if df_trend.empty:
        st.warning(f"No hay datos de series temporales disponibles para {country_sel}.")
        return

    last_row = df_trend.iloc[-1]
    
    st.markdown(f"### Estado Actual ({last_row['year']})")
    st.markdown(
        f"""
        En **{last_row['year']}**, el Índice de Riesgo Sanitario para **{country_sel}** 
        es de **{last_row['health_risk_index']:.2f}**, situándolo en la categoría de 
        **{last_row['risk_level']}**.
        """
    )
    
    # Analizar la tendencia 
    if df_trend.shape[0] >= 2:
        first_row = df_trend.iloc[0]
        initial_risk = first_row['health_risk_index']
        final_risk = last_row['health_risk_index']
        delta = final_risk - initial_risk
        
        if delta < -1.0: 
            trend_text = f"ha **mejorado significativamente** ({delta:.2f} puntos)."
            recommendation = "Se debe continuar la inversión en WASH y programas sociales."
        elif delta > 1.0:
            trend_text = f"ha **empeorado notablemente** ({delta:.2f} puntos)."
            recommendation = "Es urgente revisar los factores que causaron el aumento del riesgo."
        else:
            trend_text = f"se ha mantenido **relativamente estable**."
            recommendation = "Mantener la vigilancia y planificar mejoras específicas."

        st.markdown(
            f"""
            ### Evolución Histórica ({first_row['year']} – {last_row['year']})
            Desde **{first_row['year']}** (Índice: {initial_risk:.2f}), el riesgo sanitario {trend_text}.
            
            **Recomendación:** {recommendation}
            """
        )
    else:
        st.caption("Se necesita más de un año de datos para analizar la tendencia histórica del riesgo.")

# ======================================================================
# 4. LAYOUT Y FUNCIÓN PRINCIPAL
# ======================================================================

def layout_kpi04():
    # ------------------ Estilos y HERO ------------------
    st.markdown(
        """
        <style>
        .stApp {
            --primary-color: #4DA3FF;
        }

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

        .kpi-tag.green {
            background-color: rgba(39, 174, 96, 0.25);
        }

        .kpi-tag.blue {
            background-color: rgba(52, 152, 219, 0.25);
        }

        .kpi-tag.purple {
            background-color: rgba(155, 89, 182, 0.25);
        }

        .kpi-hero-small {
            font-size: 0.9rem;
            opacity: 0.95;
        }

        .kpi-legend-item {
            margin-bottom: 0.25rem;
        }

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

        .kpi-metric {
            margin-bottom: 0.35rem;
        }

        .kpi-metric-label {
            font-size: 0.8rem;
            opacity: 0.9;
        }

        .kpi-metric-value {
            font-size: 1.9rem;
            font-weight: 700;
            line-height: 1.1;
        }

        .kpi-metric-value.accent {
            color: #FACC15;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown(
        """
        <div class="kpi-hero">
          <div class="kpi-hero-left">
            <div class="kpi-hero-title">
              ¿En qué países la salud se ve más afectada por la falta de agua y la pobreza?
            </div>
            <div class="kpi-hero-subtitle">
              Usamos un índice de <b>0 a 100</b> que resume qué tan frágil es la salud en cada país,
              combinando tres cosas: <b>saneamiento básico</b>, <b>salud de niñas y niños</b> 
              y <b>pobreza extrema</b>.
            </div>
            <div class="kpi-hero-tags">
              <span class="kpi-tag green">Salud</span>
              <span class="kpi-tag blue">Saneamiento</span>
              <span class="kpi-tag purple">Pobreza</span>
            </div>
            <div class="kpi-hero-small">
              Mientras más alto es el índice, más vulnerable está la población
              frente a enfermedades relacionadas con el agua y la pobreza.
              Abajo verás el <b>semáforo</b>, el <b>ranking</b> y cómo ha cambiado con el tiempo.
            </div>
          </div>

          <div class="kpi-hero-right">
            <div style="font-weight: 650; margin-bottom: 0.4rem;">
              Semáforo de riesgo sanitario
            </div>
            <div class="kpi-legend-item">🟢 <b>Verde</b>: riesgo bajo.</div>
            <div class="kpi-legend-item">🟡 <b>Amarillo</b>: riesgo moderado.</div>
            <div class="kpi-legend-item">🔴 <b>Rojo</b>: riesgo alto y crítico.</div>
            <div style="margin-top: 0.5rem;" class="kpi-hero-small">
              Los países en rojo son candidatos naturales para <b>priorizar inversión</b>
              en agua, saneamiento y salud.
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ------------------ Datos y filtro de año ------------------
    df = prepare_base_df_kpi04()
    if df.empty:
        st.error("No se pudieron cargar o preparar los datos base para el KPI 4.")
        return

    years = sorted(df["year"].unique())
    if not years:
        st.error("El DataFrame no contiene la columna 'year' o está vacío.")
        return

    current_year = st.slider(
        "Año para el semáforo y el ranking",
        min_value=min(years),
        max_value=max(years),
        value=max(years),
        step=1,
    )

    df_year = df[df["year"] == current_year].copy()
    if df_year.empty:
        st.warning("No hay datos para el año seleccionado.")
        return

    # =========================
    # 1) ¿Qué mide? 
    # =========================
    left, right = st.columns([2.1, 1.2])

    with left:
        st.markdown(
            """
            ### 1. 👀 ¿Qué mide este indicador?

            - Resume en un solo número el **riesgo sanitario** de cada país
              combinando tres factores:  
              **saneamiento básico**, **mortalidad infantil** y **pobreza extrema**.
            - El índice va de **0 a 100**: mientras más alto, mayor riesgo para la salud
              de la población.

            El año que elijas arriba se aplica al **ranking** y al **análisis rápido**.
            La serie de tiempo usa todos los años disponibles para cada país.
            """
        )

        with st.expander("Ver definición técnica del indicador"):
            st.markdown(
                """
*Indicador técnico:*  
Índice compuesto de **Riesgo Sanitario Ponderado** (0–100) para cada país y año,
que combina información de acceso a saneamiento, mortalidad infantil y pobreza extrema.

*Cálculo básico:*  

- Se toman tres componentes de riesgo ya estandarizados:  
  - Riesgo asociado a **baja cobertura de saneamiento básico**.  
  - Riesgo asociado a **mortalidad infantil**.  
  - Riesgo asociado a **pobreza extrema**.  
- Cada componente se normaliza en una escala común (0–100).  
- Se combinan mediante una suma ponderada para obtener un único **índice de riesgo sanitario** entre 0 y 100 por país y año.

*Tendencia de riesgo:*  

- Comparando el índice entre años se puede ver si el riesgo **mejora, empeora o se mantiene**.  
  En los análisis se reporta el cambio en puntos del índice para cada país.

*Semáforo de riesgo sanitario (por país):*  

- 🟢 **Verde (≤ 20)**: riesgo bajo.  
- 🟡 **Amarillo (>20–40)**: riesgo moderado.  
- 🔴 **Rojo (>40)**: riesgo alto y crítico; prioridad de inversión.
                """
            )

    with right:
        total_paises = df_year["country_name"].nunique()
        n_rojo = (df_year["risk_level"] == "Rojo (Riesgo Alto)").sum()
        n_amarillo = (df_year["risk_level"] == "Amarillo (Riesgo Moderado)").sum()

        st.markdown(
            f"""
            <div class="kpi-metrics-card">
              <div class="kpi-metric">
                <div class="kpi-metric-label">Países analizados en {current_year}</div>
                <div class="kpi-metric-value accent">{total_paises}</div>
              </div>
              <div class="kpi-metric">
                <div class="kpi-metric-label">En rojo (riesgo alto)</div>
                <div class="kpi-metric-value accent">{n_rojo}</div>
              </div>
              <div class="kpi-metric">
                <div class="kpi-metric-label">En amarillo (riesgo moderado)</div>
                <div class="kpi-metric-value accent">{n_amarillo}</div>
              </div>
              <div style="font-size: 0.78rem; opacity: 0.9; margin-top: 0.25rem;">
                Cada valor refleja el número de <b>países</b> según el semáforo de riesgo sanitario.
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("---")

    # =========================
    # 2) Ranking por país
    # =========================
    st.subheader(f"2. 📊 Ranking de países donde la salud está más en riesgo por falta de agua y pobreza en {current_year}")

    df_rank = df_year.sort_values("health_risk_index", ascending=False).copy()
    plot_kpi04_ranking(df_rank, current_year)
    display_kpi04_ranking_analysis(df_year)

    # =========================
    # 3) Evolución temporal
    # =========================
    st.subheader("3. ⏱️ Evolución del Riesgo Sanitario (Análisis por País)")

    country_sel = st.selectbox(
        "Selecciona un País para ver su serie temporal",
        options=sorted(df["country_name"].unique()),
    )

    # Filtro para la serie temporal
    df_trend = df[(df["country_name"] == country_sel)].sort_values("year").copy()

    plot_kpi04_time_series(df_trend, country_sel)
    
    # ANÁLISIS ESPECÍFICO DEL PAÍS
    display_kpi04_country_trend_analysis(df_trend, country_sel)
