import streamlit as st
import pandas as pd
import altair as alt
import numpy as np
import pydeck as pdk

from common import S3_PATH_KPI07, COUNTRY_COORDS  


# ======================================================================
# 1. LÓGICA DE DATOS Y GRÁFICOS
# ======================================================================

def risk_hex(risk_level: str) -> str:
    """Colores HEX para Altair (barras)."""
    mapping = {
        "Verde": "#27AE60",
        "Amarillo": "#F2C94C",
        "Rojo": "#E53935",
    }
    return mapping.get(risk_level, "#B4B4B4")


@st.cache_data
def load_kpi07() -> pd.DataFrame:
    """Lee el modelo Gold de kpi07 desde S3 (formato Parquet)."""
    try:
        df = pd.read_parquet(S3_PATH_KPI07)
    except Exception as e:
        st.error(f"Error al cargar datos desde S3: {e}")
        return pd.DataFrame()
    return df


def prepare_base_df() -> pd.DataFrame:
    """
    Carga y prepara el dataframe base para el KPI 7,
    incluyendo coordenadas, color para PyDeck y tendencia.
    """
    df = load_kpi07().copy()
    if df.empty:
        return df

    # Filtramos sólo países con coordenadas
    df = df[df["country_name"].isin(COUNTRY_COORDS.keys())].copy()

    df["gap_water_sanitation_pp"] = df["gap_water_sanitation_pp"].astype(float)
    df["water_basic_safe_pct"] = df["water_basic_safe_pct"].astype(float)
    df["sanitation_basic_safe_pct"] = df["sanitation_basic_safe_pct"].astype(float)

    # Coordenadas
    df["lat"] = df["country_name"].map(lambda c: COUNTRY_COORDS[c][0])
    df["lon"] = df["country_name"].map(lambda c: COUNTRY_COORDS[c][1])

    # Color RGBA fijo para pydeck 
    COLOR_RGBA = {
        "Rojo": [229, 57, 53, 220],
        "Amarillo": [242, 201, 76, 220],
        "Verde": [39, 174, 96, 220],
    }
    df["color"] = df["risk_level"].map(
        lambda lvl: COLOR_RGBA.get(lvl, [180, 180, 180, 200])
    )

    # Cálculo de tendencia basado en la brecha
    df = df.sort_values(["country_name", "year"])
    df["prev_gap"] = df.groupby("country_name")["gap_water_sanitation_pp"].shift(1)
    df["delta_gap"] = df["gap_water_sanitation_pp"] - df["prev_gap"]

    def classify_trend(d):
        if pd.isna(d):
            return "Sin datos previos"
        if d > 0.1:
            return "Empeorando"
        if d < -0.1:
            return "Mejorando"
        return "Estable"

    df["gap_trend"] = df["delta_gap"].apply(classify_trend)
    df["risk_hex"] = df["risk_level"].apply(risk_hex)

    df["lon"] = df["lon"].astype(float)
    df["lat"] = df["lat"].astype(float)

    return df.dropna(subset=["gap_water_sanitation_pp"])


def get_risk_distribution(df_year: pd.DataFrame) -> pd.DataFrame:
    """Calcula cantidad y porcentaje de países por nivel de riesgo."""
    risk_order = ["Rojo", "Amarillo", "Verde"]
    distribution = df_year["risk_level"].value_counts().reset_index()
    distribution.columns = ["risk_level", "count"]
    distribution["percentage"] = (
        distribution["count"] / distribution["count"].sum()
    ) * 100
    distribution["risk_level"] = pd.Categorical(
        distribution["risk_level"], categories=risk_order, ordered=True
    )
    return distribution.sort_values("risk_level").reset_index(drop=True)


def plot_risk_distribution(df_dist: pd.DataFrame, selected_year: int):
    """Gráfico de barras para la distribución de riesgo."""
    risk_colors = {"Rojo": "#E53935", "Amarillo": "#F2C94C", "Verde": "#27AE60"}
    risk_order = ["Rojo", "Amarillo", "Verde"]

    chart = (
        alt.Chart(df_dist)
        .mark_bar()
        .encode(
            x=alt.X("percentage:Q", title="Países (%)", axis=alt.Axis(format=".1f")),
            y=alt.Y("risk_level:N", sort=risk_order, title="Nivel de riesgo"),
            color=alt.Color(
                "risk_level:N",
                scale=alt.Scale(
                    domain=risk_order, range=[risk_colors[r] for r in risk_order]
                ),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("risk_level", title="Riesgo"),
                alt.Tooltip("count", title="Países (n)"),
                alt.Tooltip("percentage", title="Porcentaje", format=".1f"),
            ],
        )
        .properties(title=f"Distribución de países por nivel de riesgo ({selected_year})")
    )

    text = chart.mark_text(align="left", baseline="middle", dx=3).encode(
        text=alt.Text("count:Q"),
        color=alt.value("black"),
    )

    st.altair_chart(chart + text, use_container_width=True)


def plot_kpi07_evolution_regional(df_full: pd.DataFrame):
    """Línea: evolución de la brecha promedio regional."""
    df_mean_gap = df_full.groupby("year")["gap_water_sanitation_pp"].mean().reset_index()
    df_mean_gap.columns = ["year", "avg_gap"]

    chart = (
        alt.Chart(df_mean_gap)
        .mark_line(point=True)
        .encode(
            x=alt.X("year:O", title="Año"),
            y=alt.Y("avg_gap:Q", title="Brecha promedio regional (p.p.)"),
            tooltip=[
                alt.Tooltip("year:O", title="Año"),
                alt.Tooltip("avg_gap:Q", title="Brecha promedio", format=".2f"),
            ],
        )
        .properties(title="Tendencia histórica de la brecha regional")
    )
    st.altair_chart(chart, use_container_width=True)


def plot_kpi07_ranking(df_final: pd.DataFrame, selected_year: int):
    """Ranking de brecha Agua–Saneamiento con Altair (barras)."""
    chart = (
        alt.Chart(df_final)
        .mark_bar()
        .encode(
            y=alt.Y(
                "country_name:N",
                sort=alt.EncodingSortField(
                    field="gap_water_sanitation_pp", order="descending"
                ),
                title="País",
                axis=alt.Axis(labelLimit=0),
            ),
            x=alt.X(
                "gap_water_sanitation_pp:Q",
                title="Brecha Agua–Saneamiento (p.p.)",
                axis=alt.Axis(format=".2f"),
            ),
            color=alt.Color(
                "risk_level:N",
                scale=alt.Scale(
                    domain=["Rojo", "Amarillo", "Verde"],
                    range=["#E53935", "#F2C94C", "#27AE60"],
                ),
                legend=alt.Legend(title="Semáforo de riesgo"),
            ),
            tooltip=[
                alt.Tooltip("country_name:N", title="País"),
                alt.Tooltip(
                    "water_basic_safe_pct:Q",
                    title="Acceso agua segura (%)",
                    format=".2f",
                ),
                alt.Tooltip(
                    "sanitation_basic_safe_pct:Q",
                    title="Acceso saneamiento seguro (%)",
                    format=".2f",
                ),
                alt.Tooltip(
                    "gap_water_sanitation_pp:Q",
                    title="Brecha (p.p.)",
                    format=".2f",
                ),
                alt.Tooltip(
                    "delta_gap:Q",
                    title="Δ vs. año previo (p.p.)",
                    format=".2f",
                ),
                alt.Tooltip("gap_trend:N", title="Tendencia reciente"),
            ],
        )
        .properties(height=alt.Step(26))
    )

    st.altair_chart(chart, use_container_width=True)


# ======================================================================
# 2. LÓGICA DE ANÁLISIS NARRATIVO
# ======================================================================

def display_kpi_analysis_07(
    df_full_history: pd.DataFrame, df_year: pd.DataFrame, selected_year: int
):
    """Análisis ejecutivo de la brecha, enfocado en prioridades."""
    st.markdown("## 📊 Análisis: ¿Dónde se queda atrás el saneamiento?")
    st.markdown("---")

    df_clean = df_year.dropna(
        subset=["water_basic_safe_pct", "sanitation_basic_safe_pct", "gap_water_sanitation_pp"]
    ).copy()

    if df_clean.empty:
        st.info(f"No hay datos para el análisis en {selected_year}.")
        return

    avg_gap = df_clean["gap_water_sanitation_pp"].mean()
    df_dist = get_risk_distribution(df_clean)
    n_rojo = df_dist[df_dist["risk_level"] == "Rojo"]["count"].sum()
    pct_rojo = df_dist[df_dist["risk_level"] == "Rojo"]["percentage"].sum()
    max_gap_country = df_clean.sort_values(
        by="gap_water_sanitation_pp", ascending=False
    ).iloc[0]

    col_dist, col_exec = st.columns([1, 1])

    with col_dist:
        plot_risk_distribution(df_dist, selected_year)

    with col_exec:
        st.markdown(f"### Conclusiones clave en {selected_year}")
        st.markdown(
            f"""
La **brecha promedio regional** (agua vs. saneamiento) es de **{avg_gap:.2f} p.p.**.

- **Alerta roja:** **{n_rojo} países** ({pct_rojo:.1f}%) están en **riesgo rojo** (brecha > 15 p.p.).  
  En ellos, la cobertura de saneamiento seguro está mucho más rezagada que la de agua segura.  
- **País más crítico:** **{max_gap_country['country_name']}** tiene la brecha más alta,
  con **{max_gap_country['gap_water_sanitation_pp']:.2f} p.p.**.  
- **Recomendación:** los países en rojo y amarillo deben ser el **foco de inversión**
  para cerrar el cuello de botella en saneamiento.
"""
        )

    st.markdown("---")

    st.markdown("### Tendencia histórica de la brecha regional")
    plot_kpi07_evolution_regional(df_full_history)

    if df_full_history["year"].nunique() >= 2:
        min_year = df_full_history["year"].min()
        avg_latest_gap = df_year["gap_water_sanitation_pp"].mean()
        avg_oldest_gap = df_full_history[
            df_full_history["year"] == min_year
        ]["gap_water_sanitation_pp"].mean()
        delta_gap = avg_latest_gap - avg_oldest_gap

        if delta_gap > 1.0:
            icon = "⚠️"
            text = (
                f"La brecha promedio regional **ha aumentado** en **{delta_gap:.2f} p.p.** "
                f"desde {min_year}. El saneamiento se está quedando aún más atrás; se requiere "
                "reforzar la inversión."
            )
        elif delta_gap < -1.0:
            icon = "✅"
            text = (
                f"La brecha promedio regional **ha disminuido** en **{abs(delta_gap):.2f} p.p.** "
                f"desde {min_year}. La tendencia es positiva: la disparidad entre agua y saneamiento "
                "se está cerrando."
            )
        else:
            icon = "🔵"
            text = (
                "La brecha promedio regional se ha mantenido **relativamente estable** "
                "(cambio menor a 1 p.p.). No hay mejoras claras; se requiere mayor impulso."
            )

        st.markdown(f"{icon} **Interpretación de la evolución ({min_year}–{selected_year}):** {text}")


# ======================================================================
# 3. LAYOUT PRINCIPAL 
# ======================================================================

def layout_kpi07():
    """Layout principal del KPI 7 – Brecha Agua vs. Saneamiento Seguro."""
    df = prepare_base_df()
    if df.empty:
        st.warning("No hay datos disponibles para este KPI. Verifica la fuente de datos.")
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
            margin-bottom: 1.0rem;
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
        .kpi-metric-value.danger { color: #E53935; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # ====== HERO ======
    st.markdown(
        """
        <div class="kpi-hero">
          <div class="kpi-hero-left">
            <div class="kpi-hero-title">
              ¿El saneamiento se está quedando atrás frente al agua segura?
            </div>
            <div class="kpi-hero-subtitle">
              Comparamos, en cada país, la cobertura de <b>agua segura</b> y de
              <b>saneamiento básico seguro</b>. Si la brecha es grande, el
              saneamiento se queda atrás respecto al agua
            </div>
            <div class="kpi-hero-tags">
              <span class="kpi-tag green">Agua segura</span>
              <span class="kpi-tag blue">Saneamiento</span>
              <span class="kpi-tag gold">Desigualdad</span>
            </div>
            <div class="kpi-hero-small">
              El indicador usa la <b>brecha en puntos porcentuales</b> entre ambos servicios.
              Círculos y barras en rojo muestran dónde hay mucha más gente con agua
              que con saneamiento seguro.
            </div>
          </div>

          <div class="kpi-hero-right">
            <div style="font-weight: 650, margin-bottom: 0.4rem;">
              Semáforo de agua y saneamiento
            </div>
            <div class="kpi-legend-item">
              🟢 <b>Verde</b>: agua y saneamiento avanzan casi al mismo ritmo.
            </div>
            <div class="kpi-legend-item">
              🟡 <b>Amarillo</b>: el saneamiento empieza a quedarse atrás.
            </div>
            <div class="kpi-legend-item">
              🔴 <b>Rojo</b>: el saneamiento está claramente rezagado;
              alta prioridad.
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ====== FILTRO DE AÑO (debajo del hero, antes de la sección 1) ======
    years = sorted(df["year"].unique())
    current_year = st.selectbox(
        "Año para el mapa, el ranking y el análisis",
        options=years,
        index=len(years) - 1,
        key="kpi07_year_selector",
    )

    df_year = df[df["year"] == current_year].copy()
    if df_year.empty:
        st.warning("No hay datos para el año seleccionado.")
        return

    total_countries = df_year["country_name"].nunique()
    red_countries = df_year[df_year["risk_level"] == "Rojo"]["country_name"].nunique()
    worst_row = df_year.sort_values(
        "gap_water_sanitation_pp", ascending=False
    ).iloc[0]

    st.markdown(" ")  # pequeño espacio

    # ====== 1) ¿Qué mide? + tarjeta ======
    left, right = st.columns([2.1, 1.2])

    with left:
        st.markdown(
            """
            ### 1. 👀 ¿Qué mide este indicador?

            - Para cada país, miramos el porcentaje de población con **agua segura**
              y con **saneamiento seguro**.
            - Calculamos la **brecha en puntos porcentuales (p.p.)**:
              agua segura (%) − saneamiento seguro (%).
            - Una brecha grande y positiva significa que hay mucha más gente con agua
              segura que con saneamiento seguro: el saneamiento se queda atrás respecto al agua**.

            El año que selecciones arriba se aplica al **mapa**, al **ranking**
            y al análisis de distribución de riesgo.
            """
        )

        with st.expander("Ver definición técnica del indicador"):
            st.markdown(
                """
*Indicador técnico*  
Brecha absoluta **agua–saneamiento seguro** en puntos porcentuales (p.p.) para cada país y año.

*Cálculo básico*  

- **Cobertura de agua segura (%):** porcentaje de población con acceso a servicios de agua funcionales y seguros.  
- **Cobertura de saneamiento seguro (%):** porcentaje de población con acceso a saneamiento básico seguro.  
- **Brecha agua–saneamiento (p.p.):** diferencia entre ambos porcentajes  
  (agua segura (%) menos saneamiento seguro (%)).  
- El semáforo se basa en el **valor de esa brecha** (en p.p.); valores más altos indican
  que el saneamiento se queda atrás respecto al agua.

*Semáforo de riesgo (según brecha en p.p.)*  

- 🟢 **Verde (0–10)**: brecha baja; agua y saneamiento avanzan juntos.  
- 🟡 **Amarillo (>10–15)**: brecha moderada; el saneamiento ya se rezaga.  
- 🔴 **Rojo (>15)**: brecha alta; el saneamiento es un cuello de botella claro.
                """
            )

    with right:
        st.markdown(
            f"""
            <div class="kpi-metrics-card">
              <div class="kpi-metric">
                <div class="kpi-metric-label">Países analizados en {current_year}</div>
                <div class="kpi-metric-value accent">{total_countries}</div>
              </div>
              <div class="kpi-metric">
                <div class="kpi-metric-label">Países en rojo (brecha &gt; 15 p.p.)</div>
                <div class="kpi-metric-value danger">{red_countries}</div>
              </div>
              <div class="kpi-metric">
                <div class="kpi-metric-label">País con mayor brecha</div>
                <div class="kpi-metric-value accent">{worst_row['country_name']}</div>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("---")

    # ====== SECCIÓN 1: Análisis ejecutivo ======
    display_kpi_analysis_07(df, df_year, current_year)
    st.markdown("---")

    # ====== SECCIÓN 2 y 3: mapa + ranking ======
    st.header("🗺️ Priorización de países y desafíos")

    col_mapa, col_ranking = st.columns([1, 1])

    # --- MAPA ---
    with col_mapa:
        st.subheader(f"1. ¿En que países hay más diferencia entre saneamiento y agua segura en {current_year}?")

        df_map = df_year.copy()
        df_map["gap_val"] = df_map["gap_water_sanitation_pp"]

        gap_max = df_map["gap_val"].max()
        if gap_max and gap_max > 0:
            min_radius, max_radius = 20000, 80000
            df_map["radius"] = 25000 + (df_map["gap_val"] / gap_max) * (
                max_radius - min_radius
            )
        else:
            df_map["radius"] = 25000

        df_map["radius"] = df_map["radius"].astype(float)

        data_for_pydeck = df_map.dropna(subset=["lon", "lat"])[
            [
                "lon",
                "lat",
                "radius",
                "color",
                "country_name",
                "gap_water_sanitation_pp",
                "risk_level",
                "gap_trend",
            ]
        ].to_dict("records")

        layer = pdk.Layer(
            "ScatterplotLayer",
            data=data_for_pydeck,
            get_position="[lon, lat]",
            get_fill_color="color",
            get_radius="radius",
            pickable=True,
            opacity=0.8,
        )

        view_state = pdk.ViewState(latitude=0, longitude=-70, zoom=2, pitch=0)

        tooltip = {
            "html": (
                "<b>{country_name}</b><br/>"
                "Brecha agua–saneamiento: <b>{gap_water_sanitation_pp:.2f} p.p.</b><br/>"
                "Riesgo: <b>{risk_level}</b><br/>"
                "Tendencia: <b>{gap_trend}</b>"
            ),
            "style": {"color": "white"},
        }

        deck = pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip=tooltip,
        )

        st.pydeck_chart(deck)

        st.caption(
            "El tamaño y color del círculo indican la magnitud de la brecha. "
            "Círculos grandes y rojos señalan los países donde el saneamiento está más rezagado."
        )

    # --- RANKING ---
    with col_ranking:
        st.subheader(f"2. Ranking de prioridad por brecha entre saneamiento y agua segura en({current_year})")

        df_rank = df_year.sort_values(
            by="gap_water_sanitation_pp", ascending=False
        ).copy()
        plot_kpi07_ranking(df_rank, current_year)

        st.caption(
            "Los países en la parte superior presentan el mayor rezago de saneamiento respecto al agua. "
            "Incluye la tendencia reciente año a año."
        )

