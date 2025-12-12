import streamlit as st
import pandas as pd
import pydeck as pdk
import altair as alt

from common import COUNTRY_COORDS, S3_PATH_KPI05

# ======================================================================
# 1. LÓGICA DE DATOS
# ======================================================================

@st.cache_data
def load_kpi05() -> pd.DataFrame:
    """Lee el modelo Gold de KPI05 desde S3 (formato Parquet)."""
    try:
        df = pd.read_parquet(S3_PATH_KPI05)
    except Exception as e:
        st.error(f"Error al cargar datos de KPI05 desde S3: {e}")
        return pd.DataFrame()
    return df


def risk_color_rgb(risk_level: str):
    """Colores RGBA para el mapa según semáforo (Verde / Amarillo / Rojo)."""
    if risk_level == "Verde":
        return [39, 174, 96, 200]
    if risk_level == "Amarillo":
        return [242, 201, 76, 220]
    if risk_level == "Rojo":
        return [229, 57, 53, 220]
    return [180, 180, 180, 180]


def risk_hex(risk_level: str) -> str:
    """Colores HEX para Altair."""
    mapping = {
        "Verde": "#27AE60",
        "Amarillo": "#F2C94C",
        "Rojo": "#E53935",
    }
    return mapping.get(risk_level, "#B4B4B4")


def prepare_base_df() -> pd.DataFrame:
    """
    Carga y enriquece el DataFrame base para el KPI 5:
    brecha absoluta urbano–rural de acceso a agua segura.
    """
    df = load_kpi05().copy()
    if df.empty:
        return df

    # Solo países que tenemos georreferenciados
    df = df[df["country_name"].isin(COUNTRY_COORDS.keys())].copy()

    # Coordenadas país
    df["lat"] = df["country_name"].map(lambda c: COUNTRY_COORDS[c][0])
    df["lon"] = df["country_name"].map(lambda c: COUNTRY_COORDS[c][1])

    # Color según semáforo y etiqueta
    df["color"] = df["risk_level"].apply(risk_color_rgb)
    df["country_label"] = df["country_name"]

    # Orden cronológico
    df = df.sort_values(["country_name", "year"])

    # Diferencia de brecha vs año previo
    df["prev_gap"] = df.groupby("country_name")["gap_urban_rural_pp"].shift(1)
    df["delta_gap"] = df["gap_urban_rural_pp"] - df["prev_gap"]

    # Etiqueta de tendencia
    def classify_trend(delta):
        if pd.isna(delta):
            return "Sin datos previos"
        if delta < -0.1:
            return "Mejorando"
        if delta > 0.1:
            return "Empeorando"
        return "Estable"

    df["gap_trend"] = df["delta_gap"].apply(classify_trend)

    # Tipos numéricos
    df["lon"] = df["lon"].astype(float)
    df["lat"] = df["lat"].astype(float)
    df["gap_urban_rural_pp"] = df["gap_urban_rural_pp"].astype(float)

    # Quitamos filas sin brecha
    df = df.dropna(subset=["gap_urban_rural_pp"])

    return df


def risk_short_text(risk_level: str) -> str:
    """Texto corto para explicar el semáforo en el ejemplo final."""
    mapping = {
        "Verde": "la brecha es baja (diferencia menor a 10 p.p.).",
        "Amarillo": "la brecha es relevante (entre 10 y 20 p.p.).",
        "Rojo": "la brecha es crítica (mayor a 20 p.p.).",
    }
    return mapping.get(risk_level, "")

# ======================================================================
# 2. ANÁLISIS NARRATIVO
# ======================================================================

def display_kpi_analysis_05(df_full_history: pd.DataFrame, selected_year: int):
    """Análisis regional de la brecha absoluta y su evolución."""

    st.markdown("## 📈 Detalle y análisis histórico de la brecha")

    if df_full_history.empty:
        st.warning("No hay datos disponibles para el análisis detallado.")
        return

    # Estado actual para el año seleccionado
    df_year = df_full_history[df_full_history["year"] == selected_year].copy()
    if df_year.empty:
        st.info(f"No hay datos de brecha para el año {selected_year}.")
        return

    df_clean = df_year.dropna(
        subset=["water_urban_pct", "water_rural_pct", "gap_urban_rural_pp"]
    ).copy()
    if df_clean.empty:
        st.warning(
            "No hay países con datos completos de cobertura urbana y rural en este año."
        )
        return

    # País con mayor brecha absoluta (valor absoluto)
    max_gap_country = df_clean.reindex(
        df_clean["gap_urban_rural_pp"].abs().sort_values(ascending=False).index
    ).iloc[0]
    avg_gap_abs = df_clean["gap_urban_rural_pp"].abs().mean()

    st.markdown(
        f"""
**Contexto regional en {selected_year}**

- La brecha absoluta promedio de acceso a agua segura en la región es de **{avg_gap_abs:.2f} p.p.**  
- El país con la **mayor desigualdad urbano–rural** es **{max_gap_country['country_name']}**:
  - Cobertura urbana: **{max_gap_country['water_urban_pct']:.2f}%**  
  - Cobertura rural: **{max_gap_country['water_rural_pct']:.2f}%**  
  - Brecha absoluto urbano–rural: **{max_gap_country['gap_urban_rural_pp']:.2f} p.p.**  
  - Semáforo: **{max_gap_country['risk_level']}**.
"""
    )

    # Evolución regional entre primer año y año seleccionado
    years_unique = sorted(df_full_history["year"].unique())
    if len(years_unique) < 2:
        return

    min_year = years_unique[0]
    df_oldest = df_full_history[df_full_history["year"] == min_year]
    avg_oldest_gap_abs = df_oldest["gap_urban_rural_pp"].abs().mean()
    avg_latest_gap_abs = avg_gap_abs

    delta_gap_abs = avg_latest_gap_abs - avg_oldest_gap_abs

    if delta_gap_abs < -1.0:
        trend_text = (
            f"la brecha promedio **ha disminuido** en "
            f"**{abs(delta_gap_abs):.2f} p.p.**, lo que indica una mejora en la equidad."
        )
    elif delta_gap_abs > 1.0:
        trend_text = (
            f"la brecha promedio **ha aumentado** en "
            f"**{delta_gap_abs:.2f} p.p.**, señal de mayor desigualdad de acceso."
        )
    else:
        trend_text = "la brecha promedio se ha mantenido **relativamente estable**."

    st.markdown(
        f"""
**Evolución regional ({min_year} → {selected_year})**

- Brecha absoluta promedio inicial: **{avg_oldest_gap_abs:.2f} p.p.**  
- Brecha absoluta promedio actual: **{avg_latest_gap_abs:.2f} p.p.**  

En resumen, {trend_text}
"""
    )

# ======================================================================
# 3. LAYOUT PRINCIPAL
# ======================================================================

def layout_kpi05():
    """Layout principal del KPI 5 – Brecha urbano–rural en acceso al agua segura."""

    # ---------- Estilos y HERO ----------
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
        .kpi-tag.green { background-color: rgba(39, 174, 96, 0.25); }
        .kpi-tag.blue { background-color: rgba(52, 152, 219, 0.25); }
        .kpi-tag.purple { background-color: rgba(155, 89, 182, 0.25); }
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

    st.markdown(
        """
        <div class="kpi-hero">
          <div class="kpi-hero-left">
            <div class="kpi-hero-title">
              ¿Qué tan grande es la brecha urbano–rural en acceso al agua?
            </div>
            <div class="kpi-hero-subtitle">
              Miramos, para cada país, cuánta diferencia hay entre el porcentaje de personas con
              <b>agua segura</b> en zonas <b>urbanas</b> y en zonas <b>rurales</b>.
              Mientras más grande la brecha, más desigual es el acceso.
            </div>
            <div class="kpi-hero-tags">
              <span class="kpi-tag green">Equidad</span>
              <span class="kpi-tag blue">Urbano vs. rural</span>
              <span class="kpi-tag purple">Agua segura</span>
            </div>
            <div class="kpi-hero-small">
              El KPI usa la <b>brecha absoluta</b> en puntos porcentuales:
              la diferencia directa entre la cobertura urbana y la rural.
              Un país en rojo tiene mucha más gente con agua segura en ciudades que en el campo.
            </div>
          </div>

          <div class="kpi-hero-right">
            <div style="font-weight: 650; margin-bottom: 0.4rem;">
              Semáforo de brecha urbano–rural
            </div>
            <div class="kpi-legend-item">
              🟢 <b>Verde</b>: casi paridad urbano–rural.
            </div>
            <div class="kpi-legend-item">
              🟡 <b>Amarillo</b>: diferencia importante.
            </div>
            <div class="kpi-legend-item">
              🔴 <b>Rojo</b>: desigualdad crítica; el campo se queda atrás.
            </div>
            <div style="margin-top: 0.5rem;" class="kpi-hero-small">
              El mapa y el ranking muestran dónde la brecha es más grande
              y si está mejorando o empeorando con el tiempo.
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ---------- Datos y filtro de año ----------
    df = prepare_base_df()
    if df.empty:
        st.warning("No hay datos disponibles para este KPI.")
        return

    years = sorted(df["year"].unique())
    current_year = st.selectbox(
        "Año para el semáforo, el mapa y el ranking",
        options=years,
        index=len(years) - 1,
    )

    df_year = df[df["year"] == current_year].copy()
    if df_year.empty:
        st.warning("No hay datos para el año seleccionado.")
        return

    # =========================
    # 1) ¿Qué mide? + tarjeta
    # =========================
    left, right = st.columns([2.1, 1.2])

    with left:
        st.markdown(
            """
            ### 1. 👀 ¿Qué mide este indicador?

            - Para cada país, compara el porcentaje de población con **agua segura**
              en zonas **urbanas** y en zonas **rurales**.
            - A partir de esa diferencia construye la **brecha absoluta** en puntos porcentuales
              (p.p.) entre ciudad y campo.
            - Una brecha grande significa que el lugar donde vives (campo o ciudad) cambia mucho
              tus posibilidades de tener agua segura.

            El año que selecciones arriba se aplica al **mapa**, al **ranking**
            y a la **serie de tiempo**.
            """
        )

        with st.expander("Ver definición técnica del indicador"):
            st.markdown(
                """
*Indicador técnico*  
Brecha absoluta urbano–rural de acceso a **agua segura**, en puntos porcentuales (p.p.)
para cada país y año.

*Cálculo básico*  

- **Cobertura urbana**: porcentaje de población urbana con agua segura.  
- **Cobertura rural**: porcentaje de población rural con agua segura.  
- **Brecha absoluta urbano–rural**: diferencia en p.p. entre cobertura urbana y rural  
  (urbano − rural).  
- Para clasificar la intensidad del riesgo se utiliza el **valor absoluto** de la brecha.

*Semáforo de riesgo (intensidad de la brecha)*  

- 🟢 **Verde (brecha &lt; 10 p.p.)**: paridad o diferencia baja.  
- 🟡 **Amarillo (10–20 p.p.)**: brecha relevante.  
- 🔴 **Rojo (≥ 20 p.p.)**: desigualdad crítica.
                """
            )

    with right:
        total_paises = df_year["country_name"].nunique()

        # País con mayor brecha absoluta
        worst_row = df_year.reindex(
            df_year["gap_urban_rural_pp"].abs().sort_values(ascending=False).index
        ).iloc[0]

        st.markdown(
            f"""
            <div class="kpi-metrics-card">
              <div class="kpi-metric">
                <div class="kpi-metric-label">Países analizados en {current_year}</div>
                <div class="kpi-metric-value accent">{total_paises}</div>
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

    # =========================
    # 2) Mapa de brecha
    # =========================
    st.subheader(f"2. 📍¿Dónde es mayor la brecha urbano–rural en acceso al agua en {current_year}?")

    df_map = df_year.copy()
    df_map["gap_abs"] = df_map["gap_urban_rural_pp"].abs()

    gap_max = df_map["gap_abs"].max()
    if gap_max and gap_max > 0:
        min_radius = 20000
        max_radius = 80000
        df_map["radius"] = min_radius + (df_map["gap_abs"] / gap_max) * (
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
            "gap_urban_rural_pp",
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
            "Brecha absoluta: <b>{gap_urban_rural_pp:.2f} p.p.</b><br/>"
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
        "El tamaño y color del círculo indican la magnitud de la brecha absoluta de acceso a agua segura (urbano − rural)."
    )

    st.markdown("---")

    # =========================
    # 3) Serie de tiempo por país
    # =========================
    st.subheader("3. 🔍 Evolución de la brecha por país")

    country_sel = st.selectbox(
        "Selecciona un país para ver la serie de tiempo:",
        options=sorted(df["country_name"].unique()),
    )

    df_trend = df[df["country_name"] == country_sel].sort_values("year").copy()

    if df_trend.empty:
        st.info("No hay datos de evolución para el país seleccionado.")
    else:
        line = (
            alt.Chart(df_trend)
            .mark_line(point=True)
            .encode(
                x=alt.X("year:O", title="Año"),
                y=alt.Y(
                    "gap_urban_rural_pp:Q",
                    title="Brecha absoluta (p.p.) [urbano − rural]",
                ),
                color=alt.value("#1f77b4"),
                tooltip=[
                    alt.Tooltip("year:O", title="Año"),
                    alt.Tooltip(
                        "gap_urban_rural_pp:Q",
                        title="Brecha (p.p.)",
                        format=".2f",
                    ),
                    alt.Tooltip("risk_level:N", title="Semáforo"),
                    alt.Tooltip("gap_trend:N", title="Tendencia"),
                ],
            )
            .properties(height=300)
        )
        st.altair_chart(line, use_container_width=True)

        last_row = df_trend.iloc[-1]
        st.markdown(
            f"""
En **{last_row['year']}**, la brecha en **{country_sel}** es de **{last_row['gap_urban_rural_pp']:.2f} p.p.**  
Semáforo: **{last_row['risk_level']}**. Tendencia: **{last_row['gap_trend']}**.
            """
        )

    st.markdown("---")

    # =========================
    # 4) Ranking tipo semáforo
    # =========================
    st.subheader(f"4. 📊 Ranking de países con mayor brecha urbano–rural en acceso al agua en {current_year}")

    df_rank = df_year.copy()
    df_rank["gap_abs_for_sort"] = df_rank["gap_urban_rural_pp"].abs()
    df_rank = df_rank.sort_values("gap_abs_for_sort", ascending=False)

    chart = (
        alt.Chart(df_rank)
        .mark_bar()
        .encode(
            y=alt.Y(
                "country_name:N",
                sort=alt.EncodingSortField(
                    field="gap_abs_for_sort",
                    order="descending",
                ),
                title="País",
                axis=alt.Axis(labelLimit=0),
            ),
            x=alt.X(
                "gap_urban_rural_pp:Q",
                title="Brecha absoluta (p.p.) [urbano − rural]",
                axis=alt.Axis(format=".1f"),
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
                    "water_urban_pct:Q",
                    title="Acceso urbano (%)",
                    format=".2f",
                ),
                alt.Tooltip(
                    "water_rural_pct:Q",
                    title="Acceso rural (%)",
                    format=".2f",
                ),
                alt.Tooltip(
                    "gap_urban_rural_pp:Q",
                    title="Brecha (p.p.)",
                    format=".2f",
                ),
                alt.Tooltip(
                    "delta_gap:Q",
                    title="Δ brecha vs. año previo (p.p.)",
                    format=".2f",
                ),
                alt.Tooltip("gap_trend:N", title="Tendencia"),
            ],
        )
        .properties(height=alt.Step(26))
    )

    st.altair_chart(chart, use_container_width=True)

    worst = df_rank.iloc[0]
    if not worst.empty:
        gap = worst["gap_urban_rural_pp"]
        risk = worst["risk_level"]

        st.markdown("### 🧩 ¿Cómo leer este ranking?")

        st.markdown(
            f"""
En **{worst['country_name']}** ({worst['year']}), la brecha absoluta es de **{gap:.2f} p.p.**.  
Eso significa que la cobertura de agua segura en zonas **urbanas** es
aproximadamente **{abs(gap):.2f} p.p.** más alta (o más baja, si la brecha es negativa)
que en las zonas **rurales**.  

El semáforo está en **{risk}**, lo que indica que {risk_short_text(risk)}
            """
        )

    # =========================
    # 5) Análisis regional adicional
    # =========================
    display_kpi_analysis_05(df, current_year)
