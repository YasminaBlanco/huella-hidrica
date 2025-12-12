# app_kpi03.py
#
# KPI 3 – Zonas críticas para inversión (clima + saneamiento, México y Argentina)

import streamlit as st
import pandas as pd
import pydeck as pdk
import altair as alt

from common import (
    load_kpi03,
    PROVINCE_COORDS_MX,
    PROVINCE_COORDS_AR,
)

# =============================================================================
# Helpers
# =============================================================================
def _add_province_coords(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega lat/lon a cada provincia usando los diccionarios de coordenadas.
    """
    df = df.copy()

    # México
    mask_mx = df["country_name"] == "Mexico"
    df.loc[mask_mx, "lat"] = df.loc[mask_mx, "province_name"].map(
        lambda n: PROVINCE_COORDS_MX.get(n, (None, None))[0]
    )
    df.loc[mask_mx, "lon"] = df.loc[mask_mx, "province_name"].map(
        lambda n: PROVINCE_COORDS_MX.get(n, (None, None))[1]
    )

    # Argentina
    mask_ar = df["country_name"] == "Argentina"
    df.loc[mask_ar, "lat"] = df.loc[mask_ar, "province_name"].map(
        lambda n: PROVINCE_COORDS_AR.get(n, (None, None))[0]
    )
    df.loc[mask_ar, "lon"] = df.loc[mask_ar, "province_name"].map(
        lambda n: PROVINCE_COORDS_AR.get(n, (None, None))[1]
    )
    df = df.dropna(subset=["lat", "lon"])

    return df


def _province_color(row: pd.Series):
    """
    Color para cada provincia en el mapa (semáforo):

    - Rojo: zona crítica (bajo saneamiento + lluvias en descenso)
    - Amarillo: cumple una de las dos condiciones (bajo saneamiento O lluvias en descenso)
    - Verde: sin alerta (buen saneamiento y lluvias estables/crecientes)
    """
    if bool(row.get("is_critical_zone", False)):
        return [229, 57, 53, 220]  # rojo
    if bool(row.get("is_low_sanitation", False)) or bool(
        row.get("is_climate_neg_trend", False)
    ):
        return [242, 201, 76, 210]  # amarillo
    return [39, 174, 96, 200]  # verde


def _sanitation_status(row: pd.Series) -> str:
    """Etiqueta simplificada solo para saneamiento."""
    if bool(row.get("is_low_sanitation", False)):
        return "Bajo saneamiento (debajo del umbral)"
    return "Saneamiento adecuado"


def _climate_status(row: pd.Series) -> str:
    """Etiqueta simplificada solo para clima."""
    if bool(row.get("is_climate_neg_trend", False)):
        return "Lluvias en descenso"
    return "Lluvias estables o en aumento"


SANITATION_COLOR_SCALE = alt.Scale(
    domain=[
        "Bajo saneamiento (debajo del umbral)",
        "Saneamiento adecuado",
    ],
    range=[
        "#ffb703",  # amarillo (bajo saneamiento)
        "#8ecae6",  # azul claro (adecuado)
    ],
)

CLIMATE_COLOR_SCALE = alt.Scale(
    domain=[
        "Lluvias en descenso",
        "Lluvias estables o en aumento",
    ],
    range=[
        "#FF9F1C",  # naranja fuerte
        "#90e0ef",  # azul claro
    ],
)

# =============================================================================
# Sección por país
# =============================================================================
def _render_country_section(df_year: pd.DataFrame, country_name: str) -> None:
    """
    Renderiza la información de saneamiento (como métrica) y clima (gráfico de barras)
    para un país concreto (México o Argentina).
    """
    df_c = df_year[df_year["country_name"] == country_name].copy()
    if df_c.empty:
        st.info(f"No hay datos para {country_name} en este año.")
        return

    n_provinces = df_c["province_name"].nunique()

    st.markdown(f"## {country_name} – saneamiento y clima por provincia")
    st.markdown(f"### Filtro de provincias ({country_name})")

    # El filtro se aplica solo al gráfico de clima
    max_provincias = st.slider(
        "Número máximo de provincias para el gráfico de clima (ordenadas por lluvia)",
        min_value=3,
        max_value=int(n_provinces),
        value=int(min(n_provinces, 10)),
        key=f"slider_max_provinces_{country_name}",
    )

    # Ordenamos por precipitación para el gráfico de clima
    df_cli = df_c.sort_values("precip_total_mm_year", ascending=False).head(
        max_provincias
    )

    # ------------------------------------------------------------------
    # MÉTRICA: saneamiento por país
    # ------------------------------------------------------------------
    st.markdown("### 🚰 Saneamiento básico: cobertura por país")

    san_pct = df_c["sanitation_basic_pct"].iloc[0] if not df_c.empty else None
    is_low_sanitation_country = df_c["is_low_sanitation"].any()
    san_status_text = (
        "Alto riesgo (debajo del umbral)"
        if is_low_sanitation_country
        else "Saneamiento adecuado"
    )

    san_color = "#ffb703" if is_low_sanitation_country else "#8ecae6"
    san_bg = (
        "rgba(255, 183, 3, 0.15)"
        if is_low_sanitation_country
        else "rgba(142, 202, 230, 0.15)"
    )
    san_border = (
        "rgba(255, 183, 3, 0.5)"
        if is_low_sanitation_country
        else "rgba(142, 202, 230, 0.5)"
    )

    st.markdown(
        f"""
        <div style="
            border-radius: 10px;
            padding: 1rem 1.2rem;
            margin-bottom: 1.5rem;
            background: {san_bg};
            border: 1.5px solid {san_border};
        ">
            <div style="font-size: 1.0rem; font-weight: 500;">
                Cobertura de saneamiento básico (% de población con acceso):
            </div>
            <div style="
                font-size: 2.2rem;
                font-weight: 700;
                color: {san_color};
                margin: 0.2rem 0 0.5rem;
            ">
                {san_pct:.1f}%
            </div>
            <div style="font-size: 0.9rem;">
                Estado: <b>{san_status_text}</b>. 
                Este valor de cobertura es <b>uniforme</b> para todas las provincias del país en {df_year["year"].iloc[0]}, según la fuente de datos.
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ------------------------------------------------------------------
    # Gráfico: clima por provincia
    # ------------------------------------------------------------------
    st.markdown("### 🌧️ Clima: lluvia anual y tendencia por provincia")

    chart_cli = (
        alt.Chart(df_cli)
        .mark_bar()
        .encode(
            x=alt.X(
                "province_name:N",
                sort=list(df_cli["province_name"]),
                axis=alt.Axis(
                    title="Provincia",
                    labelAngle=-50,
                    labelOverlap=False,
                    labelFontSize=11,
                ),
            ),
            y=alt.Y(
                "precip_total_mm_year:Q",
                title="Precipitación total anual (mm)",
            ),
            color=alt.Color(
                "climate_status:N",
                title="Estado del clima",
                scale=CLIMATE_COLOR_SCALE,
            ),
            tooltip=[
                alt.Tooltip("province_name:N", title="Provincia"),
                alt.Tooltip(
                    "precip_total_mm_year:Q",
                    title="Lluvia anual (mm)",
                    format=".0f",
                ),
                alt.Tooltip("climate_trend:N", title="Tendencia de lluvia"),
                alt.Tooltip("climate_status:N", title="Estado del clima"),
            ],
        )
        .properties(height=420)
    )

    st.altair_chart(chart_cli, use_container_width=True)

    st.markdown(
        """
**Cómo leer el gráfico de clima**

- Cada barra es una **provincia** y muestra cuánta lluvia recibió en el año.
- El gráfico va de la provincia con **más lluvia** a la de menos.
- El **color** indica la tendencia histórica:
  - Naranja intenso: las lluvias van a la baja (mayor riesgo).
  - Azul claro: lluvias estables o en aumento.
        """
    )


# =============================================================================
# Layout principal
# =============================================================================
def layout_kpi03() -> None:
    # =========================
    # Estilos globales
    # =========================
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
            color: #FACC15;  /* amarillo suave para resaltar números */
        }

        .kpi-metric-pill {
            display: inline-block;
            margin-top: 0.18rem;
            padding: 0.12rem 0.7rem;
            border-radius: 999px;
            font-size: 0.78rem;
            background: rgba(15, 118, 210, 0.08);
            border: 1px solid rgba(59, 130, 246, 0.8);
            color: #E0F2FE;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # =========================
    # HERO
    # =========================
    st.markdown(
        """
        <div class="kpi-hero">
          <div class="kpi-hero-left">
            <div class="kpi-hero-title">
              ¿Dónde se juntan clima y saneamiento para priorizar inversión?
            </div>
            <div class="kpi-hero-subtitle">
              Buscamos provincias de México y Argentina donde se combinan dos alertas:
              <b>baja cobertura de saneamiento básico</b> y <b>lluvias en descenso</b>.
              Esas provincias son candidatas naturales para <b>inversión prioritaria</b> y
              apoyo humanitario
            </div>
            <div class="kpi-hero-tags">
              <span class="kpi-tag green">Saneamiento</span>
              <span class="kpi-tag blue">Clima</span>
              <span class="kpi-tag purple">Zonas críticas</span>
            </div>
            <div class="kpi-hero-small">
              Abajo puedes ver cuántas son, en qué país están y cómo se comporta el clima en cada una.
            </div>
          </div>

          <div class="kpi-hero-right">
            <div style="font-weight: 650; margin-bottom: 0.4rem;">
              Semáforo de zonas críticas (por país)
            </div>
            <div class="kpi-legend-item">🟢 <b>Verde</b>: pocas provincias en situación crítica.</div>
            <div class="kpi-legend-item">🟡 <b>Amarillo</b>: número moderado de zonas críticas.</div>
            <div class="kpi-legend-item">🔴 <b>Rojo</b>: muchas provincias críticas, alta prioridad.</div>
            <div style="margin-top: 0.5rem;" class="kpi-hero-small">
              Donde se acumulan más <b>zonas críticas</b> es donde el país necesita
              más inversión en agua y saneamiento.
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # =========================
    # Carga de datos
    # =========================
    df = load_kpi03()
    if df is None or df.empty:
        st.warning("No se encontraron datos para el KPI 3.")
        return

    df = df[df["country_name"].isin(["Mexico", "Argentina"])].copy()
    if df.empty:
        st.warning("No hay datos de México o Argentina en este modelo.")
        return

    df = _add_province_coords(df)

    years = sorted(df["year"].dropna().unique())
    if not years:
        st.warning("No se encontraron años válidos en los datos.")
        return

    year_sel = st.selectbox("Año de análisis", years, index=len(years) - 1)

    df_year = df[df["year"] == year_sel].copy()
    if df_year.empty:
        st.info(f"No hay datos para el año {year_sel}.")
        return

    df_year["sanitation_status"] = df_year.apply(_sanitation_status, axis=1)
    df_year["climate_status"] = df_year.apply(_climate_status, axis=1)

    sem_resumen = (
        df_year.groupby("country_name")["is_critical_zone"]
        .sum()
        .reset_index(name="n_critical_zones")
    )

    # =========================
    # 1) ¿Qué mide? 
    # =========================
    top_left, top_right = st.columns([2.1, 1.2])

    with top_left:
        st.markdown(
            """
            ### 1. 👀 ¿Qué mide este indicador?

            - Cuenta cuántas **provincias** son **zonas críticas** porque combinan:
              - **Baja cobertura de saneamiento básico**, y  
              - **Lluvias en descenso** en los últimos años.
            - Se calcula por **país** (México / Argentina) y por **año**.
            - Valores altos significan más territorios donde la gente vive con
              poca infraestructura de saneamiento y un clima que se está volviendo más seco.

            El año que selecciones arriba se aplica al **semáforo**, a los
            **gráficos por país** y a los **mapas** de provincias.
            """
        )

        with st.expander("Ver definición técnica del indicador"):
            st.markdown(
                """
*Indicador técnico:*  
El KPI cuenta, para México y Argentina, el **número de provincias** que se consideran
*zonas críticas de vulnerabilidad hídrica* en cada año.

*Cálculo básico:*  

- Unidad de análisis: **provincia/estado**.  
- Una provincia se marca como **zona crítica** cuando se cumplen las dos condiciones:
  - **Bajo saneamiento básico:** la cobertura de saneamiento básico es **menor a 80%**.  
  - **Lluvias en descenso:** la tendencia de precipitación es **decreasing**, es decir,  
    la correlación entre año y precipitación anual es **≤ -0.3**.  
- Para cada país y año:  
  - **Número de zonas críticas** = conteo de provincias que cumplen ambas condiciones.

*Tendencia de la precipitación:*  

- **decreasing**: la lluvia muestra una tendencia descendente  
  (correlación año–precipitación ≤ -0.3).  
- **increasing**: la lluvia muestra una tendencia creciente  
  (correlación ≥ +0.3).  
- **stable**: no hay cambio claro  
  (correlación entre -0.3 y +0.3).  
- **uncertain**: hay menos de 3 años de datos, por lo que no se puede estimar bien la tendencia.

*Semáforo de zonas críticas (por país):*  

- 🟢 **Verde (0–5)**: pocas provincias en situación crítica.  
- 🟡 **Amarillo (6–20)**: número moderado de zonas críticas.  
- 🔴 **Rojo (>20)**: muchas provincias críticas, alta prioridad.
                """
            )

    with top_right:
        # Zonas analizadas por país
        total_zonas = df_year["province_name"].nunique()
        n_mx = (
            df_year[df_year["country_name"] == "Mexico"]["province_name"]
            .nunique()
        )
        n_ar = (
            df_year[df_year["country_name"] == "Argentina"]["province_name"]
            .nunique()
        )

        st.markdown(
            f"""
            <div class="kpi-metrics-card">
              <div class="kpi-metric">
                <div class="kpi-metric-label">Zonas analizadas en {year_sel}</div>
                <div class="kpi-metric-value accent">{total_zonas}</div>
              </div>
              <div class="kpi-metric">
                <div class="kpi-metric-label">México</div>
                <div class="kpi-metric-value accent">{n_mx}</div>
              </div>
              <div class="kpi-metric">
                <div class="kpi-metric-label">Argentina</div>
                <div class="kpi-metric-value accent">{n_ar}</div>
              </div>
              <div style="font-size: 0.78rem; opacity: 0.9; margin-top: 0.25rem;">
                Cada <b>zona</b> es una provincia o estado con datos de clima y saneamiento.
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("---")

    # =========================
    # 2) Semáforo por país
    # =========================
    st.subheader(f"2. 🚦 Semáforo de zonas críticas por país – {year_sel}")

    if sem_resumen.empty:
        st.info("No hay provincias marcadas como críticas para este año.")
    else:
        cols = st.columns(len(sem_resumen))

        for i, row in enumerate(sem_resumen.itertuples(index=False)):
            country = row.country_name
            n = int(row.n_critical_zones)

            if n <= 5:
                nivel_text = "Verde (0–5 zonas críticas)"
                bg = "rgba(46, 204, 113, 0.12)"
                border = "rgba(46, 204, 113, 0.8)"
                emoji = "🟢"
            elif n <= 20:
                nivel_text = "Amarillo (6–20 zonas críticas)"
                bg = "rgba(241, 196, 15, 0.15)"
                border = "rgba(241, 196, 15, 0.9)"
                emoji = "🟡"
            else:
                nivel_text = "Rojo (>20 zonas críticas)"
                bg = "rgba(231, 76, 60, 0.13)"
                border = "rgba(231, 76, 60, 0.85)"
                emoji = "🔴"

            with cols[i]:
                st.markdown(
                    f"""
                    <div style="
                        border-radius: 14px;
                        padding: 1rem 1.2rem;
                        margin-bottom: 0.5rem;
                        background: {bg};
                        border: 1.5px solid {border};
                        text-align: center;
                    ">
                        <div style="font-size: 1.1rem; font-weight: 600;">
                            {country}
                        </div>
                        <div style="
                            font-size: 2.0rem;
                            font-weight: 700;
                            margin: 0.35rem 0 0.15rem;
                        ">
                            {emoji} {n}
                        </div>
                        <div style="font-size: 0.95rem;">
                            {nivel_text}
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    # =========================
    # 3) Detalle por país
    # =========================
    _render_country_section(df_year, country_name="Mexico")
    _render_country_section(df_year, country_name="Argentina")

    # =========================
    # 4) Mapas de provincias críticas por país
    # =========================
    st.subheader("4. 🗺️ Mapas de zonas críticas por provincia")

    df_year["color"] = df_year.apply(_province_color, axis=1)

    col_mx, col_ar = st.columns(2)

    with col_mx:
        st.markdown("### México")
        df_mx = df_year[df_year["country_name"] == "Mexico"].copy()
        if df_mx.empty:
            st.info("Sin datos para México en este año.")
        else:
            df_mx["radius"] = df_mx["is_critical_zone"].apply(
                lambda v: 90000 if v else 50000
            )

            layer_mx = pdk.Layer(
                "ScatterplotLayer",
                data=df_mx,
                get_position="[lon, lat]",
                get_fill_color="color",
                get_radius="radius",
                pickable=True,
                opacity=0.8,
            )

            view_mx = pdk.ViewState(
                latitude=23.0,
                longitude=-102.0,
                zoom=3.5,
                pitch=0,
            )

            tooltip_mx = {
                "html": (
                    "<b>{province_name}</b><br/>"
                    "Saneamiento básico: <b>{sanitation_basic_pct}%</b><br/>"
                    "Lluvia anual: <b>{precip_total_mm_year} mm</b><br/>"
                    "Tendencia de lluvias: <b>{climate_trend}</b><br/>"
                    "Zona crítica: <b>{is_critical_zone}</b>"
                ),
                "style": {"color": "white"},
            }

            deck_mx = pdk.Deck(
                layers=[layer_mx],
                initial_view_state=view_mx,
                tooltip=tooltip_mx,
            )
            st.pydeck_chart(deck_mx)

    with col_ar:
        st.markdown("### Argentina")
        df_ar = df_year[df_year["country_name"] == "Argentina"].copy()
        if df_ar.empty:
            st.info("Sin datos para Argentina en este año.")
        else:
            df_ar["radius"] = df_ar["is_critical_zone"].apply(
                lambda v: 90000 if v else 50000
            )

            layer_ar = pdk.Layer(
                "ScatterplotLayer",
                data=df_ar,
                get_position="[lon, lat]",
                get_fill_color="color",
                get_radius="radius",
                pickable=True,
                opacity=0.8,
            )

            view_ar = pdk.ViewState(
                latitude=-38.0,
                longitude=-64.0,
                zoom=3.3,
                pitch=0,
            )

            tooltip_ar = {
                "html": (
                    "<b>{province_name}</b><br/>"
                    "Saneamiento básico: <b>{sanitation_basic_pct}%</b><br/>"
                    "Lluvia anual: <b>{precip_total_mm_year} mm</b><br/>"
                    "Tendencia de lluvias: <b>{climate_trend}</b><br/>"
                    "Zona crítica: <b>{is_critical_zone}</b>"
                ),
                "style": {"color": "white"},
            }

            deck_ar = pdk.Deck(
                layers=[layer_ar],
                initial_view_state=view_ar,
                tooltip=tooltip_ar,
            )
            st.pydeck_chart(deck_ar)

    st.markdown(
        f"""
**Cómo leer los mapas (año {year_sel})**

- Cada círculo es una **provincia**.
- Si el círculo es más grande, es porque la provincia está marcada como **zona crítica**.
- El color sigue el semáforo:
  - 🟥 Rojo: zona crítica (bajo saneamiento y lluvias en descenso).  
  - 🟨 Amarillo: cumple solo una condición (bajo saneamiento o lluvias en descenso).  
  - 🟩 Verde: saneamiento adecuado y lluvias estables o en aumento.

El objetivo es ver rápido **dónde se juntan baja infraestructura de saneamiento
y un clima que se está volviendo más seco**, para priorizar inversión y ayuda humanitaria.
        """
    )
