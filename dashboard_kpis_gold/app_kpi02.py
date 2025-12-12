# app_kpi02.py
#
# KPI 2 – Movilidad forzada para conseguir agua
# % de población cuya fuente principal de agua está a más de 30 minutos,
# por país, zona (urban/rural) y año.

import streamlit as st
import pandas as pd
import pydeck as pdk
import altair as alt

from common import COUNTRY_COORDS, S3_PATH_KPI02


# =========================
# Carga de datos
# =========================
@st.cache_data
def load_kpi02() -> pd.DataFrame:
    """
    Lee el modelo Gold de kpi02 desde S3 (formato Parquet).
    """
    df = pd.read_parquet(S3_PATH_KPI02)
    return df


# =========================
# Helpers de color / texto
# =========================
def risk_color_rgb(risk_level: str):
    """Colores RGB para el mapa según el semáforo."""
    if risk_level == "green":
        return [39, 174, 96, 200]   # verde
    if risk_level == "yellow":
        return [242, 201, 76, 220]  # amarillo
    if risk_level == "red":
        return [229, 57, 53, 220]   # rojo
    return [180, 180, 180, 180]


def risk_hex(risk_level: str) -> str:
    """Colores HEX para Altair (barras)."""
    mapping = {
        "green": "#27AE60",
        "yellow": "#F2C94C",
        "red": "#E53935",
    }
    return mapping.get(risk_level, "#B4B4B4")


def prepare_base_df() -> pd.DataFrame:
    """Carga y enriquece el dataframe base para el KPI 2."""
    df = load_kpi02().copy()

    # países con georreferenciados
    df = df[df["country_name"].isin(COUNTRY_COORDS.keys())].copy()

    # Coordenadas para el mapa
    df["lat"] = df["country_name"].map(lambda c: COUNTRY_COORDS[c][0])
    df["lon"] = df["country_name"].map(lambda c: COUNTRY_COORDS[c][1])

    # Color RGB según semáforo
    df["color"] = df["risk_level"].apply(risk_color_rgb)

    # Etiqueta amigable para barras / bullets
    df["country_zone"] = df["country_name"] + " – " + df["residence_type_desc"]

    return df


def build_interpretation(row: pd.Series) -> str:
    """
    Construye una frase corta que traduzca el % en algo entendible.
    """
    pct = row["pct_over_30min"]
    risk = row["risk_level"]

    if pct <= 0:
        base = "Prácticamente nadie tarda más de 30 minutos en conseguir agua."
    else:
        approx_n = max(1, int(round(100 / pct)))
        if approx_n <= 2:
            base = "Casi todas las personas tardan más de 30 minutos en conseguir agua."
        else:
            base = (
                f"Aproximadamente 1 de cada {approx_n} personas "
                f"tarda más de 30 minutos en conseguir agua."
            )

    if risk == "green":
        suf = "El problema existe pero está relativamente acotado."
    elif risk == "yellow":
        suf = (
            "La movilidad forzada es relevante y afecta a una fracción importante de la población."
        )
    elif risk == "red":
        suf = (
            "Situación crítica: la movilidad forzada afecta a una parte muy grande de la población."
        )
    else:
        suf = ""

    return f"{base} {suf}".strip()


def risk_short_text(risk_level: str) -> str:
    """Texto corto para usar en el ejemplo de interpretación."""
    mapping = {
        "green": "el problema existe pero está relativamente acotado (riesgo bajo).",
        "yellow": "la movilidad forzada es relevante y requiere atención.",
        "red": "es una situación crítica y debería ser una prioridad de política pública.",
    }
    return mapping.get(risk_level, "")


# =========================
# Layout principal
# =========================
def layout_kpi02():
    # =========================
    # Estilos y HERO 
    # =========================
    st.markdown(
        """
        <style>
        .stApp {
            --primary-color: #4DA3FF;
        }

        /* Hero del KPI (igual estilo que KPI 1) */
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

        /* Tarjeta de métricas (parte derecha arriba) */
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

        .kpi-metric-value.highlight-red {
            color: #F87171;
        }

        .kpi-metric-value.highlight-yellow {
            color: #FBBF24;
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

        .kpi-section-title {
            font-size: 1.4rem;
            font-weight: 700;
            margin-top: 0.3rem;
            margin-bottom: 0.6rem;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # HERO
    st.markdown(
        """
        <div class="kpi-hero">
          <div class="kpi-hero-left">
            <div class="kpi-hero-title">
              ¿Quién tiene que caminar lejos para conseguir agua?
            </div>
            <div class="kpi-hero-subtitle">
              Medimos en qué países y zonas (urbanas y rurales) la gente
              tarda más de 30 minutos en llegar a su fuente principal de agua.
            </div>
            <div class="kpi-hero-tags">
              <span class="kpi-tag green">Acceso al agua</span>
              <span class="kpi-tag blue">Tiempo de trayecto</span>
              <span class="kpi-tag purple">Movilidad forzada</span>
            </div>
            <div class="kpi-hero-small">
              Buscamos los lugares donde conseguir agua implica una
              caminata larga que se siente en la vida diaria. 
              Abajo verás el indicador, el semáforo y cómo ha cambiado con el tiempo.
            </div>
          </div>

          <div class="kpi-hero-right">
            <div style="font-weight: 650; margin-bottom: 0.4rem;">
              Semáforo de movilidad forzada
            </div>
            <div class="kpi-legend-item">🟢 <b>Verde</b>: la mayoría de los hogares tiene el agua relativamente cerca.</div>
            <div class="kpi-legend-item">🟡 <b>Amarillo</b>: la caminata larga ya se siente en la vida diaria.</div>
            <div class="kpi-legend-item">🔴 <b>Rojo</b>: al menos 1 de cada 5 personas tarda más de 30 minutos.</div>
            <div style="margin-top: 0.5rem;" class="kpi-hero-small">
              Donde se prende el <b>rojo</b> es donde la movilidad forzada por agua
              debería ser una prioridad de política pública.
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # =========================
    # Datos y filtros 
    # =========================
    df = prepare_base_df()

    if df.empty:
        st.warning("No hay datos disponibles para este indicador.")
        return

    years = sorted(df["year"].unique())
    current_year = st.selectbox(
        "Año para el semáforo, el mapa y el ranking",
        options=years,
        index=len(years) - 1,
    )

    area_option = st.radio(
        "Zona",
        options=["Ambas", "Urbana", "Rural"],
        horizontal=True,
    )

    if area_option == "Urbana":
        df_year = df[(df["year"] == current_year) & (df["residence_type_desc"] == "urban")]
    elif area_option == "Rural":
        df_year = df[(df["year"] == current_year) & (df["residence_type_desc"] == "rural")]
    else:
        df_year = df[df["year"] == current_year]

    if df_year.empty:
        st.warning("No hay datos para el año y filtros seleccionados.")
        return

    # =========================
    # 1) ¿Qué mide? 
    # =========================
    top_left, top_right = st.columns([2.1, 1.2])

    with top_left:
        st.markdown(
            """
            ### 1. 👀 ¿Qué mide este indicador?

            - Mide el **% de población** cuya fuente principal de agua
              está a más de **30 minutos** de distancia.
            - El cálculo se hace por **país** y por **tipo de zona**
              (urbana / rural), para cada año.
            - Valores altos significan que muchas personas tienen que
              caminar lejos solo para conseguir agua.

            El año y la zona que elijas arriba se aplican al **mapa** y
            al **ranking**.  
            La sección de evolución usa todos los años disponibles
            para cada país y zona.
            """
        )

        with st.expander("Ver definición técnica del indicador"):
            st.markdown(
                """
                *Indicador técnico:*  
                Porcentaje de población cuya **fuente principal de agua** está a más de **30 minutos**
                de distancia (ida y vuelta) desde la vivienda, por país, año y tipo de zona
                (urbana / rural).

                *Cálculo básico:*

                - **Numerador**: personas que reportan un tiempo &gt; 30 minutos hasta su fuente principal de agua.  
                - **Denominador**: total de personas encuestadas con dato de tiempo de acceso al agua en esa zona.  
                - **Indicador** = (numerador / denominador) × 100.

                *Semáforo de movilidad forzada (según % población &gt; 30 min):*

                - 🟢 **Verde (0–5%)**: problema presente pero acotado.  
                - 🟡 **Amarillo (&gt;5–20%)**: problema relevante; afecta a una fracción importante de hogares.  
                - 🔴 **Rojo (&gt;20%)**: situación crítica; al menos 1 de cada 5 personas tarda más de 30 minutos.
                """
            )

    with top_right:
        df_positive = df_year[df_year["pct_over_30min"] > 0]

        # Zonas = combinación país + tipo de residencia (urban/rural)
        total_zonas = (
            df_year[["country_name", "residence_type_desc"]]
            .drop_duplicates()
            .shape[0]
        )
        avg_pct = df_year["pct_over_30min"].mean() if len(df_year) > 0 else 0.0
        high_risk = (df_year["risk_level"] == "red").sum()
        medium_risk = (df_year["risk_level"] == "yellow").sum()

        if not df_positive.empty:
            worst = df_positive.sort_values("pct_over_30min", ascending=False).iloc[0]
        else:
            worst = df_year.sort_values("pct_over_30min", ascending=False).iloc[0]

        worst_pct = worst["pct_over_30min"]
        worst_name = f"{worst['country_name']} – {worst['residence_type_desc']}"
        zona_risk = worst["risk_level"]

        zona_value_class = "kpi-metric-value"
        if zona_risk == "red":
            zona_value_class += " highlight-red"
        elif zona_risk == "yellow":
            zona_value_class += " highlight-yellow"

        st.markdown(
            f"""
            <div class="kpi-metrics-card">
              <div class="kpi-metric">
                <div class="kpi-metric-label">Zonas analizadas en {current_year}</div>
                <div class="kpi-metric-value">{total_zonas}</div>
              </div>
              <div class="kpi-metric">
                <div class="kpi-metric-label">Promedio de población &gt;30 min</div>
                <div class="kpi-metric-value">{avg_pct:.1f}%</div>
              </div>
              <div class="kpi-metric">
                <div class="kpi-metric-label">Zonas en rojo (🔴)</div>
                <div class="kpi-metric-value highlight-red">{high_risk}</div>
              </div>
              <div class="kpi-metric">
                <div class="kpi-metric-label">Zona más crítica</div>
                <div class="{zona_value_class}">{worst_pct:.1f}%</div>
                <div class="kpi-metric-pill">↑ {worst_name}</div>
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("---")

    # =========================
    # 2) Mapa
    # =========================
    st.subheader(f"2. 📍 Zonas donde se camina lejos para conseguir agua en {current_year}")

    df_map = df_year.copy()

    pct_max = df_map["pct_over_30min"].max()
    if pct_max and pct_max > 0:
        min_radius = 20000
        max_radius = 80000
        df_map["radius"] = min_radius + (df_map["pct_over_30min"] / pct_max) * (
            max_radius - min_radius
        )
    else:
        df_map["radius"] = 25000

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df_map,
        get_position="[lon, lat]",
        get_fill_color="color",
        get_radius="radius",
        pickable=True,
        opacity=0.8,
    )

    view_state = pdk.ViewState(latitude=0, longitude=-70, zoom=2, pitch=0)

    tooltip = {
        "html": (
            "<b>{country_name}</b> – {residence_type_desc}<br/>"
            "% población &gt;30 min: <b>{pct_over_30min}</b><br/>"
            "Riesgo: <b>{risk_level}</b><br/>"
            "Tendencia: <b>{mobility_trend}</b>"
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
        "Cada punto representa un país y zona (urbana/rural). "
        "Cuanto más grande y más hacia rojo el punto, mayor es la proporción de personas "
        "que debe caminar más de 30 minutos para conseguir agua."
    )

    # =========================
    # 3) Ranking
    # =========================
    st.subheader(f"3. 📊 Ranking de países y zonas que más caminan por agua en {current_year}")

    df_rank = df_year[df_year["pct_over_30min"] > 0].copy()

    if df_rank.empty:
        st.info(
            "En el año y filtros seleccionados, todos los valores son 0%. "
            "No se muestra el ranking porque no hay movilidad forzada registrada."
        )
    else:
        df_rank = df_rank.sort_values("pct_over_30min", ascending=True)
        df_rank["risk_hex"] = df_rank["risk_level"].apply(risk_hex)

        chart = (
            alt.Chart(df_rank)
            .mark_bar()
            .encode(
                y=alt.Y(
                    "country_zone:N",
                    sort=None,
                    title="País – zona",
                    axis=alt.Axis(labelLimit=0),
                ),
                x=alt.X(
                    "pct_over_30min:Q",
                    title="% población >30 minutos",
                    axis=alt.Axis(format=".1f"),
                ),
                color=alt.Color(
                    "risk_level:N",
                    scale=alt.Scale(
                        domain=["green", "yellow", "red"],
                        range=["#27AE60", "#F2C94C", "#E53935"],
                    ),
                    legend=alt.Legend(title="Semáforo de movilidad"),
                ),
                tooltip=[
                    alt.Tooltip("country_name:N", title="País"),
                    alt.Tooltip("residence_type_desc:N", title="Zona"),
                    alt.Tooltip("pct_over_30min:Q", title="% >30 min", format=".2f"),
                    alt.Tooltip(
                        "delta_pct_over_30min_pp:Q",
                        title="Δ vs año previo (pp)",
                        format=".2f",
                    ),
                    alt.Tooltip("mobility_trend:N", title="Tendencia"),
                ],
            )
            .properties(height=alt.Step(26))
        )

        st.altair_chart(chart, use_container_width=True)

        worst = df_rank.sort_values("pct_over_30min", ascending=False).head(10)

        if not worst.empty:
            example = worst.iloc[0]
            pct = example["pct_over_30min"]
            risk = example["risk_level"]
            approx_n = max(1, int(round(100 / pct))) if pct > 0 else None

            st.markdown("### 🧩 ¿Cómo leer este ranking?")

            if approx_n:
                st.markdown(
                    f"""
En **{example['country_name']} – {example['residence_type_desc']}**, cerca del **{pct:.2f}%** de la población
tarda más de 30 minutos en conseguir agua.  
Eso equivale aproximadamente a **1 de cada {approx_n} personas** caminando más de media hora para llegar a su fuente principal.  

El semáforo está en **{risk}**, lo que indica que {risk_short_text(risk)}
                    """
                )
            else:
                st.markdown(
                    """
Este ranking ordena los países y zonas desde el menor al mayor porcentaje de población
que tarda más de 30 minutos en conseguir agua.  
Cuando un caso aparece en amarillo o rojo, significa que la movilidad forzada es un problema
relevante y potencialmente grave en ese territorio.
                    """
                )

    # =========================
    # 4) Evolución
    # =========================
    st.subheader("4. ⏱️ Evolución en el tiempo")

    col1, col2 = st.columns(2)

    with col1:
        country_sel = st.selectbox(
            "País",
            options=sorted(df["country_name"].unique()),
        )

    with col2:
        area_trend = st.radio(
            "Zona para la serie de tiempo",
            options=["urban", "rural"],
            horizontal=True,
        )

    df_trend = (
        df[
            (df["country_name"] == country_sel)
            & (df["residence_type_desc"] == area_trend)
        ]
        .sort_values("year")
        .copy()
    )

    if df_trend.empty or df_trend["pct_over_30min"].fillna(0).sum() == 0:
        st.info(
            "Para esta combinación de país y zona no hay datos con valores mayores a cero, "
            "por lo que la serie de tiempo no se muestra."
        )
        return

    line = (
        alt.Chart(df_trend)
        .mark_line(point=True)
        .encode(
            x=alt.X("year:O", title="Año"),
            y=alt.Y("pct_over_30min:Q", title="% población >30 minutos"),
            color=alt.value("#1f77b4"),
            tooltip=[
                alt.Tooltip("year:O", title="Año"),
                alt.Tooltip("pct_over_30min:Q", title="% >30 min", format=".2f"),
                alt.Tooltip("risk_level:N", title="Semáforo"),
                alt.Tooltip("mobility_trend:N", title="Tendencia"),
            ],
        )
        .properties(height=300)
    )

    st.altair_chart(line, use_container_width=True)

    last_row = df_trend.iloc[-1]
    st.markdown(
        f"""
En **{last_row['year']}**, en **{country_sel} – {area_trend}**:

- El **{last_row['pct_over_30min']:.2f}%** de la población tarda más de 30 minutos en llegar a su fuente principal de agua.
- Semáforo: **{last_row['risk_level']}**.
- Tendencia en el período observado: **{last_row['mobility_trend']}**.
        """
    )
