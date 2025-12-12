# app_kpi01.py
#
# Módulo de KPI 01 – Clima vs acceso a agua segura.
# Se usa importado desde app.py llamando a layout_kpi01().

import streamlit as st
import pandas as pd
import altair as alt

from common import load_kpi01


# ======================================================================
# Selector de años 
# ======================================================================
def year_selector_main(df: pd.DataFrame, label: str) -> pd.DataFrame:
    """
    Muestra un control de años en el cuerpo principal y filtra el DataFrame.
    Usa un slider de rango (año inicial, año final).
    """
    if "year" not in df.columns:
        return df

    years = sorted(df["year"].dropna().unique())
    if not years:
        return df

    min_year, max_year = int(min(years)), int(max(years))

    year_start, year_end = st.slider(
        label,
        min_value=min_year,
        max_value=max_year,
        value=(min_year, max_year),
        step=1,
        key="kpi01_year_range",
    )

    mask = (df["year"] >= year_start) & (df["year"] <= year_end)
    return df[mask]


def build_summary_kpi01(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega por país + tipo de residencia y se queda con una fila por zona."""
    summary = (
        df.groupby(["country_name", "residence_type_desc"], as_index=False)
        .agg(
            corr_precip_vs_water=("corr_precip_vs_water", "first"),
            corr_abs_value=("corr_abs_value", "first"),
            risk_level=("risk_level", "first"),
            impact_direction=("impact_direction", "first"),
            years_observed=("years_observed", "first"),
        )
    )
    summary["area"] = summary["country_name"] + " – " + summary["residence_type_desc"]
    return summary


# ======================================================================
# Semáforo tipo barra + lectura rápida 
# ======================================================================
def chart_kpi01_semaforo(summary: pd.DataFrame):
    """
    Barra horizontal tipo semáforo por zona (|correlación|),
    """

    st.markdown(
        '<div class="kpi-section-title">2. 🚦 Semáforo de riesgo: '
        '¿dónde el clima pesa más sobre el acceso al agua?</div>',
        unsafe_allow_html=True,
    )

    if summary.empty:
        st.info("No hay combinaciones para mostrar en el semáforo con los filtros actuales.")
        return

    risk_labels = {
        "green": "Bajo, relación débil",
        "yellow": "Medio, relación moderada",
        "red": "Alto, relación fuerte",
    }

    summary = summary.copy()
    summary["risk_label"] = summary["risk_level"].map(risk_labels).fillna(summary["risk_level"])

    # Ordenar de menor a mayor intensidad
    summary = summary.sort_values("corr_abs_value", ascending=True)

    risk_domain = ["green", "yellow", "red"]
    risk_colors = ["#27AE60", "#F2C94C", "#E53935"]

    left_col, right_col = st.columns([2.3, 1.7])

    with left_col:
        base = alt.Chart(summary).encode(
            y=alt.Y("area:N", title="País – zona", sort=list(summary["area"])),
            x=alt.X(
                "corr_abs_value:Q",
                title="Intensidad de la relación |clima–agua|",
                scale=alt.Scale(domain=[0, 1]),
            ),
            color=alt.Color(
                "risk_level:N",
                title="Semáforo",
                scale=alt.Scale(domain=risk_domain, range=risk_colors),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("country_name:N", title="País"),
                alt.Tooltip("residence_type_desc:N", title="Zona"),
                alt.Tooltip("corr_precip_vs_water:Q", title="Indicador (0–1)", format=".2f"),
                alt.Tooltip("risk_label:N", title="Nivel de riesgo"),
                alt.Tooltip("years_observed:Q", title="Años analizados", format=".0f"),
            ],
        )

        bars = base.mark_bar(size=40)
        text = base.mark_text(
            align="left",
            baseline="middle",
            dx=5,
            color="white",
        ).encode(
            text=alt.Text("corr_precip_vs_water:Q", format=".2f")
        )

        chart = (bars + text).properties(height=260)
        st.altair_chart(chart, use_container_width=True)

        st.caption(
            "Cada barra muestra qué tanto dependen las personas de la lluvia para mantener "
            "su acceso al agua segura. Verde ≈ relación débil; rojo ≈ el clima pesa "
            "fuertemente sobre el acceso al agua en esa zona."
        )

    with right_col:
        st.markdown("**Cómo leer este semáforo:**")
        st.markdown(
            """
            - Cada barra es un **país – zona** (urbano o rural).  
            - Mientras más larga la barra, **más sensible** es el acceso al agua frente a cambios en la lluvia.  
            - El color indica el **nivel de riesgo**:
              - 🟢 el clima casi no cambia el acceso al agua.  
              - 🟡 el clima ya empieza a sentirse.  
              - 🔴 el clima está fuertemente ligado al acceso al agua.
            """
        )

        st.markdown("**Zonas más sensibles al clima:**")
        bullets = []
        top_rows = summary.sort_values("corr_abs_value", ascending=False).head(3)
        for _, row in top_rows.iterrows():
            risk_text = risk_labels.get(row["risk_level"], row["risk_level"])
            bullets.append(
                f"- 💧 **{row['country_name']} – {row['residence_type_desc']}**: "
                f"indicador {row['corr_precip_vs_water']:.2f} "
                f"({risk_text}, impacto {row['impact_direction']})."
            )
        st.markdown("\n".join(bullets))


# ======================================================================
# Historia detallada por zona (parte baja de la Z)
# ======================================================================
def story_kpi01_zone(df: pd.DataFrame, summary: pd.DataFrame):
    """Narrativa + gráficos detalle para una zona concreta."""
    st.subheader("3. 📖 Historia por zona: clima vs agua en un territorio concreto")

    if summary.empty:
        st.info("No hay zonas para narrar con los filtros actuales.")
        return

    areas = list(summary["area"])
    selected_area = st.selectbox(
        "Elige la zona que quieres explorar",
        options=areas,
        key="kpi01_zone",
    )

    row = summary[summary["area"] == selected_area].iloc[0]
    zone_df = df
    zone_df = zone_df[
        (zone_df["country_name"] == row["country_name"])
        & (zone_df["residence_type_desc"] == row["residence_type_desc"])
    ].sort_values("year")

    corr = row["corr_precip_vs_water"]
    risk = row["risk_level"]
    years = row["years_observed"]

    risk_icon_map = {
        "green": "🟢 Bajo",
        "yellow": "🟡 Medio",
        "red": "🔴 Alto",
    }
    risk_display = risk_icon_map.get(risk, "⚪️ Desconocido")

    # Intensidad según el valor absoluto
    if abs(corr) < 0.3:
        intensity_text = "baja: el clima casi no mueve el acceso al agua."
    elif abs(corr) < 0.6:
        intensity_text = "media: el clima empieza a sentirse en el acceso al agua."
    else:
        intensity_text = "alta: el clima está muy ligado al acceso al agua."

    # Interpretación según el signo
    if corr > 0:
        direction_text = (
            "los años con *más lluvia* tienden a coincidir con *más avance* en acceso a agua segura, "
            "y los años *más secos* con *menos avance o incluso retrocesos*."
        )
    elif corr < 0:
        direction_text = (
            "los años con *más lluvia* tienden a coincidir con *menos avance* en acceso a agua segura, "
            "y los años *más secos* con *más avance*."
        )
    else:
        direction_text = (
            "no se observa una dirección clara: los cambios de precipitación no se traducen "
            "de forma consistente en cambios de acceso al agua segura."
        )

    col1, col2 = st.columns([2, 1])

    with col1:
        st.markdown(
            f"""
            ### 🌍 {row['country_name']} – zona {row['residence_type_desc']}

            En esta zona medimos qué tanto el acceso a agua segura depende de la lluvia.

            Eso significa que la sensibilidad es **{intensity_text}**

            Para este país-zona: *{direction_text}*
            """
        )

    with col2:
        st.metric("Indicador clima–agua", f"{corr:.2f}")
        st.metric("Años analizados", int(years))
        st.metric("Nivel de riesgo (semáforo)", risk_display)

    # -------- Línea de acceso a agua segura --------
    st.markdown("#### 🚰 ¿Cómo ha cambiado el acceso a agua segura en el tiempo?")

    safe_base = (
        alt.Chart(zone_df)
        .encode(
            x=alt.X("year:O", title="Año"),
            y=alt.Y(
                "safe_water_pct:Q",
                title="% población con agua segura",
                scale=alt.Scale(zero=False),
            ),
            tooltip=[
                alt.Tooltip("year:O", title="Año"),
                alt.Tooltip("safe_water_pct:Q", title="% agua segura", format=".2f"),
            ],
        )
    )

    safe_line = safe_base.mark_line(point=False, size=3, color="#00A8E8")
    safe_points = safe_base.mark_circle(size=80, color="#00A8E8")
    safe_text = safe_base.mark_text(
        dy=-10,
        color="#00A8E8",
        fontSize=11,
    ).encode(text=alt.Text("safe_water_pct:Q", format=".1f"))

    line_safe = (safe_line + safe_points + safe_text).properties(height=320)
    st.altair_chart(line_safe, use_container_width=True)

    st.caption(
        "Cada punto muestra el porcentaje de población con agua segura en un año. "
        "La escala está ajustada para resaltar cambios pequeños (1–2 puntos porcentuales) "
        "que, en número de personas, pueden representar miles de hogares."
    )

    # -------- Línea de precipitación --------
    st.markdown("#### 🌧️ ¿Y cómo se ha movido la lluvia en esos mismos años?")

    precip_base = (
        alt.Chart(zone_df)
        .encode(
            x=alt.X("year:O", title="Año"),
            y=alt.Y(
                "precip_total_mm_year:Q",
                title="Precipitación total anual (mm)",
                scale=alt.Scale(zero=False),
            ),
            tooltip=[
                alt.Tooltip("year:O", title="Año"),
                alt.Tooltip("precip_total_mm_year:Q", title="mm de lluvia", format=".0f"),
            ],
        )
    )

    precip_line = precip_base.mark_line(point=False, size=3, color="#6C5CE7")
    precip_points = precip_base.mark_circle(size=80, color="#6C5CE7")
    precip_text = precip_base.mark_text(
        dy=-10,
        color="#6C5CE7",
        fontSize=11,
    ).encode(text=alt.Text("precip_total_mm_year:Q", format=".0f"))

    line_rain = (precip_line + precip_points + precip_text).properties(height=320)
    st.altair_chart(line_rain, use_container_width=True)

    st.caption(
        "Aquí vemos la lluvia total por año para identificar mejor los años excepcionalmente secos o lluviosos."
    )

    # -------- Dispersión de deltas --------
    st.markdown("#### 🔁 Cada año, ¿se mueven juntos clima y acceso al agua?")
    zone_df = zone_df.copy()
    zone_df["label_year"] = zone_df["year"].astype(str)

    # Clasificación de cada año según cómo se mueven lluvia y agua
    def _classify_pattern(row):
        dp = row["delta_precip_mm"]
        dw = row["delta_safe_water_pp"]
        # Cualquier caso raro o sin datos lo tratamos como cambios pequeños
        if pd.isna(dp) or pd.isna(dw):
            return "Cambios pequeños"
        if abs(dp) < 1e-6 and abs(dw) < 1e-6:
            return "Cambios pequeños"
        if dp > 0 and dw > 0:
            return "Más lluvia, mejora agua"
        if dp < 0 and dw > 0:
            return "Menos lluvia, mejora agua"
        if dp > 0 and dw < 0:
            return "Más lluvia, baja agua"
        if dp < 0 and dw < 0:
            return "Menos lluvia, baja agua"
        return "Cambios pequeños"

    zone_df["pattern"] = zone_df.apply(_classify_pattern, axis=1)

    pattern_domain = [
        "Más lluvia, mejora agua",
        "Menos lluvia, mejora agua",
        "Más lluvia, baja agua",
        "Menos lluvia, baja agua",
        "Cambios pequeños",
    ]
    pattern_colors = [
        "#2ECC71",  # verde
        "#1ABC9C",  # verde-azulado
        "#F39C12",  # naranja
        "#E74C3C",  # rojo
        "#95A5A6",  # gris
    ]

    base = alt.Chart(zone_df).encode(
        x=alt.X(
            "delta_precip_mm:Q",
            title="Δ precipitación anual (mm)",
        ),
        y=alt.Y(
            "delta_safe_water_pp:Q",
            title="Δ agua segura (p.p.)",
        ),
        color=alt.Color(
            "pattern:N",
            title="Tipo de año",
            scale=alt.Scale(domain=pattern_domain, range=pattern_colors),
            legend=alt.Legend(orient="bottom"),
        ),
        tooltip=[
            alt.Tooltip("year:O", title="Año"),
            alt.Tooltip("delta_precip_mm:Q", title="Δ precipitación (mm)", format=".0f"),
            alt.Tooltip("delta_safe_water_pp:Q", title="Δ agua segura (p.p.)", format=".2f"),
            alt.Tooltip("pattern:N", title="Tipo de año"),
        ],
    )

    points = base.mark_circle(size=150, opacity=0.9)
    text = base.mark_text(
        align="center",
        baseline="bottom",
        dy=-8,
        color="white",
        fontSize=11,
    ).encode(text="label_year:N")
    x_rule = alt.Chart(zone_df).mark_rule(
        strokeDash=[4, 4], color="#BBBBBB"
    ).encode(x=alt.datum(0))
    y_rule = alt.Chart(zone_df).mark_rule(
        strokeDash=[4, 4], color="#BBBBBB"
    ).encode(y=alt.datum(0))

    scatter = (points + text + x_rule + y_rule).properties(height=320)
    st.altair_chart(scatter, use_container_width=True)

    st.caption(
        "Los colores indican el tipo de año: verdes cuando mejora el acceso al agua, "
        "naranja/rojo cuando empeora y gris cuando los cambios son muy pequeños. "
        "Horizontalmente ves si llovió más (derecha) o menos (izquierda); verticalmente, "
        "si la cobertura de agua segura subió (arriba) o bajó (abajo)."
    )

    # Resumen por año 
    if not zone_df.empty:
        st.markdown("**Resumen año por año:**")
        bullets = []
        for _, r in zone_df.sort_values("year").iterrows():
            year_label = int(r["year"]) if not pd.isna(r["year"]) else "Sin año"
            dp = r["delta_precip_mm"]
            dw = r["delta_safe_water_pp"]
            dp_txt = "NA" if pd.isna(dp) else f"{dp:.0f} mm"
            dw_txt = "NA" if pd.isna(dw) else f"{dw:.2f} p.p."
            bullets.append(
                f"- **{year_label}**: {r['pattern']} "
                f"(Δ lluvia: {dp_txt}, Δ agua segura: {dw_txt})"
            )
        st.markdown("\n".join(bullets))


# ======================================================================
# Layout principal del KPI 1 
# ======================================================================
def layout_kpi01():
    """
    Plantilla principal del KPI 1.
    Se llama desde app.py dentro de una tab.
    """

    # Estilos globales 
    st.markdown(
        """
        <style>
        .stApp {
            --primary-color: #4DA3FF;
        }

        /* Hero del KPI */
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

        /* Título de sección (semáforo) */
        .kpi-section-title {
            font-size: 1.4rem;
            font-weight: 700;
            margin-top: 0.3rem;
            margin-bottom: 0.6rem;
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
        </style>
        """,
        unsafe_allow_html=True,
    )

    # ------------------------ HERO VISUAL ------------------------
    st.markdown(
        """
        <div class="kpi-hero">
          <div class="kpi-hero-left">
            <div class="kpi-hero-title">
              ¿Qué tanto afecta la lluvia al acceso al agua segura?
            </div>
            <div class="kpi-hero-subtitle">
              Queremos saber en qué países y zonas (urbanas y rurales) el acceso a agua segura
              se ve más afectado cuando cambia la lluvia.
            </div>
            <div class="kpi-hero-tags">
              <span class="kpi-tag green">Acceso al agua</span>
              <span class="kpi-tag blue">Clima</span>
              <span class="kpi-tag purple">Riesgo hídrico</span>
            </div>
            <div class="kpi-hero-small">
              Abajo encontrarás cómo está construido
              el indicador y cómo leerlo.
            </div>
          </div>

          <div class="kpi-hero-right">
            <div style="font-weight: 650; margin-bottom: 0.4rem;">
              Semáforo de riesgo climático del agua
            </div>
            <div class="kpi-legend-item">🟢 <b>Verde</b>: el clima casi no afecta el acceso al agua.</div>
            <div class="kpi-legend-item">🟡 <b>Amarillo</b>: el clima ya empieza a sentirse.</div>
            <div class="kpi-legend-item">🔴 <b>Rojo</b>: el clima pega fuerte al acceso al agua segura.</div>
            <div style="margin-top: 0.5rem;" class="kpi-hero-small">
               <b>Donde se prende el rojo</b>, ahí es donde más preocupa
              que cambie la lluvia.
            </div>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ------------------------ DATA + CONTROLES  ------------------------
    df = load_kpi01()
    df = year_selector_main(df, "Periodo de análisis (años)")
    summary = build_summary_kpi01(df)

    top_left, top_right = st.columns([2.1, 1.2])

    with top_left:
        st.markdown(
            """
            ### 1. 👀 ¿Qué mide este indicador?

            - Es un número entre **0** y **1** que mide la fuerza de la relación entre
              lluvia y acceso a agua segura en cada país y zona (urbana / rural).
            - Valores cercanos a **1** indican alta dependencia del clima;
              valores cercanos a **0**, poca o nula dependencia.

            El rango de años que elijas arriba solo modifica las **gráficas de tendencia**
            de abajo; el valor del indicador se calcula con todos los años disponibles
            para cada zona.
            """
        )

        with st.expander("Ver definición técnica del indicador"):
            st.markdown(
                """
                *Indicador técnico:*  
                Correlación entre la variación anual de precipitación y la variación anual de cobertura
                de agua segura, por país y tipo de área (urbano/rural).

                *Semáforo de riesgo (intensidad de la correlación)*:

                - 🟢 *Verde (|r| < 0.3)*: la relación entre clima y acceso al agua es débil.  
                - 🟡 *Amarillo (0.3 ≤ |r| < 0.6)*: relación moderada; el clima empieza a sentirse.  
                - 🔴 *Rojo (|r| ≥ 0.6)*: relación fuerte; el clima está pegando directo al acceso al agua segura.
                """
            )

    with top_right:
        if summary.empty:
            st.warning("No hay datos disponibles para el periodo seleccionado.")
        else:
            total_zonas = len(summary)
            high_risk = (summary["risk_level"] == "red").sum()
            medium_risk = (summary["risk_level"] == "yellow").sum()
            max_row = summary.loc[summary["corr_abs_value"].idxmax()]
            zona_nombre = f"{max_row['country_name']} – {max_row['residence_type_desc']}"

            # Clase para colorear la zona más sensible según su nivel de riesgo
            zona_risk = max_row["risk_level"]
            zona_value_class = "kpi-metric-value"
            if zona_risk == "red":
                zona_value_class += " highlight-red"
            elif zona_risk == "yellow":
                zona_value_class += " highlight-yellow"

            st.markdown(
                f"""
                <div class="kpi-metrics-card">
                  <div class="kpi-metric">
                    <div class="kpi-metric-label">Zonas analizadas</div>
                    <div class="kpi-metric-value">{total_zonas}</div>
                  </div>
                  <div class="kpi-metric">
                    <div class="kpi-metric-label">Zonas con riesgo alto (🔴)</div>
                    <div class="kpi-metric-value highlight-red">{high_risk}</div>
                  </div>
                  <div class="kpi-metric">
                    <div class="kpi-metric-label">Zonas con riesgo medio (🟡)</div>
                    <div class="kpi-metric-value highlight-yellow">{medium_risk}</div>
                  </div>
                  <div class="kpi-metric">
                    <div class="kpi-metric-label">Zona más sensible al clima</div>
                    <div class="{zona_value_class}">{max_row['corr_precip_vs_water']:.2f}</div>
                    <div class="kpi-metric-pill">↑ {zona_nombre}</div>
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    st.markdown("---")
    chart_kpi01_semaforo(summary)
    st.markdown("---")
    story_kpi01_zone(df, summary)
