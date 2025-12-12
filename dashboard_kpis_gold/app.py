# app.py
# Dashboard principal de KPIs – Huella hídrica en América Latina

import streamlit as st

# KPIs ya implementados
from app_kpi01 import layout_kpi01
from app_kpi02 import layout_kpi02
from app_kpi03 import layout_kpi03
from app_kpi04 import layout_kpi04
from app_kpi05 import layout_kpi05
from app_kpi06 import layout_kpi06
from app_kpi07 import layout_kpi07

# ---------------------------------------------------------------------
# Configuración básica de la app
# ---------------------------------------------------------------------
st.set_page_config(
    page_title="Huella hídrica en América Latina: Indicadores",
    layout="wide",
)

# Imagen de fondo para el hero
HERO_BG_URL = (
    "https://www.gaceta.unam.mx/wp-content/uploads/2022/06/"
    "220602-Com2-des-f1-Premio-Rotopals-Fundacion-UNAM.jpg"
)

# ---------------------------------------------------------------------
# CSS global
# ---------------------------------------------------------------------
CSS = f"""
<style>
.stApp {{
    background: radial-gradient(
        circle at top left,
        #115A8C 0%,
        #074067 40%,
        #020818 100%
    );
    color: #EAF6FF;
}}
.block-container {{
    max-width: 1400px;
    padding-left: 2rem;
    padding-right: 2rem;
}}
:root {{
    --hh-azul-oscuro: #041826;
    --hh-azul-claro: #4DA3FF;
    --hh-azul-medio: #0099CC;
    --hh-turquesa: #00C2B2;
    --hh-texto-claro: #EAF6FF;
}}
h1, h2, h3, h4, h5, h6 {{
    color: var(--hh-texto-claro);
}}

/* ---------- Tabs personalizados ---------- */

.stTabs [data-baseweb="tab-list"] {{
    gap: 0.5rem;
    border-bottom: none;
    padding: 0.35rem 0.4rem;
    background: radial-gradient(circle at top,
        rgba(6, 28, 48, 0.95),
        rgba(2, 8, 23, 0.95));
    border-radius: 999px;
    box-shadow: 0 10px 30px rgba(0, 0, 0, 0.35);
}}

.stTabs [data-baseweb="tab"] {{
    font-weight: 600;
    color: #A8C4D8;
    border-radius: 999px;
    padding: 0.35rem 1.1rem;
    background: transparent;
    transition:
        background 0.2s ease-out,
        color 0.2s ease-out,
        transform 0.15s ease-out,
        box-shadow 0.15s ease-out;
}}

.stTabs [data-baseweb="tab"]:hover {{
    background: rgba(77, 163, 255, 0.12);
    color: #EAF6FF;
    transform: translateY(-1px);
}}

.stTabs [data-baseweb="tab"][aria-selected="true"] {{
    background: linear-gradient(120deg,
        var(--hh-azul-claro),
        #00C2B2);
    color: #02101E;
    box-shadow: 0 0 18px rgba(77, 163, 255, 0.55);
}}

/* ---------- Hero ---------- */

.hero-wrapper {{
    margin-top: 1.5rem;
    border-radius: 22px;
    overflow: hidden;
    position: relative;
    background-image: url('{HERO_BG_URL}');
    background-size: cover;
    background-position: center;
    min-height: 340px;
    box-shadow: 0 22px 45px rgba(0, 0, 0, 0.45);
}}

/* Fondo del overlay claro para ver la imagen */
.hero-overlay {{
    background: linear-gradient(135deg,
        rgba(2, 8, 23, 0.65),
        rgba(0, 72, 124, 0.70));
    padding: 2.8rem 3.2rem 2.6rem 3.2rem;
    color: var(--hh-texto-claro);
    display: flex;
    flex-direction: column;
    align-items: center;
    text-align: center;
    max-width: 1000px;
    margin: 0 auto;
    backdrop-filter: blur(2px);
}}

/* Animaciones */

@keyframes fadeInUp {{
    0%   {{ opacity: 0; transform: translateY(18px); }}
    100% {{ opacity: 1; transform: translateY(0); }}
}}

@keyframes titleGlow {{
    0%   {{ text-shadow: 0 0 0 rgba(77, 163, 255, 0); }}
    50%  {{ text-shadow: 0 0 22px rgba(77, 163, 255, 0.95); }}
    100% {{ text-shadow: 0 0 0 rgba(77, 163, 255, 0); }}
}}

@keyframes floatChip {{
    0%   {{ transform: translateY(0); }}
    50%  {{ transform: translateY(-4px); }}
    100% {{ transform: translateY(0); }}
}}

.hero-chips {{
    margin-bottom: 1.1rem;
}}

.hero-chip {{
    display: inline-block;
    background: rgba(0, 194, 178, 0.18);
    border-radius: 999px;
    padding: 0.23rem 1.1rem;
    margin: 0 0.35rem 0.35rem 0.35rem;
    font-size: 0.80rem;
    text-transform: uppercase;
    letter-spacing: 0.08em;
    border: 1px solid rgba(0, 194, 178, 0.5);
    animation: floatChip 4s ease-in-out infinite;
}}

.hero-title {{
    font-size: 2.8rem;
    font-weight: 800;
    margin-bottom: 0.4rem;
    opacity: 0;
    animation:
        fadeInUp 0.9s ease-out forwards,
        titleGlow 3.2s ease-in-out 1.4s infinite;
}}

.hero-subtitle {{
    font-size: 1.4rem;
    color: var(--hh-azul-claro);
    margin-bottom: 1.6rem;
    opacity: 0;
    animation: fadeInUp 1.1s ease-out forwards;
    animation-delay: 0.25s;
}}

.hero-text {{
    font-size: 1.05rem;
    line-height: 1.6;
    max-width: 900px;
    opacity: 0;
    animation: fadeInUp 1s ease-out forwards;
    animation-delay: 0.45s;
}}
.hero-text p {{
    margin-bottom: 0.45rem;
}}

.hero-footnote {{
    margin-top: 1rem;
    font-size: 0.9rem;
    color: #C7D9E8;
    opacity: 0;
    animation: fadeInUp 0.9s ease-out forwards;
    animation-delay: 0.7s;
}}

.section-separator {{
    margin-top: 1.8rem;
    margin-bottom: 0.4rem;
}}
</style>
"""

# ---------------------------------------------------------------------
# HTML del hero
# ---------------------------------------------------------------------
HERO_HTML = """
<div class="hero-wrapper">
    <div class="hero-overlay">
        <div class="hero-chips">
            <span class="hero-chip">Clima · Agua · Brechas sociales</span>
        </div>
        <h1 class="hero-title">Huella hídrica en América Latina</h1>
        <div class="hero-subtitle">
            Midiendo la huella hídrica para informar y guiar la acción
        </div>
        <div class="hero-text">
            <p>El agua segura no es solo un recurso: es un <b>derecho humano básico</b> y la base del desarrollo en la región.</p>
            <p>Este tablero integra datos de <b>clima</b>, <b>acceso al agua</b> y <b>condiciones socioeconómicas</b> para mostrar dónde están las mayores brechas en América Latina y cómo han ido cambiando en el tiempo.</p>
            <p>Cada punto en estos gráficos representa <b>hogares y comunidades reales</b>. Usa las pestañas de abajo para explorar los indicadores, comparar países y detectar las zonas donde la acción es más urgente.</p>
        </div>
        <div class="hero-footnote">
            🌎 Datos de OMS/UNICEF, World Bank y Open-Meteo · Actualizados para países de América Latina y el Caribe.
        </div>
    </div>
</div>
"""

# ---------------------------------------------------------------------
# Layout principal
# ---------------------------------------------------------------------
def main():
    # Inyectar estilos y hero
    st.markdown(CSS, unsafe_allow_html=True)
    st.markdown(HERO_HTML, unsafe_allow_html=True)

    # Separador antes de los KPIs
    st.markdown('<div class="section-separator"></div>', unsafe_allow_html=True)

    # Tabs de KPIs (01–07)
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(
        [
            "Clima vs acceso a agua segura",         # KPI 01
            "Movilidad forzada para conseguir agua", # KPI 02
            "Zonas críticas para inversión",         # KPI 03
            "Brecha agua vs saneamiento seguro",     # KPI 04
            "Brecha acceso a agua segura",           # KPI 05
            "Correlación agua vs PIB",               # KPI 06
            "Brecha agua vs saneamiento seguro",     # KPI 07
        ]
    )

    with tab1:
        layout_kpi01()

    with tab2:
        layout_kpi02()

    with tab3:
        layout_kpi03()

    with tab4:
        layout_kpi04()

    with tab5:
        layout_kpi05()

    with tab6:
        layout_kpi06()

    with tab7:
        layout_kpi07()


if __name__ == "__main__":
    main()
