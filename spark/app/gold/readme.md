# Documentación Técnica de la Capa Gold: KPIs Huella Hídrica en América Latina

## Propósito

Este documento describe la **capa Gold** del proyecto de Huella Hídrica en América Latina y la lógica que se utiliza para construir las vistas orientadas a **KPIs de agua, saneamiento (WASH), clima (Open Mateo) y contexto socioeconómico (World Bank)**.

El objetivo es que cualquier persona del equipo (datos o negocio) pueda entender:

- **Qué tablas Gold existen** y a qué pregunta de negocio responde cada una.
- **Qué significa cada columna**, qué tipo de dato tiene y cómo se calcula.
- **Cómo leer e interpretar los KPIs** 

Estas vistas Gold son la **fuente principal de consulta** para:

- Dashboards de **BI** (por ejemplo, Streamlit / Power BI / herramientas analíticas).
- Reportes y análisis específicos según lo que necesiten los usuarios sobre huella hídrica, brechas de acceso a agua y saneamiento, y su relación con el clima y el contexto socioeconómico.
- Cualquier otra aplicación o servicio que consuma KPIs del proyecto.

---

## Rol de la capa Gold en la arquitectura

La capa Gold se construye a partir de tablas **Fact** limpias y estandarizadas y sus **Dimensiones** asociadas, generadas en la capa Silver. Entre las fuentes principales se incluyen:

- `wash_coverage`  
  - Coberturas de agua y saneamiento (WASH) a partir de la fuente **JMP (WHO/UNICEF)**.
- `climate`  
  - Métricas de precipitación y clima agregadas a partir de **Open-Meteo**.
- `socioeconomic`  
  - Indicadores socioeconómicos (pobreza, PIB, etc.) obtenidos de la API del **World Bank** y estandarizados en la capa Silver.

Sobre estos datos se aplican transformaciones con Spark para:

- Agregar información por **año**, país, tipo de residencia y divisiones administrativas.
- Calcular **métricas derivadas**:
  - Porcentajes de cobertura WASH.
  - Diferencias entre años (deltas en puntos porcentuales, variaciones de precipitación).
  - Tendencias y correlaciones.
  - Banderas de riesgo y clasificación combinando clima, WASH y contexto socioeconómico.
- Construir **vistas listas para consumo**, almacenadas en formato Parquet.
---

A partir de este punto, el documento detalla cada KPI y su tabla asociada en la capa Gold:

- `KPI 01 – Clima vs acceso a agua segura`
- `KPI 02 – Movilidad forzada para conseguir agua`
- `KPI 03 – Zonas críticas para inversión`
- `KPI 04 – Brecha de agua vs saneamiento`
- `KPI 05 – Brecha acceso a agua segura`
- `KPI 06 – Correlación agua vs PIB`
- `KPI 07 – Brecha agua vs saneamiento seguro`

---
## Alcance del documento

Se describen los 7 KPIs principales actualmente modelados en la capa Gold.  
Para cada uno se documenta:

- La **pregunta de negocio**.
- La **vista Gold asociada** y su nivel de detalle (qué representa cada fila).
- La **definición del KPI** y las columnas más relevantes.
- La **lógica del semáforo**.
- Cómo **interpretar** los resultados y por qué es importante.

### KPI 01 – Impacto del clima en el acceso al agua (México y Argentina)
#### 📝 Pregunta de negocio

> Para México y Argentina, entre 2019 y 2024, ¿en qué medida los cambios en la precipitación  
> (sequías o lluvias extremas) se relacionan con cambios en la cobertura de agua segura  
> en zonas urbanas y rurales?

#### Vista Gold 

- **Nombre**: `gold_kpi01_climate_water`
- **Nivel de detalle (grain)**:  
  Cada fila representa una combinación de:
  - País (`country_name`)
  - Tipo de área (`residence_type_desc`: urbano / rural)
  - Año (`year`)

#### Definición del KPI

KPI propuesto:

> **Correlación** entre la variación de precipitación y la variación de cobertura de agua segura, por país (MX/AR) y tipo de área (urbano/rural) durante 2019-2024.

#### Columnas de `kpi01_climate_water`

| Columna                 | Tipo    | Descripción detallada                                                                                                   |
|-------------------------|---------|-------------------------------------------------------------------------------------------------------------------------|
| `country_key`           | INT     | Id interno del país (llave sustituta).                                                                                  |
| `country_name`          | STRING  | Nombre del país (por ejemplo, `Mexico`, `Argentina`).                                                                   |
| `residence_type_key`    | INT     | Id interno del tipo de residencia.                                                                                      |
| `residence_type_desc`   | STRING  | Tipo de área: normalmente `urban` o `rural`.                                                                            |
| `year`                  | INT     | Año de la observación. Solo aparecen años donde se puede comparar contra un año anterior.                              |
| `precip_total_mm_year`  | DOUBLE  | Precipitación acumulada en el **año actual**, en milímetros, para ese país y tipo de área.                             |
| `delta_precip_mm`       | DOUBLE  | Cambio de precipitación respecto al año anterior, en mm.                                                                |
| `safe_water_pct`        | DOUBLE  | % de población con **agua segura** (drinking water, at least basic) en el año actual.                                  |
| `delta_safe_water_pp`   | DOUBLE  | Cambio en la cobertura de agua segura respecto al año anterior, en **puntos porcentuales (p.p.)**.                     |
| `corr_precip_vs_water`  | DOUBLE  | Correlación de Pearson entre `delta_precip_mm` y `delta_safe_water_pp` para ese país y tipo de área.                   |
| `corr_abs_value`        | DOUBLE  | Valor absoluto de `corr_precip_vs_water`, usado para medir la **fuerza** de la relación independientemente del signo.  |
| `risk_level`            | STRING  | Semáforo de riesgo basado en `corr_abs_value` (`green`, `yellow`, `red`, `gray`).                                      |
| `impact_direction`      | STRING  | Dirección del impacto según el signo de la correlación: `direct`, `inverse` o `uncertain`.                             |
| `years_observed`       | BIGINT  | Número de **observaciones de delta** usadas para la correlación. Equivale al número de años en los que se pudo calcular “año actual vs año anterior”. |
---

#### ⚙️ Definición del KPI

El indicador resume qué tan relacionados están los cambios en la precipitación
con los cambios en la cobertura de agua segura, para cada país `c` y tipo de área `r`
(urbano/rural).

1.  **Cálculo de variaciones anuales:**  
    Para cada país `c`, tipo de área `r` y año `t` se calculan las diferencias año contra año:

    $$\Delta \text{Precip}(c,r,t) = \text{PrecipTotalMmYear}(c,r,t) - \text{PrecipTotalMmYear}(c,r,t-1)$$

    $$\Delta \text{AguaSegura}(c,r,t) = \text{SafeWaterPct}(c,r,t) - \text{SafeWaterPct}(c,r,t-1)$$

2.  **Cálculo de la correlación clima–agua:**  
    A partir de los vectores de deltas se calcula la correlación de Pearson:

    $$\text{CorrPrecipVsWater}(c,r) = \text{Corr}\text{Pearson}\big(\Delta \text{Precip}(c,r,\cdot), \Delta \text{AguaSegura}(c,r,\cdot)\big)$$

    También se utiliza el valor absoluto de la correlación:

    $$\text{CorrAbsValue}(c,r) = \left|\text{CorrPrecipVsWater}(c,r)\right|$$


 
#### 🚦 Lógica del semáforo

El semáforo se basa en `corr_abs_value` (fuerza de la correlación):

- **Verde**: `corr_abs_value < 0.3`  
  → Relación **débil**. El clima no está impactando fuertemente el acceso al agua.
- **Amarillo**: `0.3 ≤ corr_abs_value < 0.6`  
  → Relación **moderada**.
- **Rojo**: `corr_abs_value ≥ 0.6`  
  → Relación **fuerte**. El clima “pega directo” al acceso al agua.

> `impact_direction` ayuda a leer el signo:
> - `negative`: menos lluvia se asocia con menor cobertura de agua segura (muy preocupante).
> - `positive`: más lluvia se asocia con mayor cobertura.
> - `uncertain`: la señal no es clara o la correlación es muy baja.

#### 🔍 Cómo interpretarlo / importancia

- Un país/área en **rojo** indica que los cambios en precipitación se reflejan fuertemente en la cobertura de agua segura → **alta vulnerabilidad hídrica**.
- Es un insumo para decidir **dónde invertir en resiliencia** (infraestructura más robusta, almacenamiento, tratamiento, etc.).
- Permite comunicar que el clima no es solo contexto, sino un factor que impacta directamente el acceso al agua.

---

### KPI 02 – Movilidad forzada para conseguir agua

#### 📝 Pregunta de negocio

> ¿Qué países o regiones de América Latina tienen el mayor porcentaje de población que tarda  
> más de 30 minutos en llegar a su fuente principal de agua, y cómo ha evolucionado esta situación  
> en los últimos años?

#### Vista Gold asociada

- **Nombre**: `kpi02_water_mobility`
- **Nivel de detalle (grain)**:  
  Cada fila representa una combinación de:
  - País (`country_name`)
  - Tipo de área (`residence_type_desc`: `urban`, `rural`)
  - Año (`year`)

Solo se incluyen filas para años donde **existe un año anterior** con el que comparar (es decir, cuando se puede calcular el delta).

---

#### Columnas de `kpi02_water_mobility`


| Columna                    | Tipo    | Descripción detallada                                                                                                    |
|----------------------------|---------|--------------------------------------------------------------------------------------------------------------------------|
| `country_key`             | INT     | Id interno del país (llave sustituta de la dimensión país).                                                              |
| `country_name`            | STRING  | Nombre del país (por ejemplo, `Mexico`, `Argentina`, etc.).                                                              |
| `residence_type_key`      | INT     | Id interno del tipo de residencia.                                                                                       |
| `residence_type_desc`     | STRING  | Descripción del tipo de área: `urban` o `rural`.                                                                         |
| `year`                    | INT     | Año de la observación. Es el **año actual** en la comparación contra el año anterior.                                    |
| `pct_over_30min`          | DOUBLE  | % de población cuya fuente principal de agua potable está a **más de 30 minutos** de distancia (servicio limitado).     |
| `delta_pct_over_30min_pp` | DOUBLE  | Cambio año contra año anterior de `pct_over_30min`, en **puntos porcentuales (p.p.)**.                                   |
| `mobility_trend`          | STRING  | Tendencia de la movilidad forzada (`worsened`, `improved`, `stable`).                                                    |
| `risk_level`              | STRING  | Semáforo de riesgo basado en el nivel actual de `pct_over_30min`.                                                       |

---
#### ⚙️ Definición del KPI

El indicador cuantifica qué porcentaje de la población tarda más de 30 minutos
en llegar a su fuente principal de agua y cómo cambia esa situación a lo largo del tiempo,
para cada país `c`, tipo de área `r` (urbano/rural) y año `t`.

1.  **Cálculo del porcentaje de población con tiempo > 30 minutos:**  
    Para cada combinación `(c, r, t)`:

    $$
    \text{PctOver30min}(c,r,t) =
    100 \times
    \frac{\text{Población con tiempo > 30 min}}{\text{Población total con datos}}
    $$

2.  **Variación año contra año y tendencia de movilidad:**  
    Se calcula el cambio en puntos porcentuales respecto al año anterior:

    $$
    \Delta \text{PctOver30min\_pp}(c,r,t) =
    \text{PctOver30min}(c,r,t) - \text{PctOver30min}(c,r,t-1)
    $$

  
#### 🚦 Lógica del semáforo

Basado en `pct_over_30min`:

- **Verde**: `0–5%`  
  → La gran mayoría de la población tiene acceso relativamente cercano a su fuente de agua.
- **Amarillo**: `>5–20%`  
  → Fracción importante de la población debe caminar bastante para conseguir agua.
- **Rojo**: `>20%`  
  → Al menos 1 de cada 5 personas tarda más de 30 minutos para obtener agua.

`mobility_trend` indica si el problema está **mejorando** (tendencia a la baja), **empeorando** (al alza) o se mantiene estable.

#### 🔍 Cómo interpretarlo / importancia

- Convierte la idea de **movilidad forzada para conseguir agua** en un número claro y comparable.
- Un país/área en **rojo** indica una situación crítica: mucha gente dedica una parte significativa de su día solo a acceder al agua.
- Útil para priorizar:
  - Proyectos de **acercamiento de puntos de agua**.
  - Acciones de infraestructura en zonas rurales o barrios marginados.
  - Comunicación clara:  
    > “En este país, al menos una de cada cinco personas camina más de 30 minutos para conseguir agua.”

---
### KPI 03 – Zonas críticas para inversión (clima + saneamiento, México y Argentina)

#### 📝 Pregunta de negocio

> En México y Argentina, entre 2019 y 2024, ¿podemos localizar zonas donde coinciden  
> baja cobertura de saneamiento y una tendencia climática de disminución de lluvias,  
> para priorizar inversión y ayuda humanitaria?

Este KPI se calcula **solo** para México y Argentina.

---

#### Vista Gold asociada

- **Nombre**: `kpi03_critical_zones`
- **Nivel de detalle (grain)**:  
  Cada fila representa una combinación de:
  - País (`country_name`)
  - Provincia/estado (`province_name`)
  - Año (`year`)

Es decir, describe la situación **por provincia y año**, e incluye además el resumen de cuántas zonas críticas hay en ese país y año.

---

#### Columnas de `kpi03_critical_zones`

| Columna                 | Tipo    | Descripción detallada                                                                                           |
|-------------------------|---------|-----------------------------------------------------------------------------------------------------------------|
| `country_key`           | INT     | Id interno del país (llave sustituta de la dimensión país).                                                     |
| `country_name`          | STRING  | Nombre del país (por ejemplo, `Mexico`, `Argentina`).                                                           |
| `province_key`          | INT     | Id interno de la provincia/estado (dimensión `province`).                                                       |
| `province_name`         | STRING  | Nombre de la provincia/estado.                                                                                  |
| `year`                  | INT     | Año de referencia.                                                                                              |
| `sanitation_basic_pct`  | DOUBLE  | % de población con al menos saneamiento básico en la provincia/año (residencia urbana).                         |
| `is_low_sanitation`     | BOOLEAN | `TRUE` si la cobertura de saneamiento básico es baja (`sanitation_basic_pct < 80`).                             |
| `precip_total_mm_year`  | DOUBLE  | Precipitación anual acumulada en milímetros para esa provincia y año.                                          |
| `climate_trend`         | STRING  | Tendencia de la precipitación: `decreasing`, `increasing`, `stable` o `uncertain`.                              |
| `is_climate_neg_trend`  | BOOLEAN | `TRUE` cuando la provincia muestra tendencia a menos lluvia (`climate_trend = 'decreasing'`).                   |
| `is_critical_zone`      | BOOLEAN | `TRUE` cuando se combinan baja cobertura de saneamiento y tendencia climática negativa.                         |                       |

---
#### ⚙️ Definición del KPI

El indicador identifica provincias/estados donde coinciden **baja cobertura de saneamiento**
y una **tendencia climática de disminución de lluvias**, y resume cuántas “zonas críticas”
hay por país `c` y año `t`.

1.  **Saneamiento básico y bandera de baja cobertura:**  
    A partir de los niveles de servicio de saneamiento se calcula el porcentaje con
    al menos saneamiento básico en cada país `c`, provincia `p` y año `t`:

    $$
    \text{SanitationBasicPct}(c,p,t) =
    100 - \big(
      \text{PctUnimproved}(c,p,t) +
      \text{PctOpenDefecation}(c,p,t) +
      \text{PctLimitedService}(c,p,t)
    \big)
    $$

    Se marca la provincia con **saneamiento bajo** si:

    $$
    \text{is\_low\_sanitation}(c,p,t) =
    \begin{cases}
    1 & \text{si } \text{SanitationBasicPct}(c,p,t) < 80 \\
    0 & \text{en otro caso}
    \end{cases}
    $$

2.  **Tendencia climática por provincia:**  
    Se calcula primero la precipitación anual agregada y la correlación entre año y precipitación:

    $$
    \text{corr\_year\_precip}(c,p) =
    \text{corr}_\text{Pearson}
    \big(
      \text{Year},
      \text{PrecipTotalMmYear}(c,p,\text{Year})
    \big)
    $$

#### 🚦 Lógica del semáforo

El semáforo **no** se calcula provincia por provincia, sino a nivel **país + año**, a partir de `critical_zones_count`:

- **Verde** (`green`):  
  `critical_zones_count` ≤ 5  
  → Pocas zonas donde coinciden saneamiento bajo + clima adverso.
- **Amarillo** (`yellow`):  
  6 ≤ `critical_zones_count` ≤ 20  
  → Número intermedio de zonas críticas; situación de atención.
- **Rojo** (`red`):  
  `critical_zones_count` > 20  
  → Muchas provincias críticas; alta prioridad para inversión.

#### 🔍 Cómo interpretarlo / importancia
Permite:
  - Identificar estados/provincias prioritarias para **proyectos de agua y saneamiento**.
  - Comunicar de forma simple que:
    > “En este país, hay X zonas donde el clima y el saneamiento se combinan en contra de la población; aquí es donde conviene enfocar ayuda e inversión.”

---

### KPI 04 – Brecha de agua vs saneamiento

#### 📝 Pregunta de negocio

> ¿Cuál es el **riesgo sanitario compuesto** de cada país, considerando de forma ponderada su déficit de saneamiento, mortalidad infantil y pobreza?

#### 📊 Vista Gold Asociada

`gold/kpi04_weighted_health_risk_population`

| Columna | Tipo de Dato | Definición |
| :--- | :--- | :--- |
| `country_key` | `INT` | Clave del País. |
| `country_name` | `STRING` | Nombre del País. |
| `year` | `INT` | Año del cálculo. |
| `sanitation_coverage_pct` | `DECIMAL(5,2)` | % de población con saneamiento básico/seguro. |
| `child_mortality_rate` | `DECIMAL(8,3)` | Tasa de mortalidad infantil (por 1000 nacidos vivos). |
| `poverty_rate` | `DECIMAL(5,2)` | % de población viviendo con menos de 2.15 USD/día. |
| **`health_risk_index`** | `DECIMAL(6,3)` | **Índice compuesto de riesgo (0-100).** |
| `risk_level` | `STRING` | Nivel de riesgo (Verde, Amarillo, Rojo). |

#### ⚙️ Definición del KPI

El índice se calcula como un score ponderado de 0 a 100, donde $100$ es el riesgo máximo.

1.  **Transformación a Riesgo:** Se invierte el porcentaje de saneamiento:
    $$\text{Sanitation Risk} = 100 - \text{Sanitation Coverage Pct}$$
2.  **Normalización Min-Max:** Cada factor de riesgo (Sanitation Risk, Child Mortality Rate, Poverty Rate) se escala a un rango de $[0, 1]$ usando los límites predefinidos.
3.  **Cálculo del Índice :** Se aplica la suma ponderada de los factores normalizados, escalada a 100:
    $$\text{Risk Index} = \left( \sum (\text{Factor Norm} \times \text{Peso}) \right) \times 100$$
    * **Pesos aplicados:** Saneamiento (50%), Mortalidad Infantil (30%), Pobreza (20%).

#### 🚥 Lógica del Semáforo

| Nivel de Riesgo | Umbral (`health_risk_index`) |
| :--- | :--- |
| **Verde** | Índice $< 20.0$ |
| **Amarillo** | $20.0 \le \text{Índice} < 40.0$ |
| **Rojo** | Índice $\ge 40.0$ |
| **Datos Faltantes** | Si el Índice es NULL. |

#### 🔍 Interpretación / Importancia

El índice es una herramienta clave para la **priorización de recursos**.
* Un valor de **Rojo** indica un riesgo sanitario y social elevado, destacando países donde la falta de saneamiento es más crítica por la alta población vulnerable (pobreza y mortalidad infantil).
* **Importancia:** Permite a las ONG y gobiernos asignar presupuestos de infraestructura WASH y programas de salud en las regiones donde el impacto será mayor, moviéndose más allá de una métrica única.

---

### KPI 05 – Brecha Urbano–Rural en Agua Segura

#### 📝 Pregunta de negocio

> ¿Cuál es la disparidad en el acceso a agua segura (niveles básico y seguro) entre las poblaciones **urbanas y rurales** de cada país y año?

#### 📊 Vista Gold Asociada

`gold/kpi05_urban_rural_gap_water`

| Columna | Tipo de Dato | Definición |
| :--- | :--- | :--- |
| `country_key` | `INT` | Clave del País. |
| `country_name` | `STRING` | Nombre del País. |
| `year` | `INT` | Año del cálculo. |
| `water_urban_pct` | `DECIMAL(5,2)` | % de población urbana con agua segura/básica. |
| `water_rural_pct` | `DECIMAL(5,2)` | % de población rural con agua segura/básica. |
| **`gap_urban_rural_pp`** | `DECIMAL(6,3)` | **Brecha Urbano-Rural en Puntos Porcentuales (Urban % - Rural %).** |
| `risk_level` | `STRING` | Nivel de riesgo de la brecha (Verde, Amarillo, Rojo). |

#### ⚙️ Definición del KPI

La brecha se calcula como la diferencia simple en puntos porcentuales (p.p.) entre la cobertura de agua segura/básica en zonas urbanas y la cobertura en zonas rurales:

$$\text{Brecha Urbano-Rural} = \text{Water Urban Pct} - \text{Water Rural Pct}$$

* **Fuentes:** Datos de cobertura WASH filtrados para el servicio 'drinking water' (clave 2) y niveles 'basic service' o 'safely managed service'.

#### 🚥 Lógica del Semáforo

La clasificación se basa en el **valor absoluto** de la brecha:

| Nivel de Riesgo | Umbral ($\text{ABS}(\text{Gap})$) |
| :--- | :--- |
| **Verde** | $\text{ABS}(\text{Gap}) < 10.0 \text{ p.p.}$ |
| **Amarillo** | $10.0 \le \text{ABS}(\text{Gap}) < 20.0 \text{ p.p.}$ |
| **Rojo** | $\text{ABS}(\text{Gap}) \ge 20.0 \text{ p.p.}$ |

#### 🔍 Interpretación / Importancia

* Un nivel **Rojo** indica una **disparidad significativa** (mayor a 20 p.p.) en el acceso a agua, lo que va en contra del principio de equidad de los ODS.
* **Importancia:** Este KPI subraya la necesidad de políticas específicas para las áreas rurales, que históricamente suelen tener peor infraestructura. Ayuda a medir el éxito de programas enfocados en cerrar la brecha de acceso y garantiza que la inversión no se concentre solo en centros urbanos.

---

### KPI 06 – Correlación Agua Segura vs PIB per Cápita (por Región)

#### 📝 Pregunta de negocio

> ¿Existe una **relación o correlación** significativa a nivel regional entre el **acceso a agua segura** de un país y su **Producto Interno Bruto (PIB) per cápita**?

#### 📊 Vista Gold Asociada

`gold/kpi06_water_gdp_corr`

| Columna | Tipo de Dato | Definición |
| :--- | :--- | :--- |
| `region_name` | `STRING` | Región a la que pertenece el país. |
| `year` | `INT` | Año del cálculo. |
| `n_countries` | `INT` | Número de países en la región con datos disponibles para el cálculo. |
| **`corr_water_vs_gdp`** | `DECIMAL(6,3)` | **Coeficiente de Correlación de Pearson** entre % de Agua Segura y PIB per cápita. |
| **`corr_abs_value`** | `DECIMAL(6,3)` | Valor Absoluto de la Correlación. |
| `avg_safe_water_pct` | `DECIMAL(5,2)` | Promedio regional de % de Agua Segura. |
| `avg_gdp_per_capita` | `DECIMAL(18,2)` | Promedio regional de PIB per cápita. |
| `risk_level` | `STRING` | Nivel de riesgo de la correlación (Verde, Amarillo, Rojo). |

#### ⚙️ Definición del KPI

Este KPI calcula el **Coeficiente de Correlación de Pearson** entre la cobertura total de agua segura y el PIB per cápita para todos los países dentro de una región y año. Solo se incluye si hay 2 o más países en la región con datos.

$$\text{Correlación} = \text{Corr}(\text{Safe Water Pct}, \text{GDP per Capita})$$

* **Agrupación:** El cálculo se realiza por **`region_name`** y **`year`**.

#### 🚥 Lógica del Semáforo

La clasificación se basa en el **valor absoluto** de la correlación (`corr_abs_value`):

| Nivel de Riesgo | Umbral ($\text{ABS}(\text{Correlación})$) |
| :--- | :--- |
| **Verde** | $\text{ABS}(\text{Corr}) < 0.3$ (Correlación débil) |
| **Amarillo** | $0.3 \le \text{ABS}(\text{Corr}) < 0.6$ (Correlación moderada) |
| **Rojo** | $\text{ABS}(\text{Corr}) \ge 0.6$ (Correlación fuerte) |

#### 🔍 Interpretación / Importancia

* Una correlación **positiva fuerte y Roja** ($> 0.6$) indica que el acceso a agua está fuertemente ligado a la riqueza económica de un país dentro de esa región. Esto sugiere que **el progreso en WASH es desigual** y está determinado por la capacidad financiera de los gobiernos.
* **Importancia:** Si la correlación es alta, las organizaciones deben buscar mecanismos de financiamiento alternativos (ej. inversión externa, colaboración público-privada) para desvincular el acceso a servicios esenciales de la volatilidad económica del país.

---

### KPI 07 – Brecha Agua Segura vs Saneamiento Seguro

#### 📝 Pregunta de negocio

> ¿Cuál es la diferencia o **brecha** de cobertura a nivel país y año entre el **acceso a agua segura/básica** y el **acceso a saneamiento seguro/básico**?

#### 📊 Vista Gold Asociada

`gold/kpi07_water_sanitation_gap`

| Columna | Tipo de Dato | Definición |
| :--- | :--- | :--- |
| `country_key` | `INT` | Clave del País. |
| `country_name` | `STRING` | Nombre del País. |
| `year` | `INT` | Año del cálculo. |
| `water_basic_safe_pct` | `DECIMAL(5,2)` | % de población con acceso a agua segura/básica. |
| `sanitation_basic_safe_pct` | `DECIMAL(5,2)` | % de población con acceso a saneamiento seguro/básico. |
| **`gap_water_sanitation_pp`** | `DECIMAL(6,3)` | **Brecha en Puntos Porcentuales (Agua % - Saneamiento %).** |
| `risk_level` | `STRING` | Nivel de riesgo de la brecha (Verde, Amarillo, Rojo). |

#### ⚙️ Definición del KPI

La brecha se calcula como la diferencia simple en puntos porcentuales (p.p.) entre el porcentaje de cobertura de agua y el porcentaje de cobertura de saneamiento a nivel país/año:

$$\text{Brecha Agua-Saneamiento} = \text{Water Basic Safe Pct} - \text{Sanitation Basic Safe Pct}$$

* **Fuentes:** Datos de cobertura WASH, agregando los niveles 'basic service' y 'safely managed service' para ambos servicios (agua y saneamiento).

#### 🚥 Lógica del Semáforo

La clasificación se basa en el **valor absoluto** de la brecha:

| Nivel de Riesgo | Umbral ($\text{ABS}(\text{Gap})$) |
| :--- | :--- |
| **Verde** | $\text{ABS}(\text{Gap}) < 10.0 \text{ p.p.}$ |
| **Amarillo** | $10.0 \le \text{ABS}(\text{Gap}) < 15.0 \text{ p.p.}$ |
| **Rojo** | $\text{ABS}(\text{Gap}) \ge 15.0 \text{ p.p.}$ |

#### 🔍 Interpretación / Importancia

* Una brecha significativa (nivel **Rojo**) indica un **desequilibrio en la inversión y la infraestructura**. Es común que el acceso al agua esté más avanzado que el saneamiento.
* **Importancia:** Un servicio de saneamiento deficiente puede anular los beneficios para la salud del acceso al agua segura, ya que los contaminantes regresan al medio ambiente. Este KPI impulsa la necesidad de **estrategias integrales de WASH** que aborden ambos servicios simultáneamente para maximizar el impacto en la salud pública.

---
