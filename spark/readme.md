##  Guía de configuración (EC2 + Docker) y ejecución en Spark 
Se explica paso a paso cómo desplegar un entorno de *Apache Spark 4.0* dentro de un contenedor *Docker* corriendo en una *EC2, con acceso seguro a **S3 via IAM Role, y cómo ejecutar un **job de prueba* que lee datos desde Bronze y escribe a Silver.

Así cómo ejecutar los distintos **jobs de transformación** del proyecto de huella hídrica:

- Limpieza Bronze → Silver  
- Modelado Silver (dimensiones / hechos)  
- Capa Gold (KPIs) 

## Arquitectura del proyecto 

```text
spark/
├── app/
│   ├── gold/                              # Jobs de capa Gold (KPIs)
│   │   ├── gold_kpi01_climate_water.py
│   │   ├── gold_kpi02_water_mobility.py
│   │   ├── gold_kpi03_critical_zones.py
│   │   ├── gold_kpi04_health_risk_population.py
│   │   ├── gold_kpi05_urban_rural_gap_water.py
│   │   ├── gold_kpi06_water_gdp_corr.py
│   │   └── gold_kpi07_water_sanitation_gap.py
│   │   └── readme.md                 # Documentación tecnica de la capa Gold
│   │
│   ├── silver/
│   │   ├── dims_facts/                    # Modelado Silver (dimensiones y hechos)
│   │   │   ├── silver_dims_job.py
│   │   │   ├── silver_fact_climate_annual.py
│   │   │   ├── silver_fact_climate_monthly.py
│   │   │   ├── silver_fact_socioeconomic.py
│   │   │   └── silver_fact_wash_coverage_jmp.py
│   │   │
│   │   └── limpieza_transf/              # Limpieza Bronze → Silver
│   │   │   ├── etl_strategies.py         # Job de limpieza para datos de World Bank y Open Mateo 
│   │   │   ├── jmp_silver_job.py         # Job de limpieza para datos JMP
│   │   │   
│   │   └── readme.md                 # Modelo relacional y metadatos de la capa Silver
│   │
│   ├── test_spark.py                     # Job de prueba
│   ├── base_gold_model_job.py            # Clase base para jobs Gold (KPIs)
│   ├── base_silver_job.py                # Clase base para jobs de limpieza Silver
│   ├── base_silver_model_job.py          # Clase base para jobs de modelado Silver
│   ├── main_silver.py                    # Orquestador: Bronze → Silver (clean)
│   ├── main_silver_model.py              # Orquestador: Silver model (dims/facts)
│   ├── main_gold_model.py                # Orquestador: KPIs Gold
│   
│
├── conf/
│   └── spark-defaults.conf               # Configuración de Spark (S3, performance)
│
├── Dockerfile                            # Imagen base de Spark y dependencias
└── docker-compose.yml                    # Orquestación del contenedor de Spark
└── readme.md                             # Guía de ejecución del pipeline en Spark
```
## Requisitos:
- Cuenta AWS con permisos para:
- EC2: Ubuntu 22.04 (tamaño t3.large o superior recomendado).
- Docker + Contenedor Spark (bitnami/spark base).
- IAM Role asociado a la EC2 con permisos sobre los buckets

## Preparar la instancia (Ubuntu + Docker)

- Instalar Docker

``` bash
## Accede por SSH
ssh -i <tu-key.pem> ubuntu@<EC2_PUBLIC_IP>

## Instala Docker 
sudo apt-get update
sudo apt-get install -y ca-certificates curl gnupg lsb-release
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
  sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io

## (Opcional) Usa Docker sin sudo (requiere reiniciar sesión)
sudo usermod -aG docker $USER

## Instala el plugin Docker Compose V2 (docker compose)
sudo apt-get install -y docker-compose-plugin

## Verifica las versiones instaladas
docker --version
docker compose version

```
### Construir y levantar Spark

```bash
# Ir a la carpeta del proyecto
cd ~/spark-elt

# Construir la imagen de Spark
sudo docker compose build

# Levantar el contenedor
sudo docker compose up -d

# Verificar que el contenedor esté corriendo
sudo docker ps
```

### Ejecutar el job de prueba (Bronze → Silver)
```bash
# Entrar al contenedor de Spark
sudo docker exec -it spark bash

# Ir al workspace dentro del contenedor
cd /opt/elt/app

# Ejecutar el script de prueba
spark-submit test_spark.py
```
## Pipeline de transformación completo

A continuación se muestran los comandos que ejecutan los jobs reales de transformación, en el orden correcto:

- Limpieza y estandarización: Bronze → Silver
- Modelado Silver (dimensiones y hechos)
- Modelo Gold (KPIs)

🔁 En estos ejemplos se usa:

- `BASE_BUCKET=henry-pf-g2-huella-hidrica`
- `PROCESS_YEAR=2025`
- `PROCESS_MONTH=12`

Puedes cambiar `PROCESS_YEAR` y `PROCESS_MONTH` según el mes/año que quieras procesar.

- Bronze → Silver (limpieza)

```bash
docker exec -it spark bash -lc '\
  cd /opt/elt/app && \
  export BASE_BUCKET=henry-pf-g2-huella-hidrica && \
  export PROCESS_YEAR=2025 && \
  export PROCESS_MONTH=12 && \
  /opt/bitnami/spark/bin/spark-submit --master local[*] main_silver.py \
'
```
Este comando:

- Usa el bucket configurado en `BASE_BUCKET`.
- Procesa el periodo indicado por `PROCESS_YEAR` y `PROCESS_MONTH`.
- Ejecuta la lógica de limpieza y estandarización, leyendo desde `bronze/` y escribiendo en `silver/`.

- Silver Model (dimensiones y hechos)

```bash
docker exec -it spark bash -lc '\
  cd /opt/elt/app && \
  export BASE_BUCKET=henry-pf-g2-huella-hidrica && \
  export PROCESS_YEAR=2025 && \
  export PROCESS_MONTH=12 && \
  /opt/bitnami/spark/bin/spark-submit --master local[*] main_silver_model.py \
'
```
Este job:

- Parte de la capa Silver limpia.
- Construye las tablas modelo de Silver (dimensiones, hechos y vistas intermedias) siguiendo el diseño dimensional del proyecto.

- Silver Model → Gold (KPIs)

```bash
docker exec -it spark bash -lc '\
  cd /opt/elt/app && \
  export BASE_BUCKET=henry-pf-g2-huella-hidrica && \
  /opt/bitnami/spark/bin/spark-submit --master local[*] main_gold_model.py \
'
```
Este comando:

- Lee exclusivamente desde las tablas modelo de Silver.
- Genera todas las tablas de la capa Gold, con los KPIs que consumen los dashboards (Streamlit / BI).



