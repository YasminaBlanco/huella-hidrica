# Orquestador – Huella Hídrica 🌊

## 1. Introducción

Este repositorio contiene la configuración y DAG principal de **Airflow** para el proyecto *huella-hidrica*.  
El orquestador gestiona la ejecución del pipeline ELT, coordinando la ingesta desde APIs externas, las transformaciones con PySpark y la posterior subida al storage de los *KPIs* generados.

El flujo de trabajo principal del pipeline se compone de tres módulos:

- Ingesta de datos desde APIs públicas  
- Transformaciones con PySpark  
- Orquestación y programación de tareas con Airflow  

Cada módulo está dockerizado para asegurar despliegues consistentes y reproducibles 📦.

## 2. Objetivos

- Coordinar la ejecución de la ingesta y transformaciones según la periodicidad definida (única, anual, semanal).  
- Garantizar la **idempotencia** de las tareas, evitando duplicación de datos.  
- Registrar logs centralizados de ejecución y errores para monitoreo y auditoría.  
- Notificar por correo el estado de finalización de cada tarea importante.  

## 3. Arquitectura

- **Apache Airflow** en EC2 de AWS
- DAG principal: `main_ingest`  
- Orquestación mediante **LocalExecutor**  
- Base de datos para Airflow: **PostgreSQL 15**  
- Logs persistentes y DAGs montados como volúmenes de Docker  
- Ejecuta transformaciones en **PySpark** mediante SSH a instancia EC2  
- Envío de notificaciones por correo usando SMTP (Mailgun)  

### 3.1 DAG `main_ingest`

- Ingesta de datos:
  - `ingest_unique` (JMP)  
  - `ingest_annual` (World Bank)  
  - `ingest_weekly` (Open-Meteo)  
- Validaciones de salud (`healthcheck`) para cada ingest  
- Transformaciones PySpark agrupadas en `spark_jobs_group`:
  - `run_test`  
  - `run_silver`  
  - `run_silver_model`  
  - `run_gold`  
- La instancia EC2 para transformaciones PySpark se **enciende y apaga según sea necesario** para optimizar recursos y costos 💡
- Notificaciones por correo para ingesta y transformaciones  
- Control de flags y variables de Airflow para asegurar **idempotencia**  

### 3.2 DAG principal: `main_ingest`

- Ingesta de datos:
  - `ingest_unique` (JMP)  
  - `ingest_annual` (World Bank)  
  - `ingest_weekly` (Open-Meteo)  
- Validaciones de salud (`healthcheck`) para cada ingest  
- Transformaciones PySpark agrupadas en `spark_jobs_group`:
  - `run_test`  
  - `run_silver`  
  - `run_silver_model`  
  - `run_gold`  
- La instancia EC2 para transformaciones se **enciende y apaga según necesidad**  
- Notificaciones por correo para ingesta y transformaciones  
- Control de flags y variables de Airflow para asegurar **idempotencia**

## 4. Variables de entorno

### 4.1 Configuración local
En desarrollo local, crea un archivo `.env` en la raíz del proyecto (`/orch`) con las variables necesarias.

Ejemplo de `.env`:
```
AIRFLOW__CORE__SQL_ALCHEMY_CONN=
AIRFLOW__CORE__EXECUTOR=
AIRFLOW__WEBSERVER__SECRET_KEY=
AIRFLOW__CORE__FERNET_KEY=
AIRFLOW__SMTP__SMTP_HOST=
AIRFLOW__SMTP__SMTP_STARTTLS=
AIRFLOW__SMTP__SMTP_SSL=
AIRFLOW__SMTP__SMTP_PORT=
AIRFLOW__SMTP__SMTP_USER=
AIRFLOW__SMTP__SMTP_PASSWORD=
AIRFLOW__SMTP__SMTP_MAIL_FROM=
PYSPARK_INSTANCE_ID=
PYSPARK_PRIVATE_IP=
AWS_REGION=
BASE_BUCKET=
SSH_KEY_PATH=
EMAIL_TO=
API_CONN_ID=
```

### 4.2 Configuración en AWS Cloud
En producción (AWS), **no se recomienda usar archivos .env** por seguridad. En su lugar, las variables de entorno se  definen al ejecutar los contenedores, pasadas como argumentos en la línea de comandos o configúralas en el servicio de EC2. Otras pueden definirse usando la UI web de Airflow, al igual que las conexiones HTTP y SMTP.

#### Conexión SSH entre EC2
La EC2 que tiene Airflow se conecta por SSH vía IP privada a la EC2 que tiene PySpark.  
Esto se hace copiando las claves `.pem` de la EC2 con PySpark en la EC2 que tiene Airflow y dandole los permisos necesarios.

## 5. Levantar Airflow 🚀

### 5.1 Inicializar DB

```bash
docker compose up airflow-init
```

### 5.2 Levantar servicios en segundo plano

```bash
docker compose up -d
```

### 5.3 Acceder a la UI de Airflow

```
http://<EC2_PUBLIC_IP>:8081
Usuario/clave inicial: admin/ admin
```

### 5.4 Notas importantes

- La máquina EC2 para transformaciones PySpark se enciende antes de ejecutar los jobs y se apaga al finalizar, optimizando costos 🔌
- Se usan flags en Airflow (Variable) para asegurar que ciertas tareas se ejecuten una sola vez por periodo (idempotencia)
- Los logs de cada tarea se almacenan en `/opt/airflow/logs/`
- Las notificaciones por correo informan el éxito o fallo de ingestas y transformaciones

## 6. Buenas prácticas ✅

- Mantener los contenedores livianos y reproducibles
- No se almacenan credenciales en repositorios
- Usar Airflow UI para definir variables y conexiones
- Garantizar idempotencia mediante flags de Airflow y lógica de control en ShortCircuitOperator
