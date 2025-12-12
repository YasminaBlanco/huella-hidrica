# 💧"Huella Hídrica de América Latina"

### Diseño e Implementación de un Pipeline para Vulnerabilidad Hídrica y Social

---

## 💡 1. Contexto y Objetivos

Millones de personas en América Latina enfrentan un acceso limitado al agua potable y saneamiento adecuado, agravado por el cambio climático y la desigualdad socioeconómica. Este proyecto, impulsado por una ONG, busca transformar la toma de decisiones en la región.

### Objetivo General
Construir una **Plataforma de Datos abierta y escalable (Data Lake en AWS)** que integre información crítica para generar insights accionables, orientando la inversión y las políticas públicas hacia los territorios con mayor **vulnerabilidad hídrica, sanitaria y social**.

### 🎯 Preguntas Clave que Responde la Plataforma
* Impacto del clima en el acceso al agua.
* Identificación de zonas críticas para inversión en infraestructura.
* Riesgo sanitario y social asociado a la falta de saneamiento y pobreza.
* Relación entre desarrollo económico y acceso al agua.
* Brechas urbano–rural en servicios WASH.

## 📐 2. Arquitectura de Datos y Stack Tecnológico

El sistema implementa un *pipeline* **ELT (Extract, Load, Transform)** sobre una **Arquitectura Medallion** en **Amazon S3**, utilizando herramientas de código abierto para el procesamiento y la orquestación.

### Stack Tecnológico

| Categoría | Tecnología Principal | Propósito |
| :--- | :--- | :--- |
| **Plataforma Cloud** | **AWS (S3, EC2, IAM)** | Almacenamiento escalable del Data Lake
| **Data Lake** | **Amazon S3** | Arquitectura Medallion (Bronze, Silver, Gold). Formato **Parquet** optimizado. |
| **Transformación** | **Apache Spark (PySpark)** | Limpieza, estandarización y modelado dimensional (desplegado en Docker/EC2). |
| **Orquestación** | **Apache Airflow** | Programación, monitoreo y encadenamiento de *jobs* ETL (desplegado en Docker/EC2). |
| **Ingesta** | **FastAPI Microservices** | Microservicios dedicados para APIs y fuentes de datos específicas. |

---

## 🗺️ 3. Fuentes de Datos y Alcance

| Fuente | Contenido | Granularidad | Alcance Geográfico |
| :--- | :--- | :--- | :--- |
| **JMP (WHO/UNICEF)** | Acceso a Agua y Saneamiento (WASH). | País, Año (Desagregación Urbano/Rural). | Países de América Latina. |
| **Open-Meteo** | Variables climáticas históricas (Precipitación, Temperatura). | Diario (Agregado a Mensual/Anual). | México, Argentina, Brasil, Chile. |
| **World Bank** | Indicadores Socioeconómicos (PIB p/Cápita, Pobreza, Mortalidad infantil). | País, Año. | Países de América Latina. |

---

## ⚙️ 4. Guía de Instalación y Ejecución (Core Infrastructure)

La arquitectura se despliega utilizando **Docker Compose** en instancias **AWS EC2** (mínimo 8GB RAM, 2 vCPUs).

### 4.1. Configuración del Servidor

Se recomienda usar **tres instancias EC2 (Spark, Kafka, Airflow)**.

* **Instancias EC2 (Ubuntu 24.04 LTS):** Configurar y descargar la clave `.pem`. Asignar el perfil **IAM** requerido para acceder a S3.
* **Conexión SSH y Actualización:**
    ```bash
    ssh -i tu-clave.pem usuario@ip-publica
    sudo apt update && sudo apt upgrade -y
    ```
* **Instalar Docker y Docker Compose:**
    ```bash
    sudo apt install docker.io docker-compose -y
    sudo usermod -aG docker $USER 
    # ¡CERRAR Y VOLVER A ABRIR SESIÓN SSH!
    ```

### 4.2. Configuración del Proyecto

* **Clonar el Repositorio:**
    ```bash
    git clone [https://github.com/tu-usuario/proyecto-integrador.git](https://github.com/tu-usuario/proyecto-integrador.git)
    cd proyecto-integrador
    ```
* **Configurar Variables de Entorno (`.env`):**
    Editar el archivo `.env` con las credenciales y nombres de *Buckets* S3:
    ```bash
    # AWS
    AWS_ACCESS_KEY_ID=TU_ACCESS_KEY
    AWS_SECRET_ACCESS_KEY=TU_SECRET_KEY
    # Nombres de Buckets S3
    BUCKET_NAME_RAW=TU_BUCKET_RAW
    BUCKET_NAME_SILVER=TU_BUCKET_SILVER
    BUCKET_NAME_GOLD=TU_BUCKET_GOLD
    S3_REGION=TU_REGION
    ```

### 4.3. Despliegue y Ejecución

Para entornos distribuidos, la carpeta correspondiente debe copiarse a su respectiva instancia EC2 (*spark/* a la instancia Spark, etc.).

* **Construir y Levantar los Contenedores:**
    ```bash
    docker-compose up -d --build
    ```
* **Verificar el Estado:**
    ```bash
    docker-compose ps
    ```

### 4.4. Acceso a Airflow UI

* **URL:** `http://<IP_PÚBLICA>:8080`
* Acceda a la interfaz, cargue las conexiones y **desbloquee** el DAG principal para iniciar el *pipeline* Bronze → Silver → Gold.

---

## 📄 5. Documentación por Módulo

Para una comprensión profunda de la lógica de cada componente, por favor, consulte los `README.md` individuales:

* **[Infraestructura (Terraform) »](terraform/README.md)**: Estructura de la infraestructura.
* **[Ingesta (FastAPI) »](ingest/readme.md)**: Detalle sobre la extracción de datos de las APIs de World Bank y Open-Meteo y la carga a la capa Bronze.
* **[(Spark) »](spark/readme.md)**: Como levantar el contenedor de Spark, ejecutar los scripts de ETL y visualizar los resultados.
* **[Silver»](spark/app/silver/readme.md)**: Estructura de la capa Silver y los scripts de limpieza.
* **[Gold»](spark/app/gold/readme.md)**: Estructura de la capa Gold y los scripts de modelado.
* **[Orquestación (Airflow) »](orch/readme.md)**: Estructura de los DAGs, dependencias y manejo de errores.
