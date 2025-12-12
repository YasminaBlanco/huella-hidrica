# API de Ingesta – Huella Hídrica 🌊

## 1. Introducción

Este repositorio contiene la **API de ingesta** desarrollada con **FastAPI** para el proyecto *huella-hidrica*.  
La API centraliza y normaliza la recolección de datos desde distintas fuentes externas y expone los **KPIs** generados en la **capa gold** del pipeline.

Esta API forma parte del pipeline ELT del proyecto, cuyo flujo de trabajo principal se compone de tres módulos:

- Ingesta de datos desde APIs públicas  
- Transformaciones con PySpark  
- Orquestación y programación de tareas con Airflow  

Cada módulo está dockerizado para asegurar despliegues consistentes y reproducibles 📦.

## 2. Objetivos

### 2.1 Ingesta de datos  
La API obtiene y normaliza información proveniente de distintas fuentes externas, tales como:

- Open-Meteo  
- World Bank  
- JMP (Joint Monitoring Programme)  

### 2.2 Exposición de KPIs  
La API permite acceder a los KPIs generados, sirviendo como fuente de consulta, apta para distintos clientes 🌐.

## 3. Arquitectura de la API

- Construida en FastAPI.  
- Estructurada de manera modular, facilitando pruebas, mantenimiento y extensión de nuevas funcionalidades.  
- Routers principales:
  - `/ingest` → para la ingesta de datos desde las APIs externas  
  - `/kpis` → para consultar los KPIs generados en la capa gold  
- Documentación automática Swagger/OpenAPI  
- Desplegada con contenedores Docker 📦  
- Integrable con otros componentes del pipeline

## 4. AWS ☁️

La API se despliega en una instancia **AWS EC2**.  
El contenedor Docker se ejecuta en el servidor, exponiendo los endpoints mediante la configuración de los *Security Groups*.

### 4.1 Arquitectura en EC2

- La instancia EC2 ejecuta Docker y Docker Compose 🛠️  
- La API corre como contenedor  
- Redis corre como contenedor dependiente  
- Se utilizan **IAM Roles** para dar permisos seguros al contenedor  
- Acceso externo controlado mediante reglas de seguridad

## 5. Variables de entorno (Local vs. AWS)

La API utiliza varias variables de entorno:

```
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
AWS_DEFAULT_REGION=
AWS_S3_BUCKET=
REDIS_HOST=
REDIS_PORT=
API_KEY=
DEBUG=
```

### 5.1 Uso en desarrollo local 🛠️

En local, estos valores viven en un archivo `.env`:

```
AWS_ACCESS_KEY_ID=xxxx
AWS_SECRET_ACCESS_KEY=yyyy
AWS_DEFAULT_REGION=us-east-1
AWS_S3_BUCKET=mi-bucket
REDIS_HOST=redis
REDIS_PORT=6379
API_KEY=tu_api_key
DEBUG=True
```

### 5.2 Uso en AWS (producción) ⚠️

En producción **no se recomienda usar un archivo `.env`**.  

#### ✔ IAM Role + Variables de entorno

- La instancia EC2 recibe un IAM Role con permisos para acceder al bucket S3 ✅  
- Variables internas (`REDIS_HOST`, `REDIS_PORT`, etc.) se definen en `/etc/environment` o se obtienen desde **Secrets Manager / Parameter Store**.

## 6. Levantar la API 🚀

### 6.1 Clonar el repositorio

```bash
git clone https://github.com/YasminaBlanco/huella-hidrica.git
cd huella-hidrica/ingest
```

### 6.2 Levantar los servicios con Docker Compose 📦

#### Local

```bash
docker-compose up -d
```

Esto levantará:

- El servicio `api` en el puerto 8000  
- Redis en el puerto 6379  

La API estará disponible en:

```
http://localhost:8000
```

#### AWS EC2 (Producción)

```bash
docker-compose up -d
```

La API estará disponible en:

```
http://<EC2_PUBLIC_IP>:8000
```

Asegurarse de:

- Que el contenedor `api` pueda acceder a S3 mediante IAM Role  
- Que `REDIS_HOST` apunte a la dirección correcta del servicio Redis en producción

### 6.3 Notas importantes ⚠️

- El `docker-compose.yml` monta el directorio actual dentro del contenedor para desarrollo (`volumes: - ./:/app`)  
- Redis es un servicio dependiente de la API (`depends_on`)  
- La API se reiniciará automáticamente si se detiene (`restart: unless-stopped`)  
- Para detener los servicios:

```bash
docker-compose down
```

## 7. Endpoints 🌐

**Documentación automática:**

```
http://<host>:8000/docs
```

## 8. Buenas prácticas ✅

- La API es de fácil extensión a nuevas fuentes de datos.
- Los endpoints de ingesta son **idempotentes**, evitando duplicar datos en caso de que sean necesarios reintentos de ejecución.
- Se tienen **tests** para asegurar la calidad del código y facilitar el desarrollo iterativo.  
- No se almacenan credenciales en repositorios.  
- No se usan `AWS_ACCESS_KEY_ID` en EC2; se utilizan **IAM Roles y políticas** para la seguridad.  
- Documentar nuevos endpoints en Swagger/OpenAPI.  
- Mantener las imágenes Docker **ligeras y reproducibles**
