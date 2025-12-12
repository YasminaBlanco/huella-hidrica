# Infraestructura con Terraform (AWS) – Proyecto Huella Hídrica LATAM

Este documento describe la infraestructura creada y administrada con **Terraform** para el proyecto de huella hídrica en América Latina.  
El enfoque es **modular, escalable y reproducible**, con decisiones justificadas en términos de **costo, escalabilidad y mantenibilidad**.

La infraestructura cubre:

- Arquitectura de **Data Lake en S3** con esquema Medallion (**bronze / silver / gold**).
- Tres **instancias EC2**:
  - **Spark** (procesamiento de datos).
  - **API** de ingesta.
  - **Airflow** como orquestador de pipelines.
- **Roles IAM**, **políticas de mínimo privilegio** e **instance profiles** para acceso seguro a S3 y control de EC2.
- **Seguridad de red** usando la VPC por defecto y **Security Groups** específicos.
- **User data** en EC2 para instalar Docker y Docker Compose de forma automática.

---

## 1. Objetivo de la infraestructura

Adoptar **Infrastructure as Code (IaC)** para que la infraestructura del proyecto sea:

- **Reproducible** en cualquier cuenta de AWS (personal, equipo, futuros entornos).
- **Escalable**: sencillo agregar nuevas fuentes, capas o nodos de cómputo.
- **Trazable**: cada cambio queda registrado en código y en `terraform state`.
- **Mantenible**: configuración modular y separada por responsabilidad.
- **Segura**: uso de **IAM Roles + Instance Profiles** en lugar de llaves estáticas.

---
### 1.1. Componentes gestionados 100% como código

Toda la infraestructura descrita arriba se crea y administra exclusivamente con **Terraform**:

- **Storage**  
  - Bucket S3 del Data Lake y su estructura de carpetas (`bronze/`, `silver/`, `gold/`, `logs/`, `scripts/`).

- **Cómputo**  
  - Instancia EC2 de **Spark** para procesamiento batch.  
  - Instancia EC2 de **API** de ingesta.  
  - Instancia EC2 de **Airflow** como orquestador de pipelines.

- **Red**  
  - VPC por defecto declarada en `vpc.tf`.  
  - Security Groups específicos para cada instancia (SSH + puertos de servicio: 8000 para API, 8080 para Airflow).

- **Seguridad e identidad**  
  - Roles IAM, políticas de **mínimo privilegio** y *instance profiles* para acceso a S3 y control de EC2.  
  - Uso exclusivo de **IAM Roles** (sin llaves de acceso estáticas en el código).
---

## 2. Arquitectura general

### 2.1. Vista de alto nivel

aqui va el diagrama que hice 


### 2.2. Estructura de archivos

```text
Infraestructura/
└── terraform_huella/
    ├── providers.tf
    ├── variables.tf
    ├── s3_aws.tf
    ├── iam_ec2_spark.tf
    ├── iam_ec2_airflow.tf
    ├── ec2_spark.tf
    ├── ec2_api.tf
    ├── ec2_airflow.tf
    └── vpc.tf (manejo de VPC por defecto)
```
## 3. Descripción de cada archivo

A continuación se describe el rol de cada archivo Terraform dentro de la carpeta `terraform_huella`, explicando **qué hace** y **por qué existe**.

---

### 📌 `providers.tf`

Define la configuración general de Terraform:

- Especifica la versión mínima de Terraform requerida.
- Declara el **provider de AWS** (`hashicorp/aws`).
- Indica la **región** donde se crearán los recursos.
- Se apoya en el perfil configurado vía `aws configure` para obtener credenciales de forma segura.

En resumen, este archivo es el “conector” entre Terraform y AWS, evitando que se expongan credenciales en el código.

---

### 📌 `variables.tf`

Centraliza los parámetros configurables de la infraestructura:

- `aws_region`: región de AWS donde se desplegarán los recursos (por ejemplo, `us-east-2`).
- `project_bucket_name`: nombre del bucket S3 que actúa como Data Lake.
- `ec2_key_name`: nombre del **Key Pair** usado para conectarse por SSH a las instancias EC2.

Permite:

- Evitar hardcodear valores directamente en cada recurso.
- Facilitar la **replicación** de la infraestructura en otras cuentas o entornos cambiando solo las variables.

---

### 📌 `s3_aws.tf`

Define todo lo relacionado con el **Data Lake en S3**:

- Crea el **bucket** principal donde se almacenarán los datos del proyecto.
- Aplica **etiquetas (tags)** para identificar proyecto, ambiente y facilitar control de costos.
- Crea la estructura lógica de carpetas:

  - `bronze/` – datos crudos.
  - `silver/` – datos limpios/estandarizados.
  - `gold/` – vistas analíticas, modelos y KPIs.
  - `logs/` – logs de procesos.
  - `scripts/` – scripts auxiliares y artefactos técnicos.

Sigue el enfoque de **Medallion Architecture**, separando las capas de datos por nivel de procesamiento.

---

### 📌 `iam_ec2_spark.tf`

Administra los componentes **IAM** reutilizados por Spark y la API:

- Crea un **IAM Role** que las instancias EC2 (Spark y API) pueden asumir.
- Define una **política de mínimo privilegio** para acceso al Data Lake:
  - Permite listar el bucket.
  - Permite leer, escribir y borrar objetos **solo** dentro de las rutas `bronze/`, `silver/`, `gold/`, `logs/` y `scripts/`.
- Crea un **Instance Profile**, que es el “puente” entre el rol IAM y la instancia EC2.

Gracias a este archivo, las EC2 pueden acceder a S3 usando **perfiles de instancia**, sin exponer llaves de acceso en variables de entorno o código.

---

### 📌 `iam_ec2_airflow.tf`

Gestiona IAM específicamente para la instancia de **Airflow**:

- Define un **IAM Role** para la EC2 donde correrá Airflow.
- Asocia una política de acceso a S3 (similar o compatible con la de Spark), para que Airflow pueda leer/escribir en el Data Lake.
- Define una **política adicional** para que Airflow pueda:
  - Consultar el estado de las instancias EC2 del proyecto.
  - Encender y apagar las instancias de Spark y de la API usando la API de EC2..
- Crea su respectivo **Instance Profile** para asociar ese rol a la instancia de Airflow.

Este archivo permite que Airflow actúe como **orquestador**, gestionando tanto flujos de datos como el ciclo de vida de las máquinas de cómputo.

---

### 📌 `ec2_spark.tf`

Describe la instancia EC2 dedicada a **procesamiento con Spark**:

- Declara un **Security Group** que:
  - Permite acceso SSH (puerto 22) para administración remota.
  - Habilita salida a internet para instalar paquetes y comunicarse con otros servicios.
- Obtiene automáticamente una **AMI de Ubuntu 24.04 LTS** (imagen oficial de Canonical).
- Crea la instancia EC2 con:
  - Tipo de instancia orientado a cómputo (`m7i-flex.xlarge`).
  - Disco raíz de 40 GB en `gp3`.
  - El **Instance Profile** con el rol IAM de acceso a S3.
  - Etiquetas para identificar el recurso dentro del proyecto.

Además, incluye un script de **user_data** que:

- Instala Docker y Docker Compose v2 al iniciar la instancia.
- Habilita el servicio de Docker.
- Agrega al usuario `ubuntu` al grupo `docker` para poder ejecutar contenedores sin `sudo`.
- Escribe mensajes de verificación en el log de `cloud-init`, lo que permite comprobar desde consola que la instalación fue correcta.

El resultado es una máquina lista para desplegar contenedores de Spark y workloads de procesamiento de datos.

---

### 📌 `ec2_api.tf`

Define la instancia EC2 donde se desplegará la **API de ingesta**:

- Crea un **Security Group** que abre:
  - SSH (puerto 22) para administración.
  - Puerto 8000 para recibir solicitudes HTTP.
- Utiliza la misma AMI de Ubuntu 24.04 para mantener homogeneidad.
- Asigna el **mismo Instance Profile** que Spark, de modo que la API también pueda leer/escribir en el Data Lake.
- Configura un script de **user_data** similar:
  - Instala Docker y Docker Compose v2.
  - Habilita Docker y registra logs de verificación.

Con esto, la instancia queda lista para ejecutar la API (por ejemplo, FastAPI) en contenedores, con acceso directo al bucket S3 del proyecto.

---

### 📌 `ec2_airflow.tf`

Especifica la instancia EC2 para **Airflow** como orquestador:

- Define un **Security Group** con:
  - SSH (puerto 22) para acceso administrativo.
  - Puerto 8080 para la interfaz web de Airflow.
- Utiliza la AMI de Ubuntu 24.04 LTS.
- Tipo de instancia orientado a cómputo (`c7i-flex.xlarge`)
- Asigna el **Instance Profile** con el rol de Airflow (acceso a S3 + permisos controlados sobre EC2).
- Incluye un script de **user_data** que instala Docker y Docker Compose, dejando la instancia lista para levantar el stack de Airflow vía contenedores.

---

### 📌 `vpc.tf` 

Gestiona la **VPC por defecto** de la cuenta:

- Declara en Terraform el recurso que representa la VPC *default* de la región.
- Permite que todos los Security Groups (Spark, API, Airflow) se definan dentro de esa misma VPC, manteniendo una red coherente.
- Deja la red también versionada como código, sin necesidad de crear una VPC nueva desde cero.

---

En conjunto, estos archivos permiten describir **toda la infraestructura del proyecto como código**: almacenamiento (S3), cómputo (EC2), seguridad (IAM y Security Groups), red (VPC) y preparación de los nodos para trabajar con Docker, Spark, API y Airflow de forma reproducible y controlada.

## 4. Paso a paso para replicar la infraestructura

En esta sección se documenta cómo recrear la infraestructura en una cuenta de AWS utilizando **Terraform**.

### 4.0 – Prerrequisitos

Antes de ejecutar Terraform, se requieren dos herramientas:

Terraform

Descargar desde la página oficial (Windows, macOS, Linux):

- https://developer.hashicorp.com/terraform/downloads

AWS CLI

Descargar desde:

- https://docs.aws.amazon.com/es_es/cli/latest/userguide/getting-started-install.html

Verificar instalación:

```bash
terraform -version
aws --version
```
También necesitas:

- Una cuenta de AWS con permisos para S3, EC2 e IAM.

- Un Key Pair creado en la región donde desplegarás (usado para conectarte vía SSH a las EC2).

### 4.1 – Configurar credenciales AWS (seguras)

Terraform no almacena contraseñas dentro del código. Para autenticarse, se usa un perfil de AWS CLI.

Crear un perfil, por ejemplo:
```bash
aws configure --profile huella
```
Ingresar:

- AWS Access Key ID
- AWS Secret Access Key
- Region: ej. us-east-2 
- Output: json

### 4.2 – Ubicarse en el directorio de Terraform

Ir a la carpeta donde están los archivos .tf:

```bash
cd Infraestructura/terraform_huella
```
### 4.3 – Inicializar Terraform
```bash
terraform init
```
Terraform:

- Descarga el provider hashicorp/aws

- Prepara el proyecto

- Verifica dependencias

### 4.4 – Validar sintaxis y consistencia

```bash
terraform validate

Debe responder:
Success! The configuration is valid.
```
## 4.5 – Ver el plan de ejecución

```bash
terraform plan
```
Este comando:

- Revisa el estado actual en AWS
- Compara con los archivos .tf
- Muestra qué recursos creará/modificará/destruirá

### 4.6 – Aplicar la infraestructura en AWS

```bash
terraform apply

Terraform pedirá confirmación:

Do you want to perform these actions?
Only 'yes' will be accepted to approve.

Enter a value:

Escribir:
yes
```
Terraform creará automáticamente todos los recursos definidos en los scripts.

### 4.7 – Verificar la infraestructura creada

1) Bucket S3

En la consola de AWS:

- Ir a S3: Buckets.
- Verificar que existe el bucket con el nombre project_bucket_name.
- Entrar al bucket y comprobar que aparecen las rutas:
  - bronze/
  - silver/
  - gold/
  - logs/
  - scripts/

2) IAM

En IAM - Roles:

- Verificar la existencia de los roles de EC2:
  - Rol de Spark.
  - Rol de Airflow (con política adicional para EC2).

- Revisar que tienen adjuntas las políticas esperadas:
  - Política de acceso mínimo a S3.
  - Política de control de EC2 en el caso de Airflow.

3) EC2

En EC2 - Instances:

- Ver tres instancias etiquetadas:
  - Name = spark
  - Name = api
  - Name = airflow
- Verificar que están en la región correcta (aws_region).

- Conectar por SSH (ejemplo para la instancia Spark):

```bash
ssh -i ruta/a/tu/spark-ap.pem ubuntu@IP_PUBLICA_DE_SPARK
```
- Dentro de la instancia, comprobar que Docker quedó instalado:

```bash
docker --version
docker compose version
```

- Si quieres revisar todo el log del user_data:

```bash
sudo cat /var/log/cloud-init-output.log
```

### 4.8 – Destruir la infraestructura

Si quieres limpiar el entorno creado con Terraform:

```bash
terraform destroy
```
De esta manera, toda la infraestructura se mantiene controlada por código y puede recrearse o eliminarse de forma trazable y reproducible.

## 5. Decisiones de diseño (costo, escalabilidad y mantenibilidad)

Al diseñar la infraestructura se tomaron las siguientes decisiones:

- **Uso de una sola VPC por defecto (entorno de desarrollo)**  
  - Se reutiliza la VPC default de la región, lo que simplifica la configuración de red y evita costos extra de NAT Gateways o VPCs complejas.  
  - Aun así, la VPC queda declarada en `vpc.tf`, por lo que la topología de red también está versionada como código.

- **Data Lake único en S3 con arquitectura Medallion**  
  - Un solo bucket (`project_bucket_name`) con carpetas `bronze/`, `silver/`, `gold/`, `logs/` y `scripts/`.  
  - Esta estructura hace más sencillo controlar permisos, optimizar costos de almacenamiento y mantener una ruta clara desde datos crudos hasta KPIs.

- **Políticas de mínimo privilegio en IAM**  
  - Los roles de EC2 solo pueden listar el bucket del proyecto y operar dentro de las rutas definidas del Data Lake.  
  - Airflow tiene una política adicional únicamente para consultar, iniciar y detener las instancias EC2 del proyecto, evitando permisos globales sobre la cuenta.

- **Elección de tipos de instancia**  
  - **Spark**: `m7i-flex.large`, orientada a cómputo, adecuada para cargas de procesamiento de datos manteniendo un costo razonable.  
  - **API**: `t3.small`, suficiente para el servicio de ingesta HTTP, optimizando costos al no sobreaprovisionar recursos.  
  - **Airflow**: `c7i-flex.large` , balanceando memoria y CPU para el orquestador.

- **Automatización del entorno de ejecución con *user_data***  
  - Todas las instancias instalan **Docker y Docker Compose v2** al iniciar.  
  - Esto reduce el “setup manual”, facilita la reproducibilidad del entorno y permite desplegar el stack (Spark, API, Airflow) de forma homogénea.

- **Mantenibilidad y modularidad del código Terraform**  
  - Los recursos se agrupan en archivos por responsabilidad.  
  - Esta organización facilita el *peer review*, el debugging y la extensión futura (por ejemplo, añadir nuevas fuentes o más nodos de cómputo).

En conjunto, estas decisiones buscan un equilibrio entre **costo**, **escalabilidad** y **mantenibilidad**, alineado con las buenas prácticas de infraestructura en la nube para proyectos de datos.
