# Infraestructura con Terraform (AWS)

Este documento describe la infraestructura creada con **Terraform** para el proyecto, cubriendo:

- Bucket S3 (arquitectura Medallion: bronze / silver / gold)
- Instancia EC2 para cómputo (Spark)
- IAM Role + Instance Profile para acceso a S3
- Red (VPC por defecto) gestionada como código
- Parámetros de seguridad mediante Security Group

## 1. Objetivo

Adoptar **Infrastructure as Code (IaC)** para que la infraestructura sea:

- Reproducible en cualquier cuenta de AWS
- Trazable 
- Consistente entre entornos
- Más segura (sin llaves estáticas en código)

## 2. Estructura de archivos
```text
Infraestructura
└── Terraform
    ├── providers.tf          
    ├── variables.tf          
    ├── s3_aws.tf             
    ├── iam_ec2_spark.tf      
    └── ec2_spark.tf          
```
## 3. Descripción de cada archivo

### 📌 providers.tf
Define:

- La versión requerida de Terraform
- El provider de AWS (hashicorp/aws)
- La región (tomada desde variables)
- El perfil que AWS CLI utilizará de manera automática gracias al comando aws configure

Permite que Terraform hable con AWS sin exponer credenciales.

### 📌 variables.tf
Contiene variables reutilizables:

- aws_region: región donde se crea la infraestructura
- project_bucket_name: nombre del Data Lake
- ec2_key_name: nombre del Key Pair para conectarte vía SSH

Evita hardcodear valores y facilita replicación.

### 📌 s3_aws.tf
Define la arquitectura del Data Lake:

- Creación del bucket raíz
- Etiquetas para control de costos y organización
- Definición de la lista de carpetas internas:

    - bronze/
    - silver/
    - gold/
    - logs/
    - scripts/

Sigue el esquema Medallion Architecture.

### 📌 iam_ec2_spark.tf

Crea los componentes IAM necesarios:

- Rol IAM que la EC2 puede asumir
- Política administrada AmazonS3FullAccess para que la EC2 pueda leer/escribir en S3 durante el desarrollo.
- Instance Profile para adjuntar el rol a la instancia

Permite que la EC2 acceda a S3 sin usar claves 

### 📌 ec2_spark.tf
Define:

- La VPC por defecto de AWS
- Un Security Group que habilita SSH (22)
- Obtiene automáticamente la AMI Ubuntu 24.04 LTS

Crea la instancia EC2:

- Tipo: c7i-flex.large
- Disco: 40GB gp3
- Rol IAM aplicado
- Security Group aplicado
- Etiquetas para control de costos y organización

La instancia queda lista para recibir Spark.

---

# 4. Paso a paso para replicar la infraestructura

En esta sección se documenta como recrear la infraestructura en su propia cuenta de AWS utilizando Terraform.

---

## 4.0 – Prerrequisitos

Antes de ejecutar Terraform, se requieren dos herramientas:

### Terraform
Descargar desde la página oficial (Windows, macOS, Linux):

👉 https://developer.hashicorp.com/terraform/downloads

### AWS CLI
Descargar desde:

👉 https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html

Una vez instalados, puedes verificar su versión con:

```bash
terraform -version
aws --version
```
## 4.1 – Configurar credenciales AWS (seguras)

Terraform no almacena contraseñas dentro del código, por lo que se usa un perfil de AWS CLI para autenticarse.

Crear un perfil por ejemplo llamado personal:

```bash
aws configure --profile personal
```

Ingresar:

- AWS Access Key ID
- AWS Secret Access Key
- Region: ej. us-east-2 
- Output: json

Luego exportar el perfil en la sesión actual de la terminal:

```bash
- Windows PowerShell

$env:AWS_PROFILE = "personal"

- macOS / Linux

export AWS_PROFILE="personal"

Verificar:

echo $env:AWS_PROFILE     # Windows
echo $AWS_PROFILE         # Linux / Mac
```
## 4.2 – Ubicarse en el directorio de Terraform

Ir a la carpeta donde están los archivos .tf:

```bash
cd Infraestructura/Terraform
```

## 4.3 – Inicializar Terraform
```bash
terraform init
```
Terraform:

- Descarga el provider hashicorp/aws

- Prepara el proyecto

- Verifica dependencias

## 4.4 – Validar sintaxis y consistencia

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

## 4.6 – Aplicar la infraestructura en AWS

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
