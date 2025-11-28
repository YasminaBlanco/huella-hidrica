# 🚀 Despliegue de Infraestructura Cloud (IaC) & CI/CD

**Rama:** `feature/infra-ci-ale`
**Responsable:** Alejandro Nelson Herrera Soria
**Tickets Asociados:** `KAN-35` (Terraform Infra), `KAN-33` (CI/CD)

## 📑 Tabla de Contenidos

1.  [Resumen Ejecutivo](https://www.google.com/search?q=%23-resumen-ejecutivo)
2.  [Arquitectura y Decisiones de Diseño](https://www.google.com/search?q=%23-arquitectura-y-decisiones-de-dise%C3%B1o)
3.  [Detalle de Implementación: Infraestructura (Terraform)](https://www.google.com/search?q=%23-detalle-de-implementaci%C3%B3n-infraestructura-terraform)
4.  [Detalle de Implementación: CI/CD Pipeline](https://www.google.com/search?q=%23-detalle-de-implementaci%C3%B3n-cicd-pipeline)
5.  [Desafíos Técnicos y Soluciones](https://www.google.com/search?q=%23-desaf%C3%ADos-t%C3%A9cnicos-y-soluciones-troubleshooting)
6.  [Estructura del Código](https://www.google.com/search?q=%23-estructura-del-c%C3%B3digo)

-----

## 📋 Resumen Ejecutivo

Esta rama marca la transición del proyecto hacia un entorno de nube profesional. Se ha implementado **Infraestructura como Código (IaC)** utilizando **Terraform** para aprovisionar una flota de servidores EC2 optimizados para Ingesta, Orquestación y Procesamiento en AWS (Región Ohio `us-east-2`).

Adicionalmente, se ha establecido un flujo de **Integración Continua (CI)** mediante **GitHub Actions** que valida la calidad del código en todas las ramas de desarrollo, asegurando estándares de Python alineados con la infraestructura desplegada.

-----

## 🏗 Arquitectura y Decisiones de Diseño

### 1\. Cómputo Especializado (EC2 Fleet)

En lugar de una arquitectura genérica, diseñamos una flota de instancias optimizada por función para balancear **Rendimiento vs. Costo**:

  * **API de Ingesta:** Instancia `t3.micro`. Aprovecha la capa gratuita para servicios ligeros de entrada de datos.
  * **Orquestador (Airflow):** Instancia **`c7i-flex.large`** (Compute Optimized) con **4GB RAM**.
      * *Decisión:* Se escaló a esta instancia para soportar la carga concurrente de múltiples DAGs sin latencia.
      * *Almacenamiento:* Disco **EBS gp3 de 30GB** para manejar logs y metadatos sin saturación.
  * **Worker de Procesamiento (Spark):** Instancia **`m7i-flex.large`** (General Purpose) con **8GB RAM**.
      * *Decisión:* Necesaria para manejar cargas de trabajo intensivas en memoria durante la transformación de datos (Huella Hídrica).
      * *Almacenamiento:* Disco **EBS gp3 de 50GB** para soportar el "spill to disk" de Spark y almacenamiento de imágenes Docker.

### 2\. Aprovisionamiento Automatizado (Zero-Touch Provisioning)

Se eliminó la configuración manual de servidores. Mediante el uso de **Terraform `user_data`**, todas las instancias se despliegan con un script de inicialización (`install_docker.sh`) que:

  * Actualiza el sistema operativo (Ubuntu 22.04).
  * Instala **Docker** y **Docker Compose**.
  * Configura permisos de usuario y Git.
  * *Beneficio:* El equipo puede empezar a trabajar inmediatamente después del despliegue sin perder tiempo configurando entornos.

### 3\. Calidad de Código Automatizada

Se implementó un pipeline de CI agnóstico al entorno local:

  * **Validación Multi-rama:** El pipeline se ejecuta en cualquier branch (`**`), no solo en `main`, previniendo la integración de código defectuoso desde etapas tempranas.
  * **Entorno de Producción Simulado:** Los tests corren sobre **Python 3.10**, replicando la versión nativa de los servidores Ubuntu 22.04 en AWS.

-----

## 🛠 Detalle de Implementación: Infraestructura (Terraform)

El código de infraestructura se encuentra en el directorio `infra/` y sigue un enfoque modular:

  * **`compute.tf`**: Define la creación de las 3 instancias EC2, asignación de discos `gp3`, inyección de scripts `user_data` y asociación de Security Groups.
  * **`iam.tf`**: Gestiona Roles y Perfiles de Instancia (IAM) para permitir que los servidores accedan a S3 sin necesidad de hardcodear credenciales (AWS Access Keys) en el código.
  * **`storage.tf`**: Define la estructura del Data Lake en S3 (Buckets y carpetas para capas Bronze/Silver/Gold).
  * **`variables.tf`**: Centraliza la configuración (Región, AMIs, Tipos de Instancia), permitiendo cambios rápidos de hardware sin tocar el código lógico.
  * **`provider.tf`**: Configuración del proveedor AWS y versiones de Terraform.
  * **`install_docker.sh`**: Script Bash inyectado en las instancias al momento del arranque (`boot`).

-----

## 🔄 Detalle de Implementación: CI/CD Pipeline

Se configuró un Workflow de GitHub Actions (`.github/workflows/ci.yml`) estricto:

**Pasos del Pipeline:**

1.  **Trigger:** Push o Pull Request hacia cualquier rama.
2.  **Setup:** Levanta contenedor Ubuntu con Python 3.10.
3.  **Linter:** Ejecuta `flake8` para auditar sintaxis y deuda técnica.
4.  **Testing:** Ejecuta `pytest` con descubrimiento automático de tests.
5.  **Quality Gate:** Si algún paso falla, se bloquea la posibilidad de hacer Merge en GitHub.

-----

## 💥 Desafíos Técnicos y Soluciones

Durante la fase de infraestructura nos enfrentamos a desafíos de gestión de estado y seguridad:

| Desafío / Error | Causa Raíz | Solución Implementada |
| :--- | :--- | :--- |
| **InvalidKeyPair.NotFound** | Terraform intentaba usar una llave SSH creada en una región distinta o inexistente en `us-east-2`. | Unificación del nombre de la llave en `variables.tf` y recreación del KeyPair en la región correcta (Ohio). |
| **Bloqueo por Disco Lleno** | Las instancias por defecto (8GB) fallaban al levantar contenedores Docker pesados y logs de Spark. | Implementación de bloques `root_block_device` en Terraform para aprovisionar discos **gp3** de 30GB y 50GB. |
| **Configuración Manual Repetitiva** | Cada reinicio de instancia requería instalar librerías manualmente. | Automatización vía `user_data` con script Bash para instalar Docker/Git al inicio (`boot time`). |
| **Gestión de Estado (State Lock)** | Riesgo de conflictos al trabajar infraestructura en equipo sin un Backend remoto. | Estrategia de **Code Freeze** para el Sprint 1 y uso de `terraform import` planificado para sincronizar recursos existentes (S3) en el Sprint 2. |

-----

## 📂 Estructura del Código

```text
huella-hidrica/
├── .github/
│   └── workflows/
│       └── ci.yml          # Pipeline de Calidad (GitHub Actions)
├── infra/                  # Infraestructura como Código (Terraform)
│   ├── .terraform/         # Binarios de proveedores (Ignorado en git)
│   ├── compute.tf          # Definición de EC2 y Discos
│   ├── iam.tf              # Permisos y Roles
│   ├── install_docker.sh   # Script de automatización (User Data)
│   ├── provider.tf         # Configuración AWS
│   ├── storage.tf          # Definición de S3 (Data Lake)
│   ├── variables.tf        # Variables de configuración
│   └── terraform.tfstate   # Estado local (Ignorado en git)
├── .gitignore              # Exclusiones
└── README.md               # Esta documentación
```
