# Dashboard de Huella Hídrica – Guía para replicar la app de Streamlit

Este documento explica **cómo levantar y desplegar** el dashboard de KPIs construido con
**Streamlit**, usando las tablas **Gold** almacenadas en **Amazon S3**.

Se detalla como:

1. Configurar el entorno de Python.
2. Configurar **AWS CLI** con sus llaves de acceso.
3. Ejecutar la app con `streamlit run app.py` y ver el dashboard.

---

## 1. Estructura del proyecto

Dentro del repositorio, la parte del dashboard está organizada así:

```text
dashboard_kpis_gold/
    ├── app.py               # punto de entrada de Streamlit.
    ├── app_kpi01.py         # KPI 1
    ├── app_kpi02.py         # KPI 2
    ├── app_kpi03.py         # KPI 3
    ├── app_kpi04.py         # KPI 4
    ├── app_kpi05.py         # KPI 5
    ├── app_kpi06.py         # KPI 6
    ├── app_kpi07.py         # KPI 7
    └── common.py            # Utilidades compartidas para los dashboards de KPIs de huella hídrica.

```
## 2. Requisitos

### 2.1. Software

- **Python:** 3.9 o superior  
- **AWS CLI:** versión 2 recomendada  

### 2.2. Permisos en AWS

Necesitas un **usuario/rol de AWS** con permisos de **lectura** sobre el bucket de **S3** donde se encuentran los datos **Gold**.

## 3. Instalar AWS CLI

En **Windows**, **macOS** o **Linux** puedes seguir el instalador oficial:  
https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html

Para comprobar que quedó instalado:

```bash
aws --version
```
### 3.1 Configurar tus llaves con aws configure

```bash
aws configure
```
El comando te preguntará:

```bash
AWS Access Key ID [None]: TU_ACCESS_KEY
AWS Secret Access Key [None]: TU_SECRET_KEY
Default region name [None]: ej. us-east-1
Default output format [None]: json

```
- TU_ACCESS_KEY y TU_SECRET_KEY: las llaves del usuario/rol que tenga acceso al bucket.
- Default region name: la región donde se encuentra el bucket (por ej. us-east-1).
- Default output format: puedes usar json (o dejarlo vacío).

## 4. Ejecutar el dashboard localmente

### 4.1. Ir a la carpeta del dashboard

Desde la raíz del repositorio:

```bash
cd huella_hidrica/dashboard_kpis_gold
```
### 4.2. Levantar streamlit 

```bash
streamlit run app.py
```
Verás algo similar en la terminal 
```bash
Local URL: http://localhost:8501
Network URL: http://192.168.x.x:8501
```

Abre un navegador y entra a:

- http://localhost:8501

Ahí podrás navegar por los distintos **KPIs**.
