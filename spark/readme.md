##  Guía de configuración (EC2 + Docker) y pruebas de Spark 
Se explica paso a paso cómo desplegar un entorno de **Apache Spark 4.0** dentro de un contenedor **Docker** corriendo en una **EC2**, con acceso seguro a **S3 via IAM Role**, y cómo ejecutar un **job de prueba** que lee datos desde *Bronze* y escribe a *Silver*.

## Arquitectura
```text
├── app/                                   # Scripts principales de procesamiento en Spark
│   ├── test_spark.py                      # Job de prueba
├── conf/                                  # Archivos de configuración
│   └── spark-defaults.conf                # Parámetros por defecto para sesiones de Spark
│
├── dockerfile                             # Define la imagen base de Spark y dependencias
├── docker-compose.yml                     # Orquesta contenedores (Spark, dependencias, etc.)
```
## Requisitos:
- Cuenta AWS con permisos para:
- EC2: Ubuntu 22.04 (tamaño t3.large o superior recomendado).
- Docker + Contenedor Spark (bitnami/spark base).
- IAM Role asociado a la EC2 con permisos sobre los buckets

## Preparar la instancia (Ubuntu + Docker)

- Instalar Docker

```bash
# Accede por SSH
ssh -i <tu-key.pem> ubuntu@<EC2_PUBLIC_IP>

# Instala Docker 
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

# (Opcional) Usa Docker sin sudo (requiere reiniciar sesión)
sudo usermod -aG docker $USER

# Instala el plugin Docker Compose V2 (docker compose)
sudo apt-get install -y docker-compose-plugin

# Verifica las versiones instaladas
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