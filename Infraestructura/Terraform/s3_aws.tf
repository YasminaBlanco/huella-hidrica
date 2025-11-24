# --------------------------------
# 1) Bucket raíz del Data Lake
# --------------------------------

# Crea un bucket S3 
resource "aws_s3_bucket" "data_lake" {
  bucket = var.project_bucket_name
  # Tags que se aplican al bucket 
  tags = {
    Name        = var.project_bucket_name # Nombre legible del bucket
    Project     = "huella-hidrica"        # Nombre del proyecto
    Environment = "dev"                   # Ambiente 
  }
}

# -----------------------------------------
# 2) Lista de "carpetas" internas
# -----------------------------------------

locals {
  # Nombre de las carpetas para la arquitectura Medallion
  prefixes = [
    "bronze/",
    "silver/",
    "gold/",
    "logs/",
    "scripts/",
  ]
}

resource "aws_s3_object" "folders" {
  for_each = toset(local.prefixes)

  bucket  = aws_s3_bucket.data_lake.id
  key     = each.value
  content = ""

  # Tags para identificar las "carpetas".
  tags = {
    Project     = "huella-hidrica"
    Environment = "dev"
    # "bronze/", "silver/", etc.
    Layer = each.value
  }
}
