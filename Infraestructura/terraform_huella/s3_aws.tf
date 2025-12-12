# --------------------------------
# 1) Bucket raíz del Data Lake
# --------------------------------

resource "aws_s3_bucket" "data_lake" {
  # Nombre viene de var.project_bucket_name
  bucket = var.project_bucket_name

  # Tags del bucket
  tags = {
    Name        = var.project_bucket_name
    Project     = "huella-hidrica"
    Environment = "dev"
  }
}

# -----------------------------------------
# 2) "Carpetas" internas (Medallion)
# -----------------------------------------

locals {
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

  tags = {
    Project     = "huella-hidrica"
    Environment = "dev"
    Layer       = each.value
  }
}

