resource "aws_s3_bucket" "data_lake" {
  # Uso un nombre nuevo para desarrollo (que no se haya usado)
  bucket = "henry-pf-g2-huella-hidrica-dev-ale" 
  
  tags = {
    Environment = "Dev"
    Project     = "Huella Hidrica"
  }
}

# Crear carpetas vacías (objetos placeholder) para mantener el orden
resource "aws_s3_object" "folders" {
  for_each = toset(["bronze/", "silver/", "gold/", "scripts/", "logs/"])
  
  bucket = aws_s3_bucket.data_lake.id
  key    = each.key
}