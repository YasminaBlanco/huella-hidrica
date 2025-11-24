variable "aws_region" {
  description = "Región de AWS"
  type        = string
  default     = "us-east-2"
}

variable "project_bucket_name" {
  description = "Nombre del bucket raíz del data lake"
  type        = string
  # nombre del proyecto
  default = "henry-pf-g2-huella-hidrica-dev"
}

# Nombre del Key Pair para conectarte por SSH a la instancia
variable "ec2_key_name" {
  description = "Nombre del par de claves (Key Pair) para SSH"
  type        = string
  default     = "huella-spark-key"
}