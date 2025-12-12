variable "aws_region" {
  description = "Región de AWS donde se desplegará la infraestructura"
  type        = string
  default     = "us-east-2"
}

variable "project_bucket_name" {
  description = "Nombre del bucket raíz del data lake"
  type        = string
  default     = "henry-pf-g2-huella-hidrica"
}

variable "ec2_key_name" {
  description = "Nombre del par de claves (Key Pair) para conectarse por SSH a las instancias EC2"
  type        = string
  default     = "spark-ap"
}
