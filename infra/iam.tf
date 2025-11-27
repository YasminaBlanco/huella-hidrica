# 1. Creación del Rol de IAM para la EC2 (CORREGIDO: me faltó el "_iam_role" en linea 2)
resource "aws_iam_role" "spark_processor_role" {
  name = "henry_spark_processor_role_dev"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ec2.amazonaws.com"
      }
    }]
  })

  # INTEGRACIÓN: Agrego los tags de Lupita (Best Practice)
  tags = {
    Name        = "henry_spark_processor_role_dev"
    Project     = "huella-hidrica"
    Environment = "dev"
  }
}

# 2. Dar permisos FULL sobre S3
resource "aws_iam_role_policy_attachment" "s3_access" {
  role       = aws_iam_role.spark_processor_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

# 3. Creación del "Perfil de Instancia"
resource "aws_iam_instance_profile" "spark_profile" {
  name = "henry_spark_profile_dev"
  role = aws_iam_role.spark_processor_role.name
}