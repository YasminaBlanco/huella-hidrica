
# ----------------------------------------
# Role IAM para EC2 con acceso S3 
# ---------------------------------------

resource "aws_iam_role" "ec2_spark_s3_role" {
  name = "ec2-spark-s3-role"

  # Permitir que EC2 asuma este role
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Name        = "ec2-spark-s3-role"
    Project     = "huella-hidrica"
    Environment = "dev"
  }
}

# Política personalizada de mínimo privilegio 
resource "aws_iam_policy" "ec2_spark_s3_least_priv" {
  name        = "ec2-spark-s3-least-priv"
  description = "Acceso mínimo desde EC2 (Spark) al bucket henry-pf-g2-huella-hidrica"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      # Permitir listar solo ese bucket y obtener su región
      {
        Sid    = "ListBucket"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = "arn:aws:s3:::henry-pf-g2-huella-hidrica"
      },

      # Permitir leer/escribir objetos solo en estos prefijos
      {
        Sid    = "RWDataObjects"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "arn:aws:s3:::henry-pf-g2-huella-hidrica/bronze/*",
          "arn:aws:s3:::henry-pf-g2-huella-hidrica/silver/*",
          "arn:aws:s3:::henry-pf-g2-huella-hidrica/gold/*",
          "arn:aws:s3:::henry-pf-g2-huella-hidrica/logs/*",
          "arn:aws:s3:::henry-pf-g2-huella-hidrica/scripts/*"
        ]
      }
    ]
  })
}

# Adjuntar la política personalizada al role
resource "aws_iam_role_policy_attachment" "ec2_spark_s3_attach" {
  role       = aws_iam_role.ec2_spark_s3_role.name
  policy_arn = aws_iam_policy.ec2_spark_s3_least_priv.arn
}

# Instance Profile que usará la instancia EC2
resource "aws_iam_instance_profile" "ec2_spark_s3_profile" {
  name = "ec2-spark-s3-role"
  role = aws_iam_role.ec2_spark_s3_role.name
}
