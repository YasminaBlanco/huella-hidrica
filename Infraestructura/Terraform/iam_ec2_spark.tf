
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

# Adjuntar política AmazonS3FullAccess
resource "aws_iam_role_policy_attachment" "ec2_spark_s3_full_access" {
  role       = aws_iam_role.ec2_spark_s3_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

# Instance Profile que usará la instancia EC2
resource "aws_iam_instance_profile" "ec2_spark_s3_profile" {
  name = "ec2-spark-s3-role"
  role = aws_iam_role.ec2_spark_s3_role.name
}
