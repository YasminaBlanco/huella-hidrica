# ============================================================
# IAM para la instancia EC2 de Airflow
# - Role propio para Airflow
# - Reutiliza la policy de S3 (ec2_spark_s3_least_priv)
# - Policy extra para prender/apagar las instancias 
# ============================================================

#---------------------------------------------
# IAM Role para EC2 (Airflow)
#---------------------------------------------

resource "aws_iam_role" "ec2_airflow_s3_role" {
  name = "pf-ec2-airflow-s3-role"

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
    Name        = "ec2-airflow-s3-role"
    Project     = "huella-hidrica"
    Environment = "dev"
  }
}

#---------------------------------------------
# Adjuntar policy de S3 \
#---------------------------------------------

resource "aws_iam_role_policy_attachment" "ec2_airflow_s3_attach" {
  role       = aws_iam_role.ec2_airflow_s3_role.name
  policy_arn = aws_iam_policy.ec2_spark_s3_least_priv.arn
}

#---------------------------------------------
# Policy control de EC2 para Airflow
#---------------------------------------------

resource "aws_iam_policy" "airflow_ec2_control" {
  name        = "airflow-ec2-control"
  description = "Permite a Airflow encender/apagar instancias Spark y API"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "DescribeInstances"
        Effect = "Allow"
        Action = [
          "ec2:DescribeInstances",
          "ec2:DescribeInstanceStatus"
        ]
        Resource = "*"
      },
      {
        Sid    = "StartStopSparkApi"
        Effect = "Allow"
        Action = [
          "ec2:StartInstances",
          "ec2:StopInstances"
        ]
        # instancias
        Resource = [
          aws_instance.spark.arn,
          aws_instance.api.arn
        ]
      }
    ]
  })
}

# Adjuntar la policy de control EC2 solo al role de Airflow
resource "aws_iam_role_policy_attachment" "ec2_airflow_ec2_control_attach" {
  role       = aws_iam_role.ec2_airflow_s3_role.name
  policy_arn = aws_iam_policy.airflow_ec2_control.arn
}

#---------------------------------------------
# Instance Profile para usar en la EC2 de Airflow
#---------------------------------------------

resource "aws_iam_instance_profile" "ec2_airflow_s3_profile" {
  name = "pf-ec2-airflow-s3-profile"
  role = aws_iam_role.ec2_airflow_s3_role.name
}
