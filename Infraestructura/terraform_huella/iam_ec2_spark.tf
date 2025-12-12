#---------------------------------------------
# IAM Role para EC2 (Spark)
#---------------------------------------------

resource "aws_iam_role" "ec2_spark_s3_role" {
  name = "pf-ec2-spark-s3-role"

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

#---------------------------------------------
# Política de mínimo privilegio hacia el Data Lake
#---------------------------------------------

resource "aws_iam_policy" "ec2_spark_s3_least_priv" {
  name        = "pf-ec2-spark-s3-least-priv"
  description = "Acceso mínimo desde EC2 (Spark/API) al Data Lake en S3"

  # usamos aws_s3_bucket.data_lake.arn
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ListBucket"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = aws_s3_bucket.data_lake.arn
      },
      {
        Sid    = "DataLakeObjects"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = [
          "${aws_s3_bucket.data_lake.arn}/bronze/*",
          "${aws_s3_bucket.data_lake.arn}/silver/*",
          "${aws_s3_bucket.data_lake.arn}/gold/*",
          "${aws_s3_bucket.data_lake.arn}/logs/*",
          "${aws_s3_bucket.data_lake.arn}/scripts/*"
        ]
      }
    ]
  })
}

#---------------------------------------------
# Adjuntar la policy al Role
#---------------------------------------------

resource "aws_iam_role_policy_attachment" "ec2_spark_s3_attach" {
  role       = aws_iam_role.ec2_spark_s3_role.name
  policy_arn = aws_iam_policy.ec2_spark_s3_least_priv.arn
}

#---------------------------------------------
# Instance Profile para usar en las EC2
#---------------------------------------------

resource "aws_iam_instance_profile" "ec2_spark_s3_profile" {
  name = "pf-ec2-spark-s3-profile-v2"
  role = aws_iam_role.ec2_spark_s3_role.name
}

