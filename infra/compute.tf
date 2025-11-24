# 1. PREGUNTO a AWS por la última AMI de Ubuntu para poder levantar la instancia de Spark en AWS
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # ID de Canonical (Dueños de Ubuntu)

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# 2. Crear el Servidor
resource "aws_instance" "spark_server" {
  # Uso el ID dinámico que enconté arriba
  ami           = data.aws_ami.ubuntu.id 
  
  # Cambié de t3.medium a m7i-flex.large (Capa Gratuita)
  instance_type = "m7i-flex.large" 
  
  iam_instance_profile = aws_iam_instance_profile.spark_profile.name

  tags = {
    Name = "Servidor-Spark-Docker"
  }

  user_data = <<-EOF
              #!/bin/bash
              sudo apt-get update
              sudo apt-get install -y docker.io docker-compose
              sudo systemctl start docker
              sudo systemctl enable docker
              sudo usermod -aG docker ubuntu
              EOF
}