# ------------------------------------------
# Security Group para SSH + API (puerto 8000)
# ------------------------------------------

resource "aws_security_group" "api_ssh_sg" {
  name        = "pf-api-ssh-sg"
  description = "SSH and HTTP access for API EC2"
  vpc_id      = aws_default_vpc.default.id

  # SSH (solo dev; en prod limita la IP)
  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # HTTP para la API en 8000
  ingress {
    description = "HTTP API"
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name        = "api-ssh-sg"
    Project     = "huella-hidrica"
    Environment = "dev"
  }
}

# ------------------------------------------
# AMI Ubuntu 24.04 LTS en us-east-2
# ------------------------------------------

data "aws_ami" "ubuntu_24_04_api" {
  most_recent = true
  owners      = ["099720109477"] # Canonical (Ubuntu)

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-*-*"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  filter {
    name   = "architecture"
    values = ["x86_64"]
  }
}

# ---------------------------------------------
# Instancia EC2 para API con Docker
# ---------------------------------------------

resource "aws_instance" "api" {
  ami                    = data.aws_ami.ubuntu_24_04_api.id
  instance_type          = "t3.small"
  iam_instance_profile   = aws_iam_instance_profile.ec2_spark_s3_profile.name
  key_name               = var.ec2_key_name
  vpc_security_group_ids = [aws_security_group.api_ssh_sg.id]

  root_block_device {
    volume_size           = 40
    volume_type           = "gp3"
    delete_on_termination = true
  }

  tags = {
    Name        = "api"
    Project     = "huella-hidrica"
    Environment = "dev"
  }

  # user_data alineado con Spark/Airflow: instala Docker + Compose y loguea versiones
  user_data = <<-EOF
    #!/bin/bash
    set -xe

    echo "==== [API EC2] Inicio de user_data ===="

    # Actualizar paquetes
    sudo apt-get update -y

    echo "==== [API EC2] Instalando dependencias para Docker ===="
    sudo apt-get install -y ca-certificates curl gnupg lsb-release

    # Prepara keyrings para repositorio oficial de Docker
    sudo install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
      sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

    echo \
      "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
      https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | \
      sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

    sudo apt-get update -y

    echo "==== [API EC2] Instalando Docker Engine y Docker Compose plugin ===="
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

    # Permitir usar docker sin sudo (se aplica al siguiente login)
    sudo usermod -aG docker ubuntu || true

    echo "==== [API EC2] Verificando versiones de Docker y Docker Compose ===="
    echo ">> docker --version"
    docker --version || echo "Docker no disponible"

    echo ">> docker compose version"
    docker compose version || echo "Docker Compose no disponible"

    echo "==== [API EC2] Fin de user_data ===="
  EOF
}

