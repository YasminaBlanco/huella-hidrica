# ------------------------------------------
# Security Group para SSH (Spark)
# ------------------------------------------

resource "aws_security_group" "spark_ssh_sg" {
  name        = "pf-spark-ssh-sg"
  description = "SSH access for Spark EC2"
  vpc_id      = aws_default_vpc.default.id

  # SSH desde cualquier lugar (solo dev; en prod limitar IP)
  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
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
    Name        = "spark-ssh-sg"
    Project     = "huella-hidrica"
    Environment = "dev"
  }
}

# ------------------------------------------
# AMI Ubuntu 24.04 LTS en us-east-2
# ------------------------------------------

data "aws_ami" "ubuntu_24_04" {
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
# Instancia EC2 para Spark
# ---------------------------------------------

resource "aws_instance" "spark" {
  ami                    = data.aws_ami.ubuntu_24_04.id
  instance_type          = "m7i-flex.large"
  iam_instance_profile   = aws_iam_instance_profile.ec2_spark_s3_profile.name
  key_name               = var.ec2_key_name
  vpc_security_group_ids = [aws_security_group.spark_ssh_sg.id]

  # Disco raíz: 40 GB gp3
  root_block_device {
    volume_size           = 40
    volume_type           = "gp3"
    delete_on_termination = true
  }

  tags = {
    Name        = "spark"
    Project     = "huella-hidrica"
    Environment = "dev"
  }

  # Script de arranque (user_data): instala Docker + Docker Compose 
  user_data = <<-EOF
    #!/bin/bash
    set -xe

    echo "==== [Spark EC2] Inicio de user_data ===="

    # Actualizar paquetes
    sudo apt-get update -y

    echo "==== [Spark EC2] Instalando dependencias para Docker ===="
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

    echo "==== [Spark EC2] Instalando Docker Engine y Docker Compose plugin ===="
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

    # Permitir usar docker sin sudo (se aplica al siguiente login)
    sudo usermod -aG docker ubuntu || true

    echo "==== [Spark EC2] Verificando versiones de Docker y Docker Compose ===="
    echo ">> docker --version"
    docker --version || echo "Docker no disponible"

    echo ">> docker compose version"
    docker compose version || echo "Docker Compose no disponible"

    echo "==== [Spark EC2] Fin de user_data ===="
  EOF
}


