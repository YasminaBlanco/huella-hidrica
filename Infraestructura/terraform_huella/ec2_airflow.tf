# ------------------------------------------
# Security Group para Airflow 
# ------------------------------------------

resource "aws_security_group" "airflow_ssh_sg" {
  name        = "airflow-ssh-sg"
  description = "SSH and Airflow Web UI access for Airflow EC2"
  vpc_id      = aws_default_vpc.default.id

  # SSH
  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Airflow Web UI (8080)
  ingress {
    description = "Airflow Web UI"
    from_port   = 8080
    to_port     = 8080
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
    Name        = "airflow-ssh-sg"
    Project     = "huella-hidrica"
    Environment = "dev"
  }
}

# ---------------------------------------------
# Instancia EC2 para Airflow
# ---------------------------------------------

resource "aws_instance" "airflow" {
  # AMI Ubuntu 24.04 que Spark
  ami                    = data.aws_ami.ubuntu_24_04.id
  instance_type          = "c7i-flex.large"
  iam_instance_profile   = aws_iam_instance_profile.ec2_airflow_s3_profile.name
  key_name               = var.ec2_key_name
  vpc_security_group_ids = [aws_security_group.airflow_ssh_sg.id]

  # Disco raíz
  root_block_device {
    volume_size           = 40
    volume_type           = "gp3"
    delete_on_termination = true
  }

  tags = {
    Name        = "airflow"
    Project     = "huella-hidrica"
    Environment = "dev"
  }

  # Script de arranque (user_data): instala Docker y Docker Compose V2
  user_data = <<-EOF
    #!/bin/bash
    set -xe

    echo "==== [Airflow EC2] Inicio de user_data ===="

    # Actualizar paquetes
    sudo apt-get update -y

    echo "==== Instalando dependencias para Docker ===="
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

    echo "==== Instalando Docker Engine y Docker Compose plugin ===="
    sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

    # Permitir usar docker sin sudo (se aplica al siguiente login)
    sudo usermod -aG docker ubuntu || true

    echo "==== Verificando versiones de Docker y Docker Compose ===="
    echo ">> docker --version"
    docker --version || echo "Docker no disponible"

    echo ">> docker compose version"
    docker compose version || echo "Docker Compose no disponible"

    echo "==== [Airflow EC2] Fin de user_data ===="
  EOF
}
