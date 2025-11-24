resource "aws_default_vpc" "default" {}

# ------------------------------------------
# Security Group para SSH
# ------------------------------------------

resource "aws_security_group" "spark_ssh_sg" {
  name        = "spark-ssh-sg"
  description = "SSH access for Spark EC2"
  vpc_id      = aws_default_vpc.default.id

  # SSH desde cualquier lugar 
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

  # Nombre de la imagen: Ubuntu 24.04 noble, SSD gp3
  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-*-*"]
  }

  # Virtualización HVM (estándar en EC2)
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  # Asegurar que sea arquitectura x86_64 (para c7i-flex.large)
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
  instance_type          = "c7i-flex.large"
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
}
