terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = "ap-south-1"
}

variable "stop" {
  description = "Stop the instances"
  type        = bool
  default     = false
}

variable "server_count" {
  description = "Number of server instances"
  type        = number
  default     = 1
}

variable "client_count" {
  description = "Number of client instances"
  type        = number
  default     = 0
}

data "aws_ami" "ubuntu" {
  most_recent = true

  filter {
    name = "name"
    # values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-20251001"]
  }

  owners = ["099720109477"] # Canonical
}

output "ubuntu_ami" {
  value = data.aws_ami.ubuntu
}

resource "aws_vpc" "main" {
  cidr_block           = "10.0.0.0/16"
  enable_dns_hostnames = true
}

resource "aws_subnet" "main" {
  vpc_id                  = resource.aws_vpc.main.id
  cidr_block              = "10.0.0.0/16"
  map_public_ip_on_launch = true
}

resource "aws_internet_gateway" "main" {
  vpc_id = resource.aws_vpc.main.id
}

resource "aws_route_table" "main" {
  vpc_id = resource.aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = resource.aws_internet_gateway.main.id
  }
}

resource "aws_route_table_association" "_1" {
  route_table_id = resource.aws_route_table.main.id
  subnet_id      = resource.aws_subnet.main.id
}

resource "aws_security_group" "main" {
  vpc_id = resource.aws_vpc.main.id

  ingress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }
}

resource "aws_key_pair" "main" {
  public_key = file("~/.ssh/aws.pub")
}

resource "aws_instance" "build" {
  ami                    = data.aws_ami.ubuntu.id
  instance_type          = "c6a.4xlarge"
  subnet_id              = resource.aws_subnet.main.id
  vpc_security_group_ids = [resource.aws_security_group.main.id]
  key_name               = aws_key_pair.main.key_name

  root_block_device {
    volume_size = 100
  }
}

resource "aws_instance" "servers" {
  count = var.server_count

  ami                    = data.aws_ami.ubuntu.id
  instance_type          = "m5ad.2xlarge"
  subnet_id              = resource.aws_subnet.main.id
  vpc_security_group_ids = [resource.aws_security_group.main.id]
  key_name               = aws_key_pair.main.key_name
}

resource "aws_instance" "clients" {
  count = var.client_count

  ami                    = data.aws_ami.ubuntu.id
  instance_type          = "c6a.large"
  subnet_id              = resource.aws_subnet.main.id
  vpc_security_group_ids = [resource.aws_security_group.main.id]
  key_name               = aws_key_pair.main.key_name
}

resource "aws_ec2_instance_state" "build" {
  instance_id = aws_instance.build.id
  state       = var.stop ? "stopped" : "running"
}

resource "aws_ec2_instance_state" "servers" {
  count       = var.server_count
  instance_id = aws_instance.servers[count.index].id
  state       = var.stop ? "stopped" : "running"
}

resource "aws_ec2_instance_state" "clients" {
  count       = var.client_count
  instance_id = aws_instance.clients[count.index].id
  state       = var.stop ? "stopped" : "running"
}

output "build" {
  value = aws_instance.build
}

output "servers" {
  value = aws_instance.servers.*
}

output "clients" {
  value = aws_instance.clients.*
}
