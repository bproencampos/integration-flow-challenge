
# Faz com esse terraforme so funcione nessa versao
terraform {
  required_version = "1.5.7"
  required_providers {
    aws = { # identifica com base em qual provider foi provisionado
        source = "hashcorp/aws"
        version = "5.17.0"
    }
  }
}

provider "aws"{
    region = "sa-east-1"
}

resource "aws_s3_bucket" "my-bucket" {
  bucket = "flow-challenge-bucket"
  acl = "private"

  tags = {
    Name        = "My bucket"
    Environment = "Dev"
    Managedby = "Terraform"
  }
}