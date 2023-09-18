
# Faz com esse terraforme so funcione nessa versao
terraform {
  required_version = "1.5.7"
}

# provider "aws"{
#     region = var.aws_region
# }

resource "aws_s3_bucket" "my-bucket" {
  bucket = "flow-challenge-bucket"
  acl = "private"

  tags = {
    Name        = "My bucket"
    Environment = "Dev"
    Managedby = "Terraform"
  }
}
