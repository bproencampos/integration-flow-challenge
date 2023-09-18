provider "aws"{
    region = var.aws_region
}

resource "aws_iam_policy" "lambda_policy" {
  name        = "tf_aws_lambda_policy"
  description = "Policy for AWS Lambda function"
  path        = "/"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ],
        Resource = [
          "arn:aws:s3:::flow-challenge-bucket",         # ARN do bucket S3
          "arn:aws:s3:::flow-challenge-bucket/*"        # ARN de objetos no bucket S3
        ],
        Effect = "Allow"
      },
      {
        Action = [
          "rds-db:connect"
        ],
        Resource = "*",
        Effect = "Allow"
      },
      {
        Action = [
          "secretsmanager:GetSecretValue"
        ],
        Resource = "arn:aws:secretsmanager:sa-east-1:230973582134:secret:rds!db-e240d9d9-80dc-4156-8c85-e6e6ed08459f-dassur",  # ARN do seu segredo no Secrets Manager
        Effect = "Allow"
      }
    ]
  })
}

resource "aws_iam_role" "lambda_role" {
  name = "tf_lambda_execution_role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Action = "sts:AssumeRole",
        Effect = "Allow",
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

# Anexe políticas à função IAM
resource "aws_iam_policy_attachment" "lambda_permissions" {
  name       = "lambda_permissions"
  policy_arn = aws_iam_policy.lambda_policy.arn
  roles      = [aws_iam_role.lambda_role.name]
}

data "archive_file" "zip_python_code" {
    type = "zip"
    source_dir = "${path.module}/python/"
    output_path = "${path.module}/python/lambda_data.zip"
}

resource "aws_lambda_function" "my-lambda" {
  function_name = "lambda-data"
  filename = "${path.module}/python/lambda_data.zip"
  role = aws_iam_role.lambda_role.arn
  handler = "lambda_data.lambda_handler"
  runtime = "python3.8"
  depends_on = [ aws_iam_policy_attachment.lambda_permissions ]
}
