provider "aws" {
    region = "us-west-1"
    shared_credentials_files = [ "/Users/joseromeromeza/.aws/credentials" ]
}

#define variables
locals {
  layer_path        = "lambda_layer"
  layer_zip_name    = "layer.zip"
  layer_name        = "lambda_layer_dev"
  requirements_name = "requirements.txt"
  requirements_path = "${path.module}/${local.layer_path}/${local.requirements_name}"
}

# create zip file from requirements.txt. Triggers only when the file is updated
resource "null_resource" "lambda_layer" {
  triggers = {
    requirements = filesha1(local.requirements_path)
  }
  # the command to install python and dependencies to the machine and zips
  provisioner "local-exec" {
    command = <<EOT
      cd ${local.layer_path}
      rm -rf python
      mkdir python
      pip3 install -r ${local.requirements_name} -t python/
      zip -r ${local.layer_zip_name} python/
    EOT
  }
}

# define existing bucket for storing lambda layers
resource "aws_s3_bucket" "lambda_layer" {
  bucket_prefix = "lambda-layer-dev"
}

# upload zip file to s3
resource "aws_s3_object" "lambda_layer_zip" {
  bucket     = aws_s3_bucket.lambda_layer.id
  key        = "lambda_layers/${local.layer_name}/${local.layer_zip_name}"
  source     = "${local.layer_path}/${local.layer_zip_name}"
  depends_on = [null_resource.lambda_layer] # triggered only if the zip file is created
}

# create lambda layer from s3 object
resource "aws_lambda_layer_version" "lambda_layer" {
  s3_bucket           = aws_s3_bucket.lambda_layer.id
  s3_key              = aws_s3_object.lambda_layer_zip.key
  layer_name          = local.layer_name
  compatible_runtimes = ["python3.8"]
  skip_destroy        = true
  depends_on          = [aws_s3_object.lambda_layer_zip] # triggered only if the zip file is uploaded to the bucket
}

data "archive_file" "zip_the_python_code" {
    type = "zip"
    source_dir = "${path.module}/src/"
    output_path = "${path.module}/src/lambda-statements.zip" 
}

resource "aws_lambda_function" "trfm_lambda_func" {
    filename = "${path.module}/src/lambda-statements.zip"
    function_name = "wattcher-data-statements"
    role = "arn:aws:iam::767398007326:role/lambda_statements_info"
    handler = "main.lambda_handler"
    layers           = [aws_lambda_layer_version.lambda_layer.arn]
    runtime = "python3.8"
    architectures = ["arm64"]
}

resource "aws_lambda_alias" "test_alias" {
  name             = "dev"
  description      = "version for dev environments"
  function_name    = aws_lambda_function.trfm_lambda_func.function_name
  function_version = "$LATEST"
}

output "terraform_aws_lambda" {
  value = aws_lambda_function.trfm_lambda_func.arn
}