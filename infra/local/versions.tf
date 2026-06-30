terraform {
  required_version = ">= 1.9.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.80"
    }
  }
}

provider "aws" {
  region                      = var.aws_region
  access_key                  = "test"
  secret_key                  = "test"
  s3_use_path_style           = true
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    apigateway = var.floci_endpoint
    cloudwatch = var.floci_endpoint
    ecr        = var.floci_endpoint
    iam        = var.floci_endpoint
    lambda     = var.floci_endpoint
    logs       = var.floci_endpoint
    s3         = var.floci_endpoint
    sts        = var.floci_endpoint
  }
}
