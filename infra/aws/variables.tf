variable "project_name" {
  description = "Name prefix for AWS resources."
  type        = string
  default     = "microcms-content-ops-analytics"
}

variable "environment" {
  description = "Environment name used in resource names and tags."
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS region for all resources."
  type        = string
  default     = "ap-northeast-1"
}

variable "event_bucket_name" {
  description = "S3 bucket name for Parquet event files. Leave empty to derive a name from project/environment/account."
  type        = string
  default     = ""
}

variable "event_prefix" {
  description = "S3 prefix for Parquet event files."
  type        = string
  default     = "microcms_events"
}

variable "microcms_webhook_secret" {
  description = "Secret used to verify x-microcms-signature."
  type        = string
  sensitive   = true
}

variable "webhook_ingest_image_uri" {
  description = "Container image URI for the Lambda webhook-ingest function."
  type        = string
}

variable "duckdb_query_api_image_uri" {
  description = "Container image URI for the ECS duckdb-query-api task."
  type        = string
}

variable "lambda_architecture" {
  description = "Lambda container image architecture. Use x86_64 when publishing amd64 images."
  type        = string
  default     = "arm64"

  validation {
    condition     = contains(["arm64", "x86_64"], var.lambda_architecture)
    error_message = "lambda_architecture must be arm64 or x86_64."
  }
}

variable "ecs_cpu_architecture" {
  description = "ECS Fargate task CPU architecture. Use X86_64 when publishing amd64 images."
  type        = string
  default     = "ARM64"

  validation {
    condition     = contains(["ARM64", "X86_64"], var.ecs_cpu_architecture)
    error_message = "ecs_cpu_architecture must be ARM64 or X86_64."
  }
}

variable "query_api_desired_count" {
  description = "Desired ECS task count for duckdb-query-api."
  type        = number
  default     = 1
}

variable "query_api_cpu" {
  description = "Fargate task CPU units."
  type        = number
  default     = 512
}

variable "query_api_memory" {
  description = "Fargate task memory in MiB."
  type        = number
  default     = 1024
}

variable "force_destroy_bucket" {
  description = "Allow Terraform/OpenTofu to destroy a non-empty event bucket. Keep false outside disposable environments."
  type        = bool
  default     = false
}
