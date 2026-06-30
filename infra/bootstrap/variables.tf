variable "project_name" {
  description = "Name prefix for bootstrap resources."
  type        = string
  default     = "microcms-content-ops-analytics"
}

variable "environment" {
  description = "Environment name used in resource names and tags."
  type        = string
  default     = "dev"
}

variable "aws_region" {
  description = "AWS region for ECR repositories."
  type        = string
  default     = "ap-northeast-1"
}
