variable "project_name" {
  description = "Name prefix for local resources."
  type        = string
  default     = "microcms-content-ops-analytics"
}

variable "aws_region" {
  description = "AWS region value used by local emulation."
  type        = string
  default     = "ap-northeast-1"
}

variable "floci_endpoint" {
  description = "Floci AWS emulator endpoint reachable from the host running OpenTofu and AWS CLI."
  type        = string
  default     = "http://localhost:4566"
}

variable "floci_lambda_endpoint" {
  description = "Floci AWS emulator endpoint reachable from Lambda containers spawned by Floci."
  type        = string
  default     = "http://floci:4566"
}

variable "event_bucket_name" {
  description = "Local S3 bucket name for Parquet event files."
  type        = string
  default     = "microcms-content-ops-events-local"
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
  default     = "local-webhook-secret"
}

variable "webhook_ingest_image_uri" {
  description = "Local image URI used by the emulated Lambda runtime."
  type        = string
  default     = "webhook-ingest:local"
}
