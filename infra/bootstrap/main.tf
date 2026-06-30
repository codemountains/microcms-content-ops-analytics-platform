locals {
  name_prefix = "${var.project_name}-${var.environment}"

  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "opentofu"
    Stack       = "bootstrap"
  }
}

resource "aws_ecr_repository" "webhook_ingest" {
  name                 = "${local.name_prefix}-webhook-ingest"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = local.tags
}

resource "aws_ecr_repository" "duckdb_query_api" {
  name                 = "${local.name_prefix}-duckdb-query-api"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = local.tags
}
