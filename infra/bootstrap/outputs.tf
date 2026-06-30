output "webhook_ingest_repository_url" {
  description = "ECR repository URL for webhook-ingest images."
  value       = aws_ecr_repository.webhook_ingest.repository_url
}

output "duckdb_query_api_repository_url" {
  description = "ECR repository URL for duckdb-query-api images."
  value       = aws_ecr_repository.duckdb_query_api.repository_url
}
