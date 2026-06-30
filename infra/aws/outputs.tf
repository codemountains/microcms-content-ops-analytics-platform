output "event_bucket_name" {
  description = "S3 bucket that stores Parquet event files."
  value       = aws_s3_bucket.events.bucket
}

output "events_path" {
  description = "S3 path consumed by duckdb-query-api."
  value       = local.events_path
}

output "webhook_url" {
  description = "Public API Gateway URL for microCMS webhook delivery."
  value       = "${aws_api_gateway_stage.webhook.invoke_url}/webhook"
}

output "query_api_url" {
  description = "Public ALB URL for duckdb-query-api."
  value       = "http://${aws_lb.query_api.dns_name}"
}
