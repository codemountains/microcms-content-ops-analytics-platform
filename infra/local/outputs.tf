output "event_bucket_name" {
  description = "Local S3 bucket that stores Parquet event files."
  value       = aws_s3_bucket.events.bucket
}

output "events_path" {
  description = "S3 path consumed by duckdb-query-api during local debugging."
  value       = "s3://${aws_s3_bucket.events.bucket}/${var.event_prefix}/**/*.parquet"
}

output "local_webhook_url" {
  description = "Local Floci API Gateway URL for direct webhook testing."
  value       = "${var.floci_endpoint}/restapis/${aws_api_gateway_rest_api.webhook.id}/${local.stage_name}/_user_request_/webhook"
}

output "local_webhook_path" {
  description = "Path to append to an ngrok public URL that forwards to Floci."
  value       = "/restapis/${aws_api_gateway_rest_api.webhook.id}/${local.stage_name}/_user_request_/webhook"
}

output "ngrok_target" {
  description = "Local target that ngrok should tunnel."
  value       = "http://floci:4566"
}
