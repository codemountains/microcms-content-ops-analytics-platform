locals {
  webhook_function_name = "${var.project_name}-local-webhook-ingest"
  stage_name            = "local"
}

resource "aws_s3_bucket" "events" {
  bucket        = var.event_bucket_name
  force_destroy = true
}

resource "aws_iam_role" "webhook_ingest" {
  name = "${local.webhook_function_name}-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "webhook_ingest_s3" {
  name = "${local.webhook_function_name}-s3"
  role = aws_iam_role.webhook_ingest.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:PutObject"]
      Resource = "${aws_s3_bucket.events.arn}/${var.event_prefix}/*"
    }]
  })
}

resource "aws_lambda_function" "webhook_ingest" {
  function_name = local.webhook_function_name
  package_type  = "Image"
  image_uri     = var.webhook_ingest_image_uri
  role          = aws_iam_role.webhook_ingest.arn
  timeout       = 30
  memory_size   = 512

  environment {
    variables = {
      AWS_ENDPOINT_URL        = var.floci_lambda_endpoint
      AWS_REGION              = var.aws_region
      AWS_S3_FORCE_PATH_STYLE = "true"
      EVENT_BUCKET            = aws_s3_bucket.events.bucket
      EVENT_PREFIX            = var.event_prefix
      MICROCMS_WEBHOOK_SECRET = var.microcms_webhook_secret
    }
  }

  depends_on = [aws_iam_role_policy.webhook_ingest_s3]
}

resource "aws_api_gateway_rest_api" "webhook" {
  name = "${var.project_name}-local-webhook"
}

resource "aws_api_gateway_resource" "webhook" {
  rest_api_id = aws_api_gateway_rest_api.webhook.id
  parent_id   = aws_api_gateway_rest_api.webhook.root_resource_id
  path_part   = "webhook"
}

resource "aws_api_gateway_method" "webhook_post" {
  rest_api_id   = aws_api_gateway_rest_api.webhook.id
  resource_id   = aws_api_gateway_resource.webhook.id
  http_method   = "POST"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "webhook_lambda" {
  rest_api_id             = aws_api_gateway_rest_api.webhook.id
  resource_id             = aws_api_gateway_resource.webhook.id
  http_method             = aws_api_gateway_method.webhook_post.http_method
  integration_http_method = "POST"
  type                    = "AWS_PROXY"
  uri                     = aws_lambda_function.webhook_ingest.invoke_arn
}

resource "aws_lambda_permission" "apigw_webhook" {
  statement_id  = "AllowExecutionFromApiGateway"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.webhook_ingest.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_api_gateway_rest_api.webhook.execution_arn}/*/*"
}

resource "aws_api_gateway_deployment" "webhook" {
  rest_api_id = aws_api_gateway_rest_api.webhook.id

  triggers = {
    redeployment = sha1(jsonencode([
      aws_api_gateway_resource.webhook.id,
      aws_api_gateway_method.webhook_post.id,
      aws_api_gateway_integration.webhook_lambda.id,
    ]))
  }

  lifecycle {
    create_before_destroy = true
  }

  depends_on = [aws_api_gateway_integration.webhook_lambda]
}

resource "aws_api_gateway_stage" "webhook" {
  deployment_id = aws_api_gateway_deployment.webhook.id
  rest_api_id   = aws_api_gateway_rest_api.webhook.id
  stage_name    = local.stage_name
}
