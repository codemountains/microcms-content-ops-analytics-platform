data "aws_caller_identity" "current" {}
data "aws_availability_zones" "available" {
  state = "available"
}

locals {
  name_prefix       = "${var.project_name}-${var.environment}"
  bucket_name       = var.event_bucket_name != "" ? var.event_bucket_name : "${local.name_prefix}-${data.aws_caller_identity.current.account_id}"
  events_path       = "s3://${aws_s3_bucket.events.bucket}/${var.event_prefix}/**/*.parquet"
  container_port    = 8000
  availability_zone = slice(data.aws_availability_zones.available.names, 0, 2)

  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "opentofu"
  }
}

resource "aws_s3_bucket" "events" {
  bucket        = local.bucket_name
  force_destroy = var.force_destroy_bucket
  tags          = local.tags
}

resource "aws_s3_bucket_public_access_block" "events" {
  bucket                  = aws_s3_bucket.events.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "events" {
  bucket = aws_s3_bucket.events.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_cloudwatch_log_group" "webhook_ingest" {
  name              = "/aws/lambda/${local.name_prefix}-webhook-ingest"
  retention_in_days = 14
  tags              = local.tags
}

resource "aws_iam_role" "webhook_ingest" {
  name = "${local.name_prefix}-webhook-ingest-role"

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

  tags = local.tags
}

resource "aws_iam_role_policy_attachment" "webhook_ingest_basic" {
  role       = aws_iam_role.webhook_ingest.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "webhook_ingest_s3" {
  name = "${local.name_prefix}-webhook-ingest-s3"
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
  function_name = "${local.name_prefix}-webhook-ingest"
  package_type  = "Image"
  image_uri     = var.webhook_ingest_image_uri
  role          = aws_iam_role.webhook_ingest.arn
  architectures = [var.lambda_architecture]
  timeout       = 30
  memory_size   = 512

  environment {
    variables = {
      EVENT_BUCKET            = aws_s3_bucket.events.bucket
      EVENT_PREFIX            = var.event_prefix
      MICROCMS_WEBHOOK_SECRET = var.microcms_webhook_secret
      AWS_LAMBDA_LOG_FORMAT   = "Text"
      AWS_LAMBDA_LOG_LEVEL    = "INFO"
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.webhook_ingest,
    aws_iam_role_policy_attachment.webhook_ingest_basic,
    aws_iam_role_policy.webhook_ingest_s3,
  ]

  tags = local.tags
}

resource "aws_api_gateway_rest_api" "webhook" {
  name = "${local.name_prefix}-webhook"
  tags = local.tags
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
  stage_name    = var.environment
  tags          = local.tags
}

resource "aws_vpc" "main" {
  cidr_block           = "10.42.0.0/16"
  enable_dns_hostnames = true
  enable_dns_support   = true
  tags                 = merge(local.tags, { Name = "${local.name_prefix}-vpc" })
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = merge(local.tags, { Name = "${local.name_prefix}-igw" })
}

resource "aws_subnet" "public" {
  count = 2

  vpc_id                  = aws_vpc.main.id
  cidr_block              = cidrsubnet(aws_vpc.main.cidr_block, 8, count.index)
  availability_zone       = local.availability_zone[count.index]
  map_public_ip_on_launch = true

  tags = merge(local.tags, { Name = "${local.name_prefix}-public-${count.index + 1}" })
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id
  tags   = merge(local.tags, { Name = "${local.name_prefix}-public" })
}

resource "aws_route" "public_internet" {
  route_table_id         = aws_route_table.public.id
  destination_cidr_block = "0.0.0.0/0"
  gateway_id             = aws_internet_gateway.main.id
}

resource "aws_route_table_association" "public" {
  count = length(aws_subnet.public)

  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "alb" {
  name        = "${local.name_prefix}-alb"
  description = "Public ALB for duckdb-query-api"
  vpc_id      = aws_vpc.main.id
  tags        = local.tags
}

resource "aws_vpc_security_group_ingress_rule" "alb_http" {
  security_group_id = aws_security_group.alb.id
  cidr_ipv4         = "0.0.0.0/0"
  from_port         = 80
  ip_protocol       = "tcp"
  to_port           = 80
}

resource "aws_vpc_security_group_egress_rule" "alb_all" {
  security_group_id = aws_security_group.alb.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_security_group" "query_api" {
  name        = "${local.name_prefix}-query-api"
  description = "duckdb-query-api ECS tasks"
  vpc_id      = aws_vpc.main.id
  tags        = local.tags
}

resource "aws_vpc_security_group_ingress_rule" "query_api_from_alb" {
  security_group_id            = aws_security_group.query_api.id
  referenced_security_group_id = aws_security_group.alb.id
  from_port                    = local.container_port
  ip_protocol                  = "tcp"
  to_port                      = local.container_port
}

resource "aws_vpc_security_group_egress_rule" "query_api_all" {
  security_group_id = aws_security_group.query_api.id
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

resource "aws_lb" "query_api" {
  name               = substr(replace("${local.name_prefix}-query-api", "_", "-"), 0, 32)
  load_balancer_type = "application"
  internal           = false
  security_groups    = [aws_security_group.alb.id]
  subnets            = aws_subnet.public[*].id
  tags               = local.tags
}

resource "aws_lb_target_group" "query_api" {
  name        = substr(replace("${local.name_prefix}-query-api", "_", "-"), 0, 32)
  port        = local.container_port
  protocol    = "HTTP"
  target_type = "ip"
  vpc_id      = aws_vpc.main.id

  health_check {
    enabled             = true
    healthy_threshold   = 2
    interval            = 30
    matcher             = "200"
    path                = "/health"
    timeout             = 5
    unhealthy_threshold = 3
  }

  tags = local.tags
}

resource "aws_lb_listener" "query_api_http" {
  load_balancer_arn = aws_lb.query_api.arn
  port              = 80
  protocol          = "HTTP"

  default_action {
    type             = "forward"
    target_group_arn = aws_lb_target_group.query_api.arn
  }

  tags = local.tags
}

resource "aws_ecs_cluster" "query_api" {
  name = "${local.name_prefix}-query-api"
  tags = local.tags
}

resource "aws_cloudwatch_log_group" "query_api" {
  name              = "/ecs/${local.name_prefix}-duckdb-query-api"
  retention_in_days = 14
  tags              = local.tags
}

resource "aws_iam_role" "ecs_task_execution" {
  name = "${local.name_prefix}-ecs-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ecs-tasks.amazonaws.com"
      }
    }]
  })

  tags = local.tags
}

resource "aws_iam_role_policy_attachment" "ecs_task_execution" {
  role       = aws_iam_role.ecs_task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "query_api_task" {
  name = "${local.name_prefix}-query-api-task-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ecs-tasks.amazonaws.com"
      }
    }]
  })

  tags = local.tags
}

resource "aws_iam_role_policy" "query_api_s3" {
  name = "${local.name_prefix}-query-api-s3"
  role = aws_iam_role.query_api_task.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = "${aws_s3_bucket.events.arn}/${var.event_prefix}/*"
      },
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = aws_s3_bucket.events.arn
        Condition = {
          StringLike = {
            "s3:prefix" = ["${var.event_prefix}/*"]
          }
        }
      }
    ]
  })
}

resource "aws_ecs_task_definition" "query_api" {
  family                   = "${local.name_prefix}-duckdb-query-api"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.query_api_cpu
  memory                   = var.query_api_memory
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.query_api_task.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = var.ecs_cpu_architecture
  }

  container_definitions = jsonencode([{
    name      = "duckdb-query-api"
    image     = var.duckdb_query_api_image_uri
    essential = true
    portMappings = [{
      containerPort = local.container_port
      protocol      = "tcp"
    }]
    environment = [
      { name = "EVENTS_PATH", value = local.events_path },
      { name = "AWS_REGION", value = var.aws_region },
      { name = "PORT", value = tostring(local.container_port) },
      { name = "DUCKDB_EXTENSION_DIRECTORY", value = "/tmp/duckdb_extensions" }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.query_api.name
        awslogs-region        = var.aws_region
        awslogs-stream-prefix = "ecs"
      }
    }
  }])

  tags = local.tags
}

resource "aws_ecs_service" "query_api" {
  name            = "${local.name_prefix}-duckdb-query-api"
  cluster         = aws_ecs_cluster.query_api.id
  task_definition = aws_ecs_task_definition.query_api.arn
  desired_count   = var.query_api_desired_count
  launch_type     = "FARGATE"

  network_configuration {
    assign_public_ip = true
    security_groups  = [aws_security_group.query_api.id]
    subnets          = aws_subnet.public[*].id
  }

  load_balancer {
    target_group_arn = aws_lb_target_group.query_api.arn
    container_name   = "duckdb-query-api"
    container_port   = local.container_port
  }

  depends_on = [aws_lb_listener.query_api_http]
  tags       = local.tags
}
