set dotenv-load := true
set shell := ["bash", "-uc"]

aws_dir := "infra/aws"
bootstrap_dir := "infra/bootstrap"
local_dir := "infra/local"
compose := "docker compose"
local_compose := "docker compose -f docker-compose.local.yml"
aws_region := env("AWS_REGION", "ap-northeast-1")
aws_account_id := env("AWS_ACCOUNT_ID", "")
image_tag := env("IMAGE_TAG", "latest")
platform := env("DOCKER_PLATFORM", "linux/arm64")
cargo_build_jobs := env("CARGO_BUILD_JOBS", "1")
floci_port := env("FLOCI_PORT", "4566")
floci_endpoint := "http://localhost:" + floci_port
floci_lambda_endpoint := env("FLOCI_LAMBDA_ENDPOINT", "http://floci:4566")
debug_parquet_dir := env("DEBUG_PARQUET_DIR", ".debug/parquet")

default:
    @just --list

# Format Rust and OpenTofu files.
fmt:
    cargo fmt --all
    tofu fmt -recursive infra

# Run all Rust tests.
test:
    cargo test --workspace

# Run Rust lint checks.
clippy:
    cargo clippy --workspace --all-targets -- -D warnings

# Validate IaC, Compose, and Grafana dashboard JSON.
validate:
    tofu -chdir={{bootstrap_dir}} init -backend=false
    tofu -chdir={{bootstrap_dir}} validate
    tofu -chdir={{aws_dir}} init -backend=false
    tofu -chdir={{aws_dir}} validate
    tofu -chdir={{local_dir}} init -backend=false
    tofu -chdir={{local_dir}} validate
    env 'EVENTS_PATH=s3://example-bucket/microcms_events/**/*.parquet' {{compose}} config >/dev/null
    env NGROK_AUTHTOKEN="${NGROK_AUTHTOKEN:-dummy}" {{local_compose}} config >/dev/null
    jq empty grafana/dashboards/microcms-content-ops-analytics.json
    jq -e '.panels[] | select(.title == "Top Updated Contents") | .targets[].fields[] | select(.jsonPath == "$[*].count" and .name == "updated_count" and .type == "number")' grafana/dashboards/microcms-content-ops-analytics.json >/dev/null
    jq -e '.panels[] | select(.title == "Top Updated Contents") | .fieldConfig.overrides[] | select(.matcher.options == "last_event_at") | .properties[] | select(.id == "unit" and .value == "dateTimeAsLocal")' grafana/dashboards/microcms-content-ops-analytics.json >/dev/null

# Run the full local static verification suite.
check: fmt test clippy validate

# Generate a hard-to-guess webhook secret value.
secret:
    @openssl rand -hex 20

# Build the local container images used by Floci and the Query API.
debug-build:
    {{local_compose}} --profile build-images build webhook-ingest-image duckdb-query-api

# Start local support services only.
debug-up:
    {{local_compose}} up -d floci duckdb-query-api grafana ngrok

# Apply local Floci resources with OpenTofu.
debug-apply:
    tofu -chdir={{local_dir}} init
    TF_VAR_floci_endpoint="{{floci_endpoint}}" TF_VAR_floci_lambda_endpoint="{{floci_lambda_endpoint}}" TF_VAR_microcms_webhook_secret="${MICROCMS_WEBHOOK_SECRET:-local-webhook-secret}" tofu -chdir={{local_dir}} apply

# Build image, start Floci, apply local IaC, and start Query API/Grafana/ngrok.
debug: debug-build
    {{local_compose}} up -d floci
    tofu -chdir={{local_dir}} init
    TF_VAR_floci_endpoint="{{floci_endpoint}}" TF_VAR_floci_lambda_endpoint="{{floci_lambda_endpoint}}" TF_VAR_microcms_webhook_secret="${MICROCMS_WEBHOOK_SECRET:-local-webhook-secret}" tofu -chdir={{local_dir}} apply
    {{local_compose}} up -d duckdb-query-api grafana ngrok
    @just debug-outputs

# Show local OpenTofu outputs and ngrok tunnel metadata.
debug-outputs:
    tofu -chdir={{local_dir}} output
    @echo
    @echo "ngrok tunnels:"
    @curl -fsS http://localhost:4040/api/tunnels || (echo "ngrok API is not reachable. Run: docker compose -f docker-compose.local.yml logs ngrok" >&2; exit 1)

# Send a signed sample webhook directly to the local Floci API Gateway URL.
debug-webhook:
    payload='{"service":"example-service","api":"blogs","id":"content-id","type":"edit","contents":{"old":{"status":["DRAFT"],"updatedAt":"2026-06-28T12:00:00Z"},"new":{"status":["PUBLISH"],"updatedAt":"2026-06-29T12:00:00Z","publishValue":{"createdAt":"2026-06-27T12:00:00Z","publishedAt":"2026-06-29T12:00:00Z"}}}}'; \
    secret="${MICROCMS_WEBHOOK_SECRET:-local-webhook-secret}"; \
    signature="$(printf '%s' "$payload" | openssl dgst -sha256 -hmac "$secret" -binary | xxd -p -c 256)"; \
    webhook_url="$(tofu -chdir={{local_dir}} output -raw local_webhook_url)"; \
    curl -i "$webhook_url" \
      -H "content-type: application/json" \
      -H "x-microcms-signature: $signature" \
      --data "$payload"
    @just debug-parquet-persist

# Persist local Floci S3 Parquet objects to a git-ignored directory.
debug-parquet-persist:
    @mkdir -p "{{debug_parquet_dir}}"
    @bucket="$(tofu -chdir={{local_dir}} output -raw event_bucket_name)"; \
    AWS_ENDPOINT_URL={{floci_endpoint}} AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION={{aws_region}} \
      aws s3 sync \
        "s3://$bucket/microcms_events/" \
        "{{debug_parquet_dir}}/microcms_events/" \
        --exclude "*" \
        --include "*.parquet"; \
    echo "Persisted debug Parquet files under {{debug_parquet_dir}}/microcms_events/."

# Delete debug-generated Parquet files from local Floci S3 and the persisted directory.
debug-parquet-delete:
    @dir="{{debug_parquet_dir}}"; \
    case "$dir" in ""|"/"|"."|".."|"-"*) echo "Refusing to delete unsafe DEBUG_PARQUET_DIR: '$dir'" >&2; exit 1;; esac; \
    bucket="$(tofu -chdir={{local_dir}} output -raw event_bucket_name 2>/dev/null || true)"; \
    if [ -n "$bucket" ]; then \
      AWS_ENDPOINT_URL={{floci_endpoint}} AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION={{aws_region}} \
        aws s3 rm "s3://$bucket/microcms_events/" --recursive --exclude "*" --include "*.parquet" || true; \
    fi; \
    if [ -d "$dir" ]; then rm -rf "$dir"; fi; \
    echo "Deleted debug Parquet files from local Floci S3 and $dir."

# Call local Query API health and metrics endpoints.
debug-metrics:
    curl http://localhost:8000/health
    curl "http://localhost:8000/metrics/calendar-heatmap"
    curl "http://localhost:8000/metrics/api-activity?days=3660"
    curl "http://localhost:8000/metrics/top-updated-contents?days=3660&limit=20"
    curl "http://localhost:8000/metrics/average-time-to-publish-by-api?days=3660"

# List local Floci S3 event objects.
debug-s3-ls:
    @bucket="$(tofu -chdir={{local_dir}} output -raw event_bucket_name)"; \
    objects="$(AWS_ENDPOINT_URL={{floci_endpoint}} AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION={{aws_region}} \
      aws s3api list-objects-v2 \
        --bucket "$bucket" \
        --prefix microcms_events/ \
        --output json)" || exit 1; \
    if [ "$(printf '%s' "$objects" | jq '.Contents // [] | length')" = "0" ]; then \
      echo "No objects found under s3://$bucket/microcms_events/. Run 'just debug-webhook' first to create a sample event."; \
    else \
      printf 'LastModified\tSize\tKey\n'; \
      printf '%s' "$objects" | jq -r '.Contents[] | [.LastModified, (.Size | tostring), .Key] | @tsv'; \
    fi

# Stop local debug containers.
debug-down:
    {{local_compose}} down

# Destroy local Floci resources managed by OpenTofu.
debug-destroy:
    TF_VAR_floci_endpoint="{{floci_endpoint}}" TF_VAR_floci_lambda_endpoint="{{floci_lambda_endpoint}}" TF_VAR_microcms_webhook_secret="${MICROCMS_WEBHOOK_SECRET:-local-webhook-secret}" tofu -chdir={{local_dir}} destroy

# Apply the AWS bootstrap stack that creates ECR repositories.
bootstrap:
    tofu -chdir={{bootstrap_dir}} init
    tofu -chdir={{bootstrap_dir}} apply

# Show AWS bootstrap outputs.
bootstrap-outputs:
    tofu -chdir={{bootstrap_dir}} output

# Run an AWS OpenTofu plan using images from the bootstrap ECR repositories.
deploy-plan:
    tofu -chdir={{aws_dir}} init
    webhook_repo="$(tofu -chdir={{bootstrap_dir}} output -raw webhook_ingest_repository_url)"; \
    query_repo="$(tofu -chdir={{bootstrap_dir}} output -raw duckdb_query_api_repository_url)"; \
    tofu -chdir={{aws_dir}} plan \
      -var "webhook_ingest_image_uri=$webhook_repo:{{image_tag}}" \
      -var "duckdb_query_api_image_uri=$query_repo:{{image_tag}}"

# Apply the AWS OpenTofu stack using images from the bootstrap ECR repositories.
deploy:
    tofu -chdir={{aws_dir}} init
    webhook_repo="$(tofu -chdir={{bootstrap_dir}} output -raw webhook_ingest_repository_url)"; \
    query_repo="$(tofu -chdir={{bootstrap_dir}} output -raw duckdb_query_api_repository_url)"; \
    tofu -chdir={{aws_dir}} apply \
      -var "webhook_ingest_image_uri=$webhook_repo:{{image_tag}}" \
      -var "duckdb_query_api_image_uri=$query_repo:{{image_tag}}"

# Bootstrap ECR, build/push images, and deploy the AWS stack.
deploy-all: bootstrap ecr-login docker-build-aws docker-push-aws deploy

# Show AWS OpenTofu outputs.
deploy-outputs:
    tofu -chdir={{aws_dir}} output

# Destroy the AWS OpenTofu stack. This is intentionally separate from deploy.
deploy-destroy:
    tofu -chdir={{aws_dir}} destroy

# Destroy the AWS bootstrap stack. ECR repositories must be empty.
bootstrap-destroy:
    tofu -chdir={{bootstrap_dir}} destroy

# Log Docker in to the current AWS account's ECR registry.
ecr-login:
    account_id="{{aws_account_id}}"; \
    if [ -z "$account_id" ]; then account_id="$(aws sts get-caller-identity --query Account --output text)"; fi; \
    aws ecr get-login-password --region {{aws_region}} | \
      docker login --username AWS --password-stdin "$account_id.dkr.ecr.{{aws_region}}.amazonaws.com"

# Build production images for AWS using DOCKER_PLATFORM and IMAGE_TAG.
docker-build-aws:
    docker build --platform {{platform}} --build-arg CARGO_BUILD_JOBS={{cargo_build_jobs}} -f webhook-ingest/Dockerfile -t webhook-ingest:{{image_tag}} .
    docker build --platform {{platform}} --build-arg CARGO_BUILD_JOBS={{cargo_build_jobs}} -f duckdb-query-api/Dockerfile -t duckdb-query-api:{{image_tag}} .

# Tag and push production images to ECR.
docker-push-aws:
    webhook_repo="$(tofu -chdir={{bootstrap_dir}} output -raw webhook_ingest_repository_url)"; \
    query_repo="$(tofu -chdir={{bootstrap_dir}} output -raw duckdb_query_api_repository_url)"; \
    docker tag webhook-ingest:{{image_tag}} "$webhook_repo:{{image_tag}}"; \
    docker tag duckdb-query-api:{{image_tag}} "$query_repo:{{image_tag}}"; \
    docker push "$webhook_repo:{{image_tag}}"; \
    docker push "$query_repo:{{image_tag}}"
