# AWS デプロイ手順

この手順では、OpenTofu と `just deploy-all` を使って、初回から AWS 環境へデプロイします。

デプロイされる構成:

- API Gateway REST API
- Lambda / `webhook-ingest`
- S3 / Parquet event bucket
- ECS Fargate / `duckdb-query-api`
- public ALB HTTP endpoint
- ECR repositories
- IAM roles / policies
- CloudWatch Logs
- VPC / public subnets / security groups

Grafana は AWS にはデプロイしません。ローカル Grafana から `query_api_url` を見る前提です。

## 前提

- AWS CLI
- Docker / Docker Compose
- OpenTofu
- just
- jq
- 対象 AWS account で、ECR/Lambda/ECS/VPC/IAM/API Gateway/S3/CloudWatch Logs/ALB を作成できる権限

AWS credentials が有効であることを確認します。

```bash
aws sts get-caller-identity
```

## 1. AWS 用 tfvars を用意する

```bash
cp infra/aws/terraform.tfvars.example infra/aws/terraform.tfvars
```

最低限、`microcms_webhook_secret` を実際の microCMS Webhook secret に変更します。

```hcl
microcms_webhook_secret = "replace-with-real-secret"
```

必要に応じて変更する値:

| 変数 | 用途 |
| --- | --- |
| `project_name` | AWS resource name prefix |
| `environment` | `dev`, `stg`, `prod` など |
| `aws_region` | deploy region |
| `event_bucket_name` | S3 bucket name を明示したい場合 |
| `lambda_architecture` | `arm64` または `x86_64` |
| `ecs_cpu_architecture` | `ARM64` または `X86_64` |

Apple Silicon のローカル Docker で build する場合、既定の `arm64` / `ARM64` のままで問題ありません。
amd64 image を push する場合は、次のように合わせます。

```hcl
lambda_architecture  = "x86_64"
ecs_cpu_architecture = "X86_64"
```

```bash
export DOCKER_PLATFORM=linux/amd64
```

## 2. ワンコマンドで初回デプロイする

```bash
just deploy-all
```

`just deploy-all` は次を順に実行します。

1. `infra/bootstrap` で ECR repositories を作成
2. ECR login
3. `webhook-ingest` と `duckdb-query-api` の image build
4. ECR へ image push
5. `infra/aws` で API Gateway、Lambda、S3、ECS Fargate、ALB などを作成

image tag を明示したい場合:

```bash
IMAGE_TAG=$(git rev-parse --short HEAD) just deploy-all
```

## 3. 出力を確認する

```bash
just deploy-outputs
```

主な output:

| Output | 用途 |
| --- | --- |
| `webhook_url` | microCMS Webhook の送信先 |
| `query_api_url` | DuckDB Query API の public ALB HTTP endpoint |
| `event_bucket_name` | Parquet 保存先 S3 bucket |
| `events_path` | Query API が読む S3 path |

## 4. microCMS Webhook を設定する

microCMS 管理画面で Webhook URL に `webhook_url` を設定します。

Header:

```text
x-microcms-signature
```

署名 secret は `microcms_webhook_secret` と同じ値にします。

このプロジェクトの Webhook endpoint は API Gateway URL を使います。独自ドメインは不要です。

## 5. Query API を確認する

ALB 作成直後は ECS task の起動と ALB health check に少し時間がかかります。

```bash
QUERY_API_URL="$(tofu -chdir=infra/aws output -raw query_api_url)"
curl "$QUERY_API_URL/health"
curl "$QUERY_API_URL/metrics/calendar-heatmap"
```

Grafana から見る場合は、ローカル Grafana の datasource URL を `query_api_url` に変更します。

## 6. ドメイン設定について

今回の初期デプロイは public ALB HTTP 公開までです。

- `webhook-ingest`: API Gateway の `webhook_url` をそのまま microCMS に設定します。
- `duckdb-query-api`: ALB の `query_api_url` をそのまま使います。

独自ドメイン、HTTPS、ACM certificate、Route 53、WAF、認証はこの手順の対象外です。
必要になった場合は、ALB listener を HTTPS 化し、Route 53 alias record と ACM certificate を追加してください。

## 7. 更新デプロイ

コード変更後に同じ tag で再デプロイする場合:

```bash
just docker-build-aws
just docker-push-aws
just deploy
```

tag を変える場合:

```bash
IMAGE_TAG=$(git rev-parse --short HEAD) just deploy-all
```

## 8. 削除

main stack を削除します。

```bash
just deploy-destroy
```

ECR bootstrap stack を削除します。

```bash
just bootstrap-destroy
```

ECR repository に image が残っていると bootstrap destroy は失敗します。必要に応じて ECR image を削除してから再実行してください。

S3 bucket に object が残っている場合、`force_destroy_bucket = false` のままでは削除に失敗します。検証環境で全削除してよい場合だけ `force_destroy_bucket = true` に変更してください。
