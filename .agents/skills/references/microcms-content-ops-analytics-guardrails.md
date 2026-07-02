# microCMS Content Ops Analytics Guardrails

`microcms-content-ops-analytics-platform` の coding agent / reviewer / implementer が共通で守る guardrails。`README.md`、`requirements/`、`docs/`、`AGENTS.md`、実装が矛盾する場合は、勝手に一方へ寄せず、差分を明示してから判断する。

## Source Of Truth

1. `README.md`: project overview、local commands、環境変数、API 概要
2. `requirements/microcms-content-ops-analytics.spec.md`: product behavior、scope、API、schema、acceptance criteria
3. `docs/local-debug.md`: Floci / ngrok / Docker Compose / OpenTofu を使う local debug workflow
4. `docs/aws-deploy.md`: AWS deploy workflow、required permissions、OpenTofu outputs
5. `justfile`: command definitions and verification surface
6. current implementation: Rust code、tests、Dockerfiles、Compose、Grafana dashboard、OpenTofu

## Product Boundary

- microCMS Webhook を CMS 運用イベントログとして扱い、S3 Parquet に保存し、DuckDB Query API と Grafana で可視化する。
- `webhook-ingest` は Webhook 受信、`x-microcms-signature` 検証、payload 正規化、Parquet 生成、S3 保存に集中する。
- `duckdb-query-api` は固定メトリクス API を提供する。任意 SQL 実行 API は初期スコープ外。
- Grafana は可視化レイヤであり、分析対象データの保存先ではない。
- microCMS Management API からの全件同期、複数テナント対応、厳密な監査ログ用途、リアルタイムストリーミング分析は初期スコープ外。

## Architecture

- Rust workspace は `webhook-ingest` と `duckdb-query-api` を主要 member として扱う。
- component boundary を守り、Lambda ingest と Query API の責務を混ぜない。
- S3 Parquet path は Hive partition として DuckDB から読める形を保つ。
- Query API は Grafana が扱いやすい JSON を返す固定 endpoint として扱う。
- OpenTofu は `infra/bootstrap`、`infra/aws`、`infra/local` の役割分担を保つ。
- Local debug は Floci、ngrok、Docker Compose、OpenTofu の既存 workflow に合わせる。

## Data And API Contracts

- Webhook signature verification を弱めない。検証失敗時は S3 へ保存しない。
- Parquet schema、S3 key layout、environment variables、public endpoints を変更する場合は docs 更新要否を必ず判断する。
- S3 key layout は `microcms_events/service=<service>/api=<api>/dt=<YYYY-MM-DD>/<event_id>.parquet` を基準にする。
- 現在の主要 API は `GET /health`、`GET /metrics/calendar-heatmap`、`GET /metrics/api-activity`、`GET /metrics/top-updated-contents`、`GET /metrics/average-time-to-publish-by-api`、`GET /metrics/average-draft-to-publish-by-api`。
- SQL に user input を安易に埋め込まない。固定メトリクス API と parameter validation を維持する。

## Security And Artifacts

- `.env`、AWS credentials、microCMS webhook secret、ngrok authtoken、private endpoints を commit しない。
- logs、errors、tracing に secrets や過剰な raw payload を出さない。
- `target/`、Docker build artifacts、cache、generated Parquet、大きな sample data、不要な S3 artifacts を commit しない。
- OpenTofu state、destroy、deploy、ECR push、S3 bucket deletion は影響範囲を確認し、user の明示指示がある場合のみ行う。
- `force_destroy_bucket` のような破壊的設定を変更する場合は、final response または PR description に影響を明記する。

## Verification

変更対象に対応する明示的な command を実行する。

- Rust code: `just fmt`、`just test`、`just clippy`
- Infrastructure / Compose / Grafana dashboard: `just validate`
- 広範囲の変更や PR 前: `just check`
- Local behavior: `just debug`、`just debug-webhook`、`just debug-metrics`、`just debug-s3-ls`

実行できない検証がある場合は、理由と未検証 risk を final response または PR description に明記する。

## Baseline Workflow

- 作業前に `git status --short --branch` を確認し、既存の未コミット変更を壊さない。
- user が作成した可能性のある変更は revert しない。必要なら、その変更を前提に作業する。
- 実装前に source of truth と relevant code / tests / config を確認する。
- 変更範囲を task に必要な範囲へ絞る。無関係な refactor、format churn、metadata 変更を混ぜない。
- branch や PR の作成は user から依頼された場合に行う。

## Commit And Pull Request

- commit 前に `git diff` を review し、unrelated changes を含めていないか確認する。
- commit message は Conventional Commits を推奨する。
- PR description には context、変更内容、検証結果、未検証リスクを含める。
- requirements、docs、API、schema、infra に影響がある場合は該当ファイルや変更点を明記する。
- 既存 PR の更新であれば、新規 PR を作らず同じ PR に push する。

推奨 branch naming:

```text
feature/<short-topic>
fix/<short-topic>
docs/<short-topic>
chore/<short-topic>
```

## Documentation Ownership

- product behavior、scope、API、schema、acceptance criteria: `requirements/microcms-content-ops-analytics.spec.md`
- project overview、environment variables、local command overview: `README.md`
- local debug workflow: `docs/local-debug.md`
- AWS deploy workflow: `docs/aws-deploy.md`
- coding-agent workflow、review workflow、verification guardrails: `.agents/skills/`
- command behavior: `justfile`
