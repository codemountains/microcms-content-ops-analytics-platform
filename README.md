# microcms-content-ops-analytics

microCMS Webhook をコンテンツ運用イベントとして収集し、S3 Parquet に保存するサンプル実装です。
S3 上の Parquet を DuckDB Query API が集計し、Grafana でコンテンツ運用状況を可視化します。

このプロジェクトは、microCMS Webhook を「デプロイのトリガー」だけでなく、CMS 運用を改善するためのイベントログとして活用することを目的としています。

## コンセプト

microCMS のコンテンツ作成・編集・削除イベントを Webhook で受信し、分析しやすい Parquet 形式で S3 に蓄積します。
DuckDB は S3 上の Parquet を `read_parquet()` で直接読み込み、Grafana は DuckDB Query API の集計結果を可視化します。

```text
microCMS
  │ Webhook
  ▼
API Gateway
  │
  ▼
Lambda / webhook-ingest
  │
  ▼
S3 Parquet
  ▲
  │ read_parquet()
  │
duckdb-query-api
  ▲
  │ HTTP / JSON
  │
Grafana
```

## 役割分担

| コンポーネント | 役割 |
| --- | --- |
| `webhook-ingest` | microCMS Webhook を受信し、イベントを Parquet として S3 に保存する Rust Lambda |
| `duckdb-query-api` | S3 上の Parquet を DuckDB で集計し、Grafana 向けに JSON API を提供する Rust API |
| `grafana` | DuckDB Query API のレスポンスを可視化するダッシュボード |
| S3 | コンテンツ運用イベントの保存先 |
| DuckDB | S3 Parquet を読む分析クエリエンジン |

Grafana はデータを保存しません。
データ本体は S3 Parquet、分析エンジンは DuckDB、可視化レイヤーは Grafana です。

## ディレクトリ構成

```text
microcms-content-ops-analytics/
├── README.md
├── Cargo.toml
├── docker-compose.yml
├── docs/
│   └── microcms-content-ops-analytics.spec.md
├── webhook-ingest/
├── duckdb-query-api/
└── grafana/
```

## 技術スタック

| 領域 | 技術 |
| --- | --- |
| Webhook 受信 | API Gateway, AWS Lambda |
| Lambda 実装 | Rust |
| オブジェクトストレージ | Amazon S3 |
| 保存形式 | Parquet |
| 分析エンジン | DuckDB |
| Query API | Rust, Axum, duckdb-rs |
| 可視化 | Grafana |
| ローカル実行 | Docker Compose |

## 想定する可視化指標

このサンプルでは、次のようなコンテンツ運用指標を可視化します。

| 指標 | 目的 |
| --- | --- |
| 日別 Webhook 件数 | コンテンツ更新量の推移を見る |
| 公開数 | ダッシュボードの time range 内の公開・再公開アクション件数を見る |
| 公開状態率 | ダッシュボードの time range 内の状態到達・維持イベントに対する公開状態の割合を見る |
| 公開アクション数の推移 | 公開・再公開アクションの流れを見る |
| API 別イベント件数 | どのコンテンツ種別が活発かを見る |
| イベント種別別件数 | API ごとの `event_kind` 件数を見る |
| 編集回数が多いコンテンツ | 運用負荷が高い記事を見つける |
| 公開状態遷移別イベント数 | 14 種の公開状態遷移を見る |
| 下書き作成から公開までの平均所要日数 | 下書き運用のリードタイムを見る |

## S3 保存形式

Webhook イベントは、分析しやすいように正規化して Parquet として保存します。

```text
s3://<bucket>/microcms_events/
  service=<service>/
    api=<api>/
      dt=YYYY-MM-DD/
        <event_id>.parquet
```

`dt` は Webhook 受信時刻を JST（日本時間）のカレンダー日に変換した日付です。
`received_at` 自体は UTC タイムスタンプのまま Parquet に保存します。

初期実装では、分かりやすさを優先して `1 Webhook event = 1 Parquet file` とします。
ファイル数が増える場合は、日次単位で compaction する設計に拡張できます。

## 主要スキーマ

| カラム | 型 | 説明 |
| --- | --- | --- |
| `received_at` | timestamp | Webhook を受信した日時 |
| `service` | string | microCMS のサービス ID |
| `api` | string | microCMS の API ID |
| `content_id` | string | コンテンツ ID |
| `event_type` | string | microCMS Webhook の `type` (`new` / `edit` / `delete`) |
| `event_kind` | string | 公開状態の変化を加味した分析用イベント分類（例: `INITIAL_DRAFT`, `PUBLISH_FROM_DRAFT`, `DISCARD_DRAFT_ON_PUBLISHED`） |
| `old_status` | string | 変更前ステータス |
| `new_status` | string | 変更後ステータス |
| `old_updated_at` | timestamp | 変更前コンテンツの更新日時 |
| `new_updated_at` | timestamp | 変更後コンテンツの更新日時 |
| `draft_created_at` | timestamp | `contents.new.draftValue.createdAt` |
| `content_created_at` | timestamp | `contents.new.publishValue.createdAt` |
| `content_published_at` | timestamp | `contents.new.publishValue.publishedAt` |
| `raw_payload` | string | Webhook payload の原文 |

## `webhook-ingest`

`webhook-ingest` は Rust Lambda として実装します。

主な責務は次の通りです。

1. microCMS Webhook を受信する
2. `x-microcms-signature` を検証する
3. Webhook payload を正規化する
4. Arrow RecordBatch を生成する
5. Parquet に変換する
6. S3 に保存する

## `duckdb-query-api`

`duckdb-query-api` は Rust + Axum で実装します。

主な責務は次の通りです。

1. S3 上の Parquet を DuckDB の `read_parquet()` で読み込む
2. 固定メトリクス API ごとに SQL を実行する
3. Grafana が扱いやすい JSON を返す
4. 任意有効化された MCP endpoint から同じ固定メトリクスを read-only tool として返す

想定 API は次の通りです。

```text
GET /health
GET /metrics/calendar-heatmap
GET /metrics/publish-action-summary
GET /metrics/publish-action-trend
GET /metrics/api-activity
GET /metrics/top-updated-contents
GET /metrics/average-time-to-publish-by-api
GET /metrics/average-draft-to-publish-by-api
```

任意 SQL を受け付ける API は初期実装では提供しません。
Grafana から実行できるクエリを固定することで、安全性と説明の分かりやすさを優先します。
MCP endpoint でも任意 SQL tool は提供せず、既存の固定 metrics のみを公開します。

## Grafana

Grafana は DuckDB Query API の JSON レスポンスを可視化します。
Calendar Heatmap には [`tim012432-calendarheatmap-panel`](https://grafana.com/grafana/plugins/tim012432-calendarheatmap-panel/) を使います。
Calendar Heatmap の日付バケットは S3 パーティション `dt` と同じ JST カレンダー日です。
Grafana 自体には分析対象データを保存しません。
ローカル実行では Docker Compose の file provisioning を使い、AWS デプロイ後は既存 Grafana Cloud stack へ datasource と dashboard を反映できます。
dashboard JSON は Grafana v2 resource schema（`apiVersion: dashboard.grafana.app/v2`、`spec.elements` / `spec.layout`）を source of truth とします。
Grafana 13 UI で編集した内容を取り込む場合は V2 Resource JSON として export し、`scripts/validate-grafana-dashboard.sh` で panel / query / variable の欠落がないことを確認してから repository の JSON を更新してください。

```text
Grafana = 可視化
DuckDB Query API = 集計 API
DuckDB = SQL 実行エンジン
S3 Parquet = データ本体
```

## ローカル実行方針

初期サンプルでは、次の構成を想定します。

```text
AWS:
  API Gateway
  Lambda / webhook-ingest
  S3

Local:
  duckdb-query-api
  Grafana
```

`duckdb-query-api` と Grafana は Docker Compose で起動します。
AWS 認証情報はローカルの credential chain を利用します。

## 実装済みコンポーネント

このリポジトリは Rust workspace として構成されています。

| パス | 内容 |
| --- | --- |
| `webhook-ingest` | Lambda 用の Webhook ingest。署名検証、payload 正規化、Parquet 生成、S3 保存を行います |
| `duckdb-query-api` | Axum ベースの固定メトリクス API。DuckDB `read_parquet()` で S3/ローカル Parquet を集計します |
| `grafana` | Infinity datasource と初期 dashboard provisioning |
| `docker-compose.yml` | `duckdb-query-api` と Grafana のローカル起動構成 |

## 環境変数

### `webhook-ingest`

| 変数名 | 必須 | 既定値 | 説明 |
| --- | --- | --- | --- |
| `EVENT_BUCKET` | yes | なし | Parquet 保存先 S3 bucket |
| `EVENT_PREFIX` | no | `microcms_events` | Parquet 保存先 prefix |
| `MICROCMS_WEBHOOK_SECRET` | yes | なし | `x-microcms-signature` 検証用 secret |

### `duckdb-query-api`

| 変数名 | 必須 | 既定値 | 説明 |
| --- | --- | --- | --- |
| `EVENTS_PATH` | yes | なし | `read_parquet()` で読む path |
| `AWS_REGION` | no | `ap-northeast-1` | S3 bucket の region |
| `DUCKDB_EXTENSION_DIRECTORY` | no | `/tmp/duckdb_extensions` | DuckDB extension の保存先 |
| `PORT` | no | `8000` | HTTP server port |
| `MCP_ENABLED` | no | `false` | `true` の場合だけ Streamable HTTP MCP endpoint `/mcp` を有効化 |
| `MCP_BEARER_TOKEN` | MCP 有効時 yes | なし | `/mcp` で要求する bearer token |
| `MCP_ALLOWED_ORIGINS` | MCP 有効時 yes | なし | `/mcp` で許可する `Origin`。comma-separated exact match |

### Grafana Cloud provisioning

| 変数名 | 必須 | 既定値 | 説明 |
| --- | --- | --- | --- |
| `GRAFANA_STACK_URL` | yes | なし | 既存 Grafana Cloud stack URL |
| `GRAFANA_STACK_SERVICE_ACCOUNT_TOKEN` | yes | なし | datasource / dashboard を書き込める service account token |
| `QUERY_API_URL` | no | OpenTofu output `query_api_url` | datasource に設定する DuckDB Query API URL |
| `GRAFANA_DASHBOARD_UID` | no | `microcms-content-ops` | upsert する dashboard uid |
| `GRAFANA_DASHBOARD_NAMESPACE` | yes | なし | `dashboard.grafana.app/v2` API の namespace。Grafana Cloud は `stacks-<stack_id>` |
| `GRAFANA_FOLDER_UID` | no | なし | dashboard 配置先 folder uid |
| `GRAFANA_SKIP_PLUGIN_CHECK` | no | `0` | `1` の場合だけ Grafana plugin 確認を skip |

## 開発環境（Nix）

このリポジトリは [Nix flake](https://nixos.wiki/wiki/Flakes) で開発用ツールチェーンを提供します。
Rust toolchain は [`rust-toolchain.toml`](./rust-toolchain.toml) を single source of truth とし、devShell では `rustfmt` / `clippy` も含めます。

### 前提

- [Nix](https://nixos.org/download/) 2.18 以降（flakes 有効）
  - 初回利用時は `~/.config/nix/nix.conf` などに `experimental-features = nix-command flakes` を設定してください（詳細は [NixOS Wiki: Flakes](https://nixos.wiki/wiki/Flakes)）
- Docker / Docker Compose（ローカル起動・`just debug`・`just check-ci` で必要。Nix では daemon を提供しません）

### 使い方

[direnv](https://direnv.net/) がインストール済みの場合:

```bash
direnv allow
```

direnv を使わない場合:

```bash
nix develop
```

### devShell に含まれる主なツール

| ツール | 用途 |
| --- | --- |
| `rustc` / `cargo` / `rustfmt` / `clippy` | Rust 開発（`rust-toolchain.toml` 準拠） |
| `git` | バージョン管理（devShell は PATH を上書きするため同梱） |
| `just` | リポジトリ共通コマンド |
| `tofu` | OpenTofu（`infra/` の validate / apply） |
| `aws` | AWS CLI（ローカル Floci S3 操作など） |
| `jq` / `curl` / `openssl` / `xxd` | スクリプト・デバッグ補助 |
| `docker` / `docker-compose` | Docker CLI / Compose CLI（daemon は別途必要） |
| `gitleaks` | secret scan（任意。CI の `security.yml` と同種の検出を手元でも確認できる） |

devShell 内では `just test` や `just validate` をそのまま実行できます。
Docker が必要なコマンド（`just debug`、`just check-ci`、`docker compose up` など）は、別途 Docker Desktop / OrbStack などを起動してください。

### Codex CLI

[Codex CLI](https://developers.openai.com/codex) では [`.codex/environments/environment.toml`](./.codex/environments/environment.toml) が Nix devShell を利用します。
setup 時は `direnv` を試し、主要ツールが不足している場合は `nix develop` に fallback します。

Codex actions から次を実行できます。

| Action | 内容 |
| --- | --- |
| Check | `just check` |
| Test | `just test` |
| Validate | `just validate` |
| Clippy | `just clippy` |

`Validate` と `Check` は `docker compose config` を呼びますが、これは Compose ファイルの構文検証のみで Docker daemon は不要です。
`just debug` や `just check-ci` のように実際にコンテナを起動・ビルドする操作では、Codex 実行環境側で Docker daemon が利用可能である必要があります。

## テスト

```bash
just test
```

テストでは次を確認します。

- HMAC-SHA256 署名検証
- microCMS Webhook payload の正規化
- Arrow/Parquet への変換
- S3 partition key の生成
- DuckDB による Hive partitioned Parquet の集計

開発時の一括検証:

```bash
just check
```

CI では GitHub Actions が Pull Request と `main` への push 時に検証を実行します。
ローカルで CI 相当の静的検証を確認する場合は次を実行します。

```bash
just check-ci
```

CI は Gitleaks secret scan、Rust fmt/test/clippy、OpenTofu/Docker Compose/Grafana の validate、OpenTofu fmt check、Docker build smoke を実行します。
workflow は責務ごとに `security.yml`、`rust.yml`、`infra.yml`、`docker-build.yml` へ分割しています。
`rust.yml` は backend ごとに `webhook-ingest-test-and-clippy` と `duckdb-query-api-test-and-clippy` を分け、各 backend の test と clippy を同じ job で実行して重複ビルドを抑えます。
`rust.yml`、`infra.yml`、`docker-build.yml` は差分検出 job で重い job の実行を制御します。`webhook-ingest` の変更は `webhook-ingest-test-and-clippy` と `webhook-ingest-smoke`、`duckdb-query-api` の変更は `duckdb-query-api-test-and-clippy` と `duckdb-query-api-smoke` を実行し、どちらかの backend に関わる変更がある場合だけ Rust fmt を実行します。`Cargo.toml` / `Cargo.lock` と各 workflow 自身の変更は、対応する backend の Rust / Docker job を両方起動します。`docker-build.yml` はさらに `rust-toolchain.toml` と `.dockerignore` の変更でも両 backend の smoke を起動し、Buildx と GitHub Actions cache を使って再 push 時は差分中心の Docker build smoke になります。`infra.yml` は `infra/`、Compose、Grafana、`scripts/`、`justfile`、workflow 自身に関わる変更がある場合だけ validate と OpenTofu fmt check を実行します。
Gitleaks は Marketplace action の最新確認済み version を使い、`just check-ci` は Gitleaks 以外の主要検証をローカルで再現します。
organization repository で Gitleaks action を使う場合は、repository または organization secret として `GITLEAKS_LICENSE` が必要です。
GitHub Actions の `uses:` は supply-chain risk を抑えるため、tag ではなく commit SHA に pin しています。

主な `just` コマンド:

| コマンド | 内容 |
| --- | --- |
| `just test` | Rust workspace のテストを実行 |
| `just clippy` | Clippy を warning error として実行 |
| `just validate` | OpenTofu、Docker Compose、Grafana JSON を検証 |
| `just check-ci` | CI 相当の静的検証をローカルで実行 |
| `just docker-build-ci` | CI smoke 用に両 Dockerfile を debug profile で build |
| `just check` | format、test、clippy、validate を一括実行 |
| `just debug` | Floci/ngrok/Grafana を使うローカルデバッグ環境を起動 |
| `just debug-webhook` | ローカル API Gateway に署名付き sample webhook を送信 |
| `just debug-parquet-seed` | smoke 用ダミー Parquet を生成し、ローカル `microcms_events/` をクリアしたうえで Floci S3 に `--delete` sync |
| `just debug-parquet-seed-large` | 1 年分（既定 50,000 件 / 365 日）の bulk ダミー Parquet を生成し、Floci S3 に `--delete` sync |
| `just debug-parquet-persist` | Floci S3 の debug Parquet を `.debug/parquet/` に保存 |
| `just debug-parquet-delete` | debug で生成した Parquet を削除 |
| `just debug-metrics` | Query API の health/metrics を確認 |
| `just deploy-all` | ECR bootstrap、image build/push、AWS deploy を一括実行 |
| `just deploy-plan` | 実 AWS 向け OpenTofu plan |
| `just deploy` | 実 AWS 向け OpenTofu apply |
| `just deploy-grafana-cloud` | 既存 Grafana Cloud stack に datasource と dashboard を反映 |

## ローカル起動

`.env.example` を参考に `EVENTS_PATH` を設定してから起動します。

```bash
cp .env.example .env
docker compose up --build
```

起動後の URL:

| URL | 用途 |
| --- | --- |
| `http://localhost:8000/health` | DuckDB Query API の health check |
| `http://localhost:8000/metrics/calendar-heatmap` | Calendar Heatmap 用の日別イベント件数 |
| `http://localhost:8000/metrics/publish-action-summary` | 公開数 / 公開状態率 |
| `http://localhost:8000/metrics/publish-action-trend` | 日別の公開アクション数 |
| `http://localhost:8000/metrics/api-activity` | API ごとの `event_kind` 別件数 |
| `http://localhost:8000/metrics/top-updated-contents` | 更新回数が多いコンテンツ |
| `http://localhost:8000/metrics/average-time-to-publish-by-api` | API ごとの平均公開所要日数 / 時間 |
| `http://localhost:8000/metrics/average-draft-to-publish-by-api` | API ごとの下書き作成から公開までの平均所要日数 / 時間 |
| `http://localhost:8000/mcp` | 任意有効化された MCP Streamable HTTP endpoint |
| `http://localhost:3000` | Grafana |

MCP endpoint はローカル/検証用途の opt-in 機能です。
有効化する場合は `.env` で `MCP_ENABLED=true`、`MCP_BEARER_TOKEN`、`MCP_ALLOWED_ORIGINS` を設定してください。
ECS / ALB で外部公開する運用は初期スコープ外であり、公開する場合は ALB 側認証やネットワーク境界を別途設計してください。

## OpenTofu / ローカルデバッグ

IaC は OpenTofu で管理します。

| パス | 用途 |
| --- | --- |
| `infra/bootstrap` | 実 AWS 向け bootstrap。ECR repository を作成します |
| `infra/aws` | 実 AWS 向け。API Gateway + Lambda、S3、ECS Fargate + ALB、IAM を作成します |
| `infra/local` | Floci 向け。ローカル S3、Lambda、API Gateway を作成します |

Floci と ngrok を使ったローカル Webhook 検証手順は [`docs/local-debug.md`](./docs/local-debug.md) を参照してください。

AWS への初回デプロイ手順は [`docs/aws-deploy.md`](./docs/aws-deploy.md) を参照してください。

## 設計ドキュメント

詳細仕様は次のドキュメントを参照してください。

- [`requirements/microcms-content-ops-analytics.spec.md`](./requirements/microcms-content-ops-analytics.spec.md)

## 注意事項

このプロジェクトはブログ記事向けのサンプル実装です。
