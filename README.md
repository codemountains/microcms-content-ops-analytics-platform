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
| API 別イベント件数 | どのコンテンツ種別が活発かを見る |
| イベント種別別件数 | 作成・編集・削除の比率を見る |
| 編集回数が多いコンテンツ | 運用負荷が高い記事を見つける |
| ステータス別イベント数 | 下書き・公開・非公開などの状態変化を見る |

## S3 保存形式

Webhook イベントは、分析しやすいように正規化して Parquet として保存します。

```text
s3://<bucket>/microcms_events/
  service=<service>/
    api=<api>/
      dt=YYYY-MM-DD/
        <event_id>.parquet
```

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
| `event_kind` | string | 公開状態の変化を加味した分析用イベント分類 |
| `old_status` | string | 変更前ステータス |
| `new_status` | string | 変更後ステータス |
| `old_updated_at` | timestamp | 変更前コンテンツの更新日時 |
| `new_updated_at` | timestamp | 変更後コンテンツの更新日時 |
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

想定 API は次の通りです。

```text
GET /health
GET /metrics/calendar-heatmap
GET /metrics/api-activity
GET /metrics/top-updated-contents
GET /metrics/average-time-to-publish-by-api
```

任意 SQL を受け付ける API は初期実装では提供しません。
Grafana から実行できるクエリを固定することで、安全性と説明の分かりやすさを優先します。

## Grafana

Grafana は DuckDB Query API の JSON レスポンスを可視化します。
Calendar Heatmap には [`tim012432-calendarheatmap-panel`](https://grafana.com/grafana/plugins/tim012432-calendarheatmap-panel/) を使います。
Grafana 自体には分析対象データを保存しません。

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
| `grafana` | JSON API datasource と初期 dashboard provisioning |
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

主な `just` コマンド:

| コマンド | 内容 |
| --- | --- |
| `just test` | Rust workspace のテストを実行 |
| `just clippy` | Clippy を warning error として実行 |
| `just validate` | OpenTofu、Docker Compose、Grafana JSON を検証 |
| `just check` | format、test、clippy、validate を一括実行 |
| `just debug` | Floci/ngrok/Grafana を使うローカルデバッグ環境を起動 |
| `just debug-webhook` | ローカル API Gateway に署名付き sample webhook を送信 |
| `just debug-parquet-persist` | Floci S3 の debug Parquet を `.debug/parquet/` に保存 |
| `just debug-parquet-delete` | debug で生成した Parquet を削除 |
| `just debug-metrics` | Query API の health/metrics を確認 |
| `just deploy-all` | ECR bootstrap、image build/push、AWS deploy を一括実行 |
| `just deploy-plan` | 実 AWS 向け OpenTofu plan |
| `just deploy` | 実 AWS 向け OpenTofu apply |

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
| `http://localhost:8000/metrics/api-activity` | API ごとの `new` / `edit` / `delete` 件数 |
| `http://localhost:8000/metrics/top-updated-contents` | 更新回数が多いコンテンツ |
| `http://localhost:8000/metrics/average-time-to-publish-by-api` | API ごとの平均公開所要日数 |
| `http://localhost:3000` | Grafana |

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
