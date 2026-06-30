# microCMS Content Ops Analytics 仕様書

## 1. 概要

このドキュメントは、`microcms-content-ops-analytics` の仕様を定義する。

本プロジェクトは、microCMS Webhook をコンテンツ運用イベントとして収集し、S3 Parquet に保存する。
保存された Parquet は DuckDB Query API から読み込み、Grafana で可視化する。

目的は、microCMS のコンテンツ運用状況を軽量に分析し、改善に活用することである。

## 2. 目的

### 2.1 解決したい課題

microCMS の Webhook は、一般的にビルド・デプロイ・外部通知のトリガーとして使われることが多い。
しかし、Webhook にはコンテンツの作成・編集・削除といった運用イベントが含まれるため、これを蓄積すれば CMS 運用の分析データとして活用できる。

本プロジェクトでは、次のような問いに答えることを目的とする。

- どの日にコンテンツ更新が多いか
- どの API の運用量が多いか
- 編集回数が多いコンテンツはどれか
- 作成・編集・削除の比率はどうなっているか
- 下書き・公開などのステータス変化はどう推移しているか

### 2.2 提案するユースケース

microCMS Webhook を CMS 運用イベントログとして扱い、S3 Parquet、DuckDB、Grafana を組み合わせて、低コストなコンテンツ運用分析基盤を構築する。

## 3. スコープ

### 3.1 対象範囲

初期実装の対象範囲は次の通り。

- microCMS Webhook の受信
- Webhook 署名検証
- Webhook payload の正規化
- S3 Parquet への保存
- DuckDB による S3 Parquet の集計
- Grafana 向け JSON API の提供
- Grafana ダッシュボードでの可視化

### 3.2 対象外

初期実装では、次の内容は対象外とする。

- microCMS Management API からの全件同期
- 厳密な監査ログ用途
- 完全なイベントソーシング
- 複数テナント対応
- ユーザー単位の編集者分析
- リアルタイムストリーミング分析
- 大規模 DWH 連携
- 任意 SQL 実行 API

## 4. アーキテクチャ

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

## 5. コンポーネント仕様

## 5.1 `webhook-ingest`

### 5.1.1 概要

`webhook-ingest` は、microCMS Webhook を受信する Rust Lambda である。

### 5.1.2 責務

- API Gateway 経由で Webhook request を受信する
- `x-microcms-signature` を検証する
- JSON payload を parse する
- 分析用イベント schema に正規化する
- Arrow RecordBatch を作成する
- Parquet に変換する
- S3 に保存する

### 5.1.3 入力

API Gateway から渡される HTTP request を入力とする。

想定 header:

| Header | 必須 | 説明 |
| --- | --- | --- |
| `x-microcms-signature` | yes | microCMS Webhook の署名 |
| `content-type` | no | 通常は `application/json` |

想定 body:

```json
{
  "service": "example-service",
  "api": "blogs",
  "id": "content-id",
  "type": "edit",
  "contents": {
    "old": {},
    "new": {}
  }
}
```

### 5.1.4 署名検証

`webhook-ingest` は、リクエスト body と `MICROCMS_WEBHOOK_SECRET` を使って HMAC-SHA256 を計算し、`x-microcms-signature` と比較する。

署名検証に失敗した場合は、S3 へ保存せず `401 Unauthorized` を返す。

### 5.1.5 出力

S3 に Parquet file を保存する。

保存先 key:

```text
microcms_events/service=<service>/api=<api>/dt=<YYYY-MM-DD>/<event_id>.parquet
```

HTTP response:

成功時:

```json
{
  "ok": true,
  "s3_key": "microcms_events/service=example/api=blogs/dt=2026-06-29/<event_id>.parquet"
}
```

失敗時:

```json
{
  "ok": false,
  "message": "error message"
}
```

### 5.1.6 環境変数

| 変数名 | 必須 | 説明 |
| --- | --- | --- |
| `EVENT_BUCKET` | yes | Parquet 保存先 S3 bucket |
| `EVENT_PREFIX` | no | Parquet 保存先 prefix。既定値は `microcms_events` |
| `MICROCMS_WEBHOOK_SECRET` | yes | microCMS Webhook の署名検証用 secret |

## 5.2 `duckdb-query-api`

### 5.2.1 概要

`duckdb-query-api` は、S3 上の Parquet を DuckDB で集計し、Grafana 向けに JSON を返す Rust API である。

### 5.2.2 責務

- DuckDB connection を作成する
- `httpfs` extension を load する
- S3 credential chain を設定する
- S3 Parquet を `read_parquet()` で読み込む
- 固定 SQL を実行する
- Grafana が扱いやすい JSON を返す

### 5.2.3 API

#### `GET /health`

ヘルスチェック用 API。

Response:

```json
{
  "ok": true
}
```

#### `GET /metrics/daily-events`

日別・API 別・イベント種別別のイベント件数を返す。

Query parameters:

| Parameter | Default | 説明 |
| --- | --- | --- |
| `days` | `30` | 集計対象期間 |

Response example:

```json
[
  {
    "dt": "2026-06-29",
    "api": "blogs",
    "event_type": "edit",
    "event_count": 12
  }
]
```

#### `GET /metrics/events-by-api`

API 別のイベント件数とコンテンツ数を返す。

Query parameters:

| Parameter | Default | 説明 |
| --- | --- | --- |
| `days` | `30` | 集計対象期間 |

Response example:

```json
[
  {
    "api": "blogs",
    "event_count": 120,
    "content_count": 42
  }
]
```

#### `GET /metrics/top-edited-contents`

編集回数が多いコンテンツを返す。

Query parameters:

| Parameter | Default | 説明 |
| --- | --- | --- |
| `days` | `30` | 集計対象期間 |
| `limit` | `20` | 返却件数 |

Response example:

```json
[
  {
    "api": "blogs",
    "content_id": "content-id",
    "title": "Example title",
    "edit_count": 8,
    "last_event_at": "2026-06-29T12:00:00Z"
  }
]
```

#### `GET /metrics/status-events`

ステータス別のイベント件数を返す。

Query parameters:

| Parameter | Default | 説明 |
| --- | --- | --- |
| `days` | `30` | 集計対象期間 |

Response example:

```json
[
  {
    "dt": "2026-06-29",
    "api": "blogs",
    "status": "PUBLISH",
    "event_count": 5
  }
]
```

### 5.2.4 環境変数

| 変数名 | 必須 | 説明 |
| --- | --- | --- |
| `EVENTS_PATH` | yes | `read_parquet()` で読む S3 path |
| `AWS_REGION` | no | S3 bucket の region。既定値は `ap-northeast-1` |
| `PORT` | no | HTTP server port。既定値は `8000` |

`EVENTS_PATH` example:

```text
s3://microcms-content-ops-events/microcms_events/**/*.parquet
```

## 6. データ仕様

## 6.1 Parquet schema

| カラム | 型 | Nullable | 説明 |
| --- | --- | --- | --- |
| `received_at` | timestamp | no | Webhook を受信した日時 |
| `service` | string | yes | microCMS service ID |
| `api` | string | yes | microCMS API ID |
| `content_id` | string | yes | microCMS content ID |
| `event_type` | string | yes | Webhook event type |
| `old_status` | string | yes | 変更前 status |
| `new_status` | string | yes | 変更後 status |
| `old_updated_at` | timestamp | yes | 変更前 content の updatedAt |
| `new_updated_at` | timestamp | yes | 変更後 content の updatedAt |
| `title` | string | yes | content title |
| `raw_payload` | string | no | Webhook payload の原文 |

## 6.2 Partition

S3 key は次の partition を含む。

| Partition | 説明 |
| --- | --- |
| `service` | microCMS service ID |
| `api` | microCMS API ID |
| `dt` | Webhook 受信日 |

例:

```text
microcms_events/service=my-service/api=blogs/dt=2026-06-29/01JXXXXXXXX.parquet
```

## 7. 集計 SQL

## 7.1 日別イベント件数

```sql
SELECT
  CAST(dt AS VARCHAR) AS dt,
  api,
  event_type,
  COUNT(*) AS event_count
FROM read_parquet(
  '<EVENTS_PATH>',
  hive_partitioning = true,
  union_by_name = true
)
WHERE dt >= current_date - INTERVAL 30 DAY
GROUP BY dt, api, event_type
ORDER BY dt, api, event_type;
```

## 7.2 API 別イベント件数

```sql
SELECT
  api,
  COUNT(*) AS event_count,
  COUNT(DISTINCT content_id) AS content_count
FROM read_parquet(
  '<EVENTS_PATH>',
  hive_partitioning = true,
  union_by_name = true
)
WHERE dt >= current_date - INTERVAL 30 DAY
GROUP BY api
ORDER BY event_count DESC;
```

## 7.3 編集回数が多いコンテンツ

```sql
SELECT
  api,
  content_id,
  any_value(title) AS title,
  COUNT(*) AS edit_count,
  MAX(received_at) AS last_event_at
FROM read_parquet(
  '<EVENTS_PATH>',
  hive_partitioning = true,
  union_by_name = true
)
WHERE
  dt >= current_date - INTERVAL 30 DAY
  AND event_type = 'edit'
GROUP BY api, content_id
ORDER BY edit_count DESC, last_event_at DESC
LIMIT 20;
```

## 7.4 ステータス別イベント件数

```sql
SELECT
  CAST(dt AS VARCHAR) AS dt,
  api,
  new_status AS status,
  COUNT(*) AS event_count
FROM read_parquet(
  '<EVENTS_PATH>',
  hive_partitioning = true,
  union_by_name = true
)
WHERE dt >= current_date - INTERVAL 30 DAY
GROUP BY dt, api, new_status
ORDER BY dt, api, new_status;
```

## 8. Grafana 仕様

Grafana は `duckdb-query-api` に HTTP request を送り、JSON response をパネルとして可視化する。

初期ダッシュボードでは、次のパネルを想定する。

| パネル | API | 可視化形式 |
| --- | --- | --- |
| 日別 Webhook 件数 | `/metrics/daily-events` | Time series / Bar chart |
| API 別イベント件数 | `/metrics/events-by-api` | Bar chart |
| 編集回数が多いコンテンツ | `/metrics/top-edited-contents` | Table |
| ステータス別イベント件数 | `/metrics/status-events` | Bar chart / Time series |

## 9. セキュリティ仕様

## 9.1 Webhook 署名検証

`webhook-ingest` は `x-microcms-signature` を必ず検証する。
署名検証に失敗した payload は保存しない。

## 9.2 S3 権限

`webhook-ingest` には、対象 prefix への `s3:PutObject` のみを付与する。

例:

```json
{
  "Effect": "Allow",
  "Action": ["s3:PutObject"],
  "Resource": "arn:aws:s3:::<bucket>/microcms_events/*"
}
```

`duckdb-query-api` には、対象 prefix への `s3:GetObject` と必要に応じて `s3:ListBucket` を付与する。

## 9.3 raw payload の扱い

`raw_payload` にはコンテンツ本文や内部情報が含まれる可能性がある。
本番利用時は、次のいずれかを検討する。

- `raw_payload` を保存しない
- `raw_payload` をマスクする
- `raw_payload` を別 prefix に保存してアクセス制御を分ける
- 保存対象カラムを allowlist 化する

## 10. 運用上の制約

## 10.1 Webhook の完全性

このサンプルは、CMS 運用傾向の可視化を目的とする。
Webhook の失敗、重複、順序入れ替わりを完全には扱わない。

厳密な監査用途では、次の設計を追加する。

- retry
- Dead Letter Queue
- idempotency key
- event deduplication
- event ordering strategy

## 10.2 小ファイル問題

初期実装では `1 event = 1 Parquet file` とする。
これはブログサンプルとして理解しやすい一方、イベント数が増えると S3 上の小ファイルが増える。

運用では日次 compaction を検討する。

```text
microcms_events_raw/
  service=<service>/api=<api>/dt=<date>/*.parquet

microcms_events_compacted/
  service=<service>/api=<api>/dt=<date>/events.parquet
```

## 11. 非機能要件

| 項目 | 方針 |
| --- | --- |
| コスト | S3 + Lambda + ローカル Grafana を中心に低コストで構成する |
| データ規模 | 数百 MB から数 GB 程度を主な対象とする |
| 可用性 | サンプルでは高可用構成を必須としない |
| 性能 | Grafana の手動閲覧・低頻度クエリを想定する |
| 保守性 | 固定 API と明示的な schema で実装を単純化する |

## 12. 実装方針

## 12.1 Rust 統一

`webhook-ingest` と `duckdb-query-api` は Rust で実装する。

理由:

- Webhook payload を型安全に扱える
- HMAC 署名検証を堅牢に実装できる
- Arrow / Parquet への変換を直接実装できる
- DuckDB Query API を固定型の API として実装できる
- 技術記事としての一貫性が高い

## 12.2 任意 SQL API を提供しない

初期実装では、Grafana から任意 SQL を実行する API は提供しない。

理由:

- SQL injection や意図しないファイルアクセスのリスクを避けるため
- API の責務を Grafana 用 metrics に限定するため
- ブログサンプルとして読みやすくするため

## 13. 今後の拡張候補

- 日次 compaction
- raw payload のマスキング
- Management API による初期バックフィル
- API ごとの schema 拡張
- CloudWatch Logs / Metrics 連携
- Grafana dashboard provisioning
- ECS Fargate での `duckdb-query-api` 常時稼働
- Basic 認証または reverse proxy による API 保護
- S3 lifecycle policy による保存期間管理
