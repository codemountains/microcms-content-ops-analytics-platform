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
- 作成から初回公開までにどれくらい時間がかかっているか
- 下書き・公開などの公開状態変化を分析しやすい分類で扱えるか

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

#### `GET /metrics/calendar-heatmap`

Calendar Heatmap 用の日別イベント件数を返す。
`tim012432-calendarheatmap-panel` が消費する time-series 形式で、0件の日も含める。
`time` は S3 パーティション `dt` と同じ **UTC カレンダー日** を表し、`YYYY-MM-DDT00:00:00Z` 形式で返す。

Query parameters:

| Parameter | Default | 説明 |
| --- | --- | --- |
| `from` | なし | 集計開始時刻（Unix epoch ミリ秒）。`to` とセットで指定する |
| `to` | なし | 集計終了時刻（Unix epoch ミリ秒）。`from` とセットで指定する |

`from` / `to` を省略した場合は、直近 365 日を返す。
Grafana ダッシュボードでは `${__from}` / `${__to}` を渡す。

Response example:

```json
[
  {
    "time": "2026-06-29T00:00:00Z",
    "value": 12
  }
]
```

#### `GET /metrics/api-activity`

API ごとの `type = new` / `edit` / `delete` 件数を返す。

Query parameters:

| Parameter | Default | 説明 |
| --- | --- | --- |
| `days` | `30` | 集計対象期間 |

Response example:

```json
[
  {
    "api": "blogs",
    "new_count": 12,
    "edit_count": 96,
    "delete_count": 3,
    "total_count": 111
  }
]
```

#### `GET /metrics/top-updated-contents`

更新回数が多いコンテンツを返す。
`event_type IN ('new', 'edit')` かつ `content_id IS NOT NULL` のイベントを対象にする。

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
    "count": 8,
    "last_event_at": "2026-06-29T12:00:00Z"
  }
]
```

#### `GET /metrics/average-time-to-publish-by-api`

API ごとに、コンテンツ作成 (`createdAt`) から初回公開 (`publishedAt`) までの平均所要日数を返す。
`event_kind IN ('CREATE_PUBLISH', 'FIRST_PUBLISH')` を対象にし、timestamp は `contents.new.publishValue` から抽出した値を使う。

Query parameters:

| Parameter | Default | 説明 |
| --- | --- | --- |
| `days` | `30` | 集計対象期間 |

Response example:

```json
[
  {
    "api": "blogs",
    "avg_days": 1.25
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
| `event_type` | string | yes | microCMS Webhook の `type` (`new` / `edit` / `delete`) |
| `event_kind` | string | yes | 公開状態の変化を加味した分析用イベント分類 |
| `old_status` | string | yes | 変更前 status。複数 status は comma-separated string |
| `new_status` | string | yes | 変更後 status。複数 status は comma-separated string |
| `old_updated_at` | timestamp | yes | 変更前 content の updatedAt |
| `new_updated_at` | timestamp | yes | 変更後 content の updatedAt |
| `content_created_at` | timestamp | yes | `contents.new.publishValue.createdAt` |
| `content_published_at` | timestamp | yes | `contents.new.publishValue.publishedAt` |
| `raw_payload` | string | no | Webhook payload の原文 |

## 6.2 event_kind

`event_kind` は microCMS Webhook の `type` と `contents.old.status` / `contents.new.status` から導出する。
確実に分類できないイベントでは `NULL` とする。

| event_kind | 判定 |
| --- | --- |
| `CREATE_DRAFT` | `type = new` かつ new status に `DRAFT` を含み `PUBLISH` を含まない |
| `CREATE_PUBLISH` | `type = new` かつ new status に `PUBLISH` を含む |
| `FIRST_PUBLISH` | `type = edit` かつ old status に `PUBLISH` を含まず new status に `PUBLISH` を含む |
| `UPDATE_PUBLISH` | `type = edit` かつ old/new status の両方に `PUBLISH` を含む |
| `UNPUBLISH` | `type = edit` かつ old status に `PUBLISH` を含み new status に `PUBLISH` を含まない |
| `DELETE` | `type = delete` |

## 6.3 Partition

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

## 7.1 Calendar Heatmap

```sql
WITH bounds AS (
  SELECT
    CAST(epoch_ms(<from_ms>) AS DATE) AS start_date,
    CAST(epoch_ms(<to_ms>) AS DATE) AS end_date
),
calendar AS (
  SELECT CAST(day AS DATE) AS dt
  FROM generate_series(
    (SELECT start_date FROM bounds),
    (SELECT end_date FROM bounds),
    INTERVAL 1 DAY
  ) AS series(day)
),
daily AS (
  SELECT
    dt,
    COUNT(*) AS event_count
  FROM read_parquet(
    '<EVENTS_PATH>',
    hive_partitioning = true,
    union_by_name = true
  )
  WHERE
    dt >= (SELECT start_date FROM bounds)
    AND dt <= (SELECT end_date FROM bounds)
  GROUP BY dt
)
SELECT
  CONCAT(CAST(calendar.dt AS VARCHAR), 'T00:00:00Z') AS time,
  COALESCE(daily.event_count, 0) AS value
FROM calendar
LEFT JOIN daily ON daily.dt = calendar.dt
ORDER BY calendar.dt;
```

## 7.2 API Activity

```sql
SELECT
  api,
  SUM(CASE WHEN event_type = 'new' THEN 1 ELSE 0 END) AS new_count,
  SUM(CASE WHEN event_type = 'edit' THEN 1 ELSE 0 END) AS edit_count,
  SUM(CASE WHEN event_type = 'delete' THEN 1 ELSE 0 END) AS delete_count,
  COUNT(*) AS total_count
FROM read_parquet(
  '<EVENTS_PATH>',
  hive_partitioning = true,
  union_by_name = true
)
WHERE dt >= current_date - INTERVAL 30 DAY
GROUP BY api
ORDER BY total_count DESC, api;
```

## 7.3 Top Updated Contents

```sql
SELECT
  api,
  content_id,
  COUNT(*) AS count,
  MAX(received_at) AS last_event_at
FROM read_parquet(
  '<EVENTS_PATH>',
  hive_partitioning = true,
  union_by_name = true
)
WHERE
  dt >= current_date - INTERVAL 30 DAY
  AND event_type IN ('new', 'edit')
  AND content_id IS NOT NULL
GROUP BY api, content_id
ORDER BY count DESC, last_event_at DESC
LIMIT 20;
```

## 7.4 Average Time to Publish by API

```sql
SELECT
  api,
  AVG(date_diff('second', content_created_at, content_published_at) / 86400.0) AS avg_days
FROM read_parquet(
  '<EVENTS_PATH>',
  hive_partitioning = true,
  union_by_name = true
)
WHERE
  dt >= current_date - INTERVAL 30 DAY
  AND event_kind IN ('CREATE_PUBLISH', 'FIRST_PUBLISH')
  AND content_created_at IS NOT NULL
  AND content_published_at IS NOT NULL
  AND content_published_at >= content_created_at
GROUP BY api
ORDER BY avg_days DESC, api;
```

## 8. Grafana 仕様

Grafana は `duckdb-query-api` に HTTP request を送り、JSON response をパネルとして可視化する。

初期ダッシュボードでは、次のパネルを想定する。

| パネル | API | 可視化形式 |
| --- | --- | --- |
| Calendar Heatmap | `/metrics/calendar-heatmap` | `tim012432-calendarheatmap-panel` |
| API Activity | `/metrics/api-activity` | Stacked Bar Chart |
| Top Updated Contents | `/metrics/top-updated-contents` | Table |
| Average Time to Publish by API | `/metrics/average-time-to-publish-by-api` | Horizontal Bar Chart |

Calendar Heatmap は `tim012432-calendarheatmap-panel` の Green カラースキームで日別件数を表示する。
ダッシュボードの time range（既定 `now-365d`）を `${__from}` / `${__to}` として API に渡す。
ダッシュボード timezone は `utc` とし、ヒートマップの日付バケットを S3 パーティション `dt`（Webhook 受信日の UTC 日付）と一致させる。
Top Updated Contents は API response の `count` field を Table 上では `updated_count` として表示し、`last_event_at` は field override で `dateTimeAsLocal` 表示とする。
Average Time to Publish by API は green `< 1日`、yellow `< 3日`、red `>= 3日` の threshold を使う。

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

## 12.2 任意 SQL API を提供しない

初期実装では、Grafana から任意 SQL を実行する API は提供しない。

理由:

- SQL injection や意図しないファイルアクセスのリスクを避けるため
- API の責務を Grafana 用 metrics に限定するため

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
