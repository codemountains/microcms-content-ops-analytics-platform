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
- API ごとの `event_kind` 件数はどうなっているか
- 作成から公開・再公開アクションまでにどれくらい時間がかかっているか
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

`dt` は Webhook 受信時刻を JST（日本時間、UTC+09:00）のカレンダー日に変換した日付とする。
Parquet の `received_at` 列は UTC タイムスタンプのまま保持する。

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
`time` は S3 パーティション `dt` と同じ **JST カレンダー日** を表し、`YYYY-MM-DDT00:00:00+09:00` 形式で返す。

Query parameters:

| Parameter | Default | 説明 |
| --- | --- | --- |
| `from` | なし | 集計開始時刻（Unix epoch ミリ秒）。`to` とセットで指定する |
| `to` | なし | 集計終了時刻（Unix epoch ミリ秒）。`from` とセットで指定する |

`from` / `to` を省略した場合は、直近 365 日を返す。
Grafana ダッシュボードでは Infinity datasource の backend-interpolated time macro `${__timeFrom}` / `${__timeTo}` を渡す。

Response example:

```json
[
  {
    "time": "2026-06-29T00:00:00+09:00",
    "value": 12
  }
]
```

#### `GET /metrics/api-activity`

API ごとの `event_kind` 別件数を返す。
`event_kind` が `NULL` または想定外の値の場合、既知 event_kind の series には含めず、`total_count` には含める。

Query parameters:

| Parameter | Default | 説明 |
| --- | --- | --- |
| `days` | `30` | 集計対象期間 |

Response example:

```json
[
  {
    "api": "blogs",
    "initial_draft_count": 2,
    "save_draft_count": 5,
    "publish_from_draft_count": 4,
    "initial_publish_count": 1,
    "update_published_count": 20,
    "add_draft_to_published_count": 3,
    "discard_draft_on_published_count": 2,
    "unpublish_to_draft_count": 1,
    "unpublish_to_closed_count": 1,
    "reopen_to_draft_count": 1,
    "republish_from_closed_count": 1,
    "delete_draft_count": 1,
    "delete_published_count": 2,
    "delete_closed_count": 1,
    "total_count": 45
  }
]
```

#### `GET /metrics/publish-action-summary`

公開 KPI 用に、対象期間内の公開アクション数と公開状態率を返す。
公開アクションは `event_kind IN ('PUBLISH_FROM_DRAFT', 'INITIAL_PUBLISH', 'REPUBLISH_FROM_CLOSED')` とする。
公開状態率は、状態到達・維持イベントのうち公開状態に到達・維持した割合とする。
`published_state_count` は `PUBLISH_FROM_DRAFT` / `INITIAL_PUBLISH` / `UPDATE_PUBLISHED` / `REPUBLISH_FROM_CLOSED` の合計、`state_arrival_count` は `INITIAL_DRAFT` / `SAVE_DRAFT` / `PUBLISH_FROM_DRAFT` / `INITIAL_PUBLISH` / `UPDATE_PUBLISHED` / `UNPUBLISH_TO_DRAFT` / `UNPUBLISH_TO_CLOSED` / `REOPEN_TO_DRAFT` / `REPUBLISH_FROM_CLOSED` の合計とする。
`ADD_DRAFT_TO_PUBLISHED` / `DISCARD_DRAFT_ON_PUBLISHED` / `DELETE_*` / `event_kind IS NULL` は公開状態率の計算から除外する。
`state_arrival_count = 0` の場合は `published_state_rate = null` とする。

Query parameters:

| Parameter | Default | 説明 |
| --- | --- | --- |
| `days` | `30` | 集計対象期間。今日の公開数 / 公開状態率パネルでは `1` を指定する |

Response example:

```json
[
  {
    "publish_count": 7,
    "published_state_count": 14,
    "state_arrival_count": 20,
    "published_state_rate": 0.7
  }
]
```

#### `GET /metrics/publish-action-trend`

日別の公開アクション数を返す。
`PUBLISH_FROM_DRAFT`、`INITIAL_PUBLISH`、`REPUBLISH_FROM_CLOSED` を別 series として返し、`publish_count` には合計を返す。
`time` は S3 パーティション `dt` と同じ **JST カレンダー日** を表し、`YYYY-MM-DDT00:00:00+09:00` 形式で返す。
0件の日も含める。

Query parameters:

| Parameter | Default | 説明 |
| --- | --- | --- |
| `from` | なし | 集計開始時刻（Unix epoch ミリ秒）。`to` とセットで指定する |
| `to` | なし | 集計終了時刻（Unix epoch ミリ秒）。`from` とセットで指定する |

`from` / `to` を省略した場合は、直近 365 日を返す。
Grafana ダッシュボードでは Calendar Heatmap と同じく Infinity datasource の backend-interpolated time macro `${__timeFrom}` / `${__timeTo}` を渡す。

Response example:

```json
[
  {
    "time": "2026-06-29T00:00:00+09:00",
    "publish_from_draft_count": 4,
    "initial_publish_count": 1,
    "republish_from_closed_count": 2,
    "publish_count": 7
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

API ごとに、コンテンツ作成 (`createdAt`) から公開・再公開アクション (`publishedAt`) までの平均所要時間を返す。
`event_kind IN ('PUBLISH_FROM_DRAFT', 'INITIAL_PUBLISH', 'REPUBLISH_FROM_CLOSED')` を対象にし、timestamp は `contents.new.publishValue` から抽出した値を使う。
`unit` により平均日数または平均時間を選択する。

Query parameters:

| Parameter | Default | 説明 |
| --- | --- | --- |
| `days` | `30` | 集計対象期間 |
| `unit` | `days` | `days` または `hours` |

レスポンスは Grafana Infinity datasource が各 column を同じ長さの field として扱えるよう、`avg_days` と `avg_hours` の両方を返す。
選択していない unit の field は `null` とする。

`unit=days` response example:

```json
[
  {
    "api": "blogs",
    "avg_days": 1.25,
    "avg_hours": null
  }
]
```

`unit=hours` response example:

```json
[
  {
    "api": "blogs",
    "avg_days": null,
    "avg_hours": 30.0
  }
]
```

#### `GET /metrics/average-draft-to-publish-by-api`

API ごとに、下書き作成 (`contents.new.draftValue.createdAt`) から下書き経由の公開 (`contents.new.publishValue.publishedAt`) までの平均所要時間を返す。
同一 `api` / `content_id` の `INITIAL_DRAFT` と `PUBLISH_FROM_DRAFT` を結合し、`SAVE_DRAFT` / `INITIAL_PUBLISH` は対象に含めない。
複数の `INITIAL_DRAFT` / `PUBLISH_FROM_DRAFT` がある場合は、それぞれ最初の timestamp (`MIN`) を使う。
期間フィルタは `PUBLISH_FROM_DRAFT` 側の `dt` に適用し、`published_at < draft_at` になる異常ペアは除外する。
`unit` により平均日数または平均時間を選択する。

Query parameters:

| Parameter | Default | 説明 |
| --- | --- | --- |
| `days` | `30` | 集計対象期間 |
| `unit` | `days` | `days` または `hours` |

レスポンスは Grafana Infinity datasource が各 column を同じ長さの field として扱えるよう、`avg_days` と `avg_hours` の両方を返す。
選択していない unit の field は `null` とする。`sample_count` は平均算出件数の確認用であり、duration chart では描画しない。

`unit=days` response example:

```json
[
  {
    "api": "blogs",
    "avg_days": 3.5,
    "avg_hours": null,
    "sample_count": 12
  }
]
```

`unit=hours` response example:

```json
[
  {
    "api": "blogs",
    "avg_days": null,
    "avg_hours": 84.0,
    "sample_count": 12
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
| `draft_created_at` | timestamp | yes | `contents.new.draftValue.createdAt` |
| `content_created_at` | timestamp | yes | `contents.new.publishValue.createdAt` |
| `content_published_at` | timestamp | yes | `contents.new.publishValue.publishedAt` |
| `raw_payload` | string | no | Webhook payload の原文 |

## 6.2 event_kind

`event_kind` は microCMS Webhook の `type` と `contents.old.status` / `contents.new.status` から導出する。
確実に分類できないイベントでは `NULL` とする。

| event_kind | 判定 |
| --- | --- |
| `INITIAL_DRAFT` | `type = new` かつ old status がなく、new status が `DRAFT` |
| `SAVE_DRAFT` | `type = edit` かつ old/new status がともに `DRAFT` |
| `PUBLISH_FROM_DRAFT` | `type = edit` かつ old status が `DRAFT`、new status が `PUBLISH` |
| `INITIAL_PUBLISH` | `type = new` かつ old status がなく、new status が `PUBLISH` |
| `UPDATE_PUBLISHED` | `type = edit` かつ old/new status がともに `PUBLISH` |
| `ADD_DRAFT_TO_PUBLISHED` | `type = edit` かつ old status set が `PUBLISH`、new status set が `PUBLISH` と `DRAFT` を含む |
| `DISCARD_DRAFT_ON_PUBLISHED` | `type = edit` かつ old status set が `PUBLISH` と `DRAFT` を含み、new status set が `PUBLISH` |
| `UNPUBLISH_TO_DRAFT` | `type = edit` かつ old status が `PUBLISH`、new status が `DRAFT` |
| `UNPUBLISH_TO_CLOSED` | `type = edit` かつ old status が `PUBLISH`、new status が `CLOSED` |
| `REOPEN_TO_DRAFT` | `type = edit` かつ old status が `CLOSED`、new status が `DRAFT` |
| `REPUBLISH_FROM_CLOSED` | `type = edit` かつ old status が `CLOSED`、new status が `PUBLISH` |
| `DELETE_DRAFT` | `type = delete` かつ old status が `DRAFT`、new status がない |
| `DELETE_PUBLISHED` | `type = delete` かつ old status が `PUBLISH`、new status がない |
| `DELETE_CLOSED` | `type = delete` かつ old status が `CLOSED`、new status がない |

`PUBLISH` と `DRAFT` を同時に含む status は、`ADD_DRAFT_TO_PUBLISHED` / `DISCARD_DRAFT_ON_PUBLISHED` の special case を先に判定する。
それ以外では公開中である事実を優先し、`PUBLISH` を含む status set は `PUBLISH` とみなす。
`PUBLISH` を含まない複合 status や未知 status は `NULL` とし、既知 series には含めない。

### 6.2.1 event_kind カテゴリ（API Activity 集約用）

Grafana の API Activity パネルでは、既定で次の 4 カテゴリに集約して表示する。
`api_activity_view` が詳細表示のときは、API が返す 14 種の個別 count を表示する。

| カテゴリ field | 表示名 | 含める `event_kind` |
| --- | --- | --- |
| `draft_activity` | 下書き操作 | `INITIAL_DRAFT`, `SAVE_DRAFT`, `ADD_DRAFT_TO_PUBLISHED`, `DISCARD_DRAFT_ON_PUBLISHED` |
| `publish_activity` | 公開・更新 | `PUBLISH_FROM_DRAFT`, `INITIAL_PUBLISH`, `UPDATE_PUBLISHED`, `REPUBLISH_FROM_CLOSED` |
| `unpublish_activity` | 非公開・再開 | `UNPUBLISH_TO_DRAFT`, `UNPUBLISH_TO_CLOSED`, `REOPEN_TO_DRAFT` |
| `delete_activity` | 削除 | `DELETE_DRAFT`, `DELETE_PUBLISHED`, `DELETE_CLOSED` |

各カテゴリ count は所属 `event_kind` の合計とする。`GET /metrics/api-activity` の response shape は 14 種の個別 count を維持し、集約は Grafana transformation で行う。

## 6.3 Partition

S3 key は次の partition を含む。

| Partition | 説明 |
| --- | --- |
| `service` | microCMS service ID |
| `api` | microCMS API ID |
| `dt` | Webhook 受信時刻を JST に変換したカレンダー日 |

例:

```text
microcms_events/service=my-service/api=blogs/dt=2026-06-29/01JXXXXXXXX.parquet
```

## 7. 集計 SQL

## 7.1 Calendar Heatmap

```sql
WITH bounds AS (
  SELECT
    CAST(epoch_ms(<from_ms>) + INTERVAL '9 HOURS' AS DATE) AS start_date,
    CAST(epoch_ms(<to_ms>) + INTERVAL '9 HOURS' AS DATE) AS end_date
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
  CONCAT(CAST(calendar.dt AS VARCHAR), 'T00:00:00+09:00') AS time,
  COALESCE(daily.event_count, 0) AS value
FROM calendar
LEFT JOIN daily ON daily.dt = calendar.dt
ORDER BY calendar.dt;
```

## 7.2 API Activity

```sql
SELECT
  api,
  SUM(CASE WHEN event_kind = 'INITIAL_DRAFT' THEN 1 ELSE 0 END) AS initial_draft_count,
  SUM(CASE WHEN event_kind = 'SAVE_DRAFT' THEN 1 ELSE 0 END) AS save_draft_count,
  SUM(CASE WHEN event_kind = 'PUBLISH_FROM_DRAFT' THEN 1 ELSE 0 END) AS publish_from_draft_count,
  SUM(CASE WHEN event_kind = 'INITIAL_PUBLISH' THEN 1 ELSE 0 END) AS initial_publish_count,
  SUM(CASE WHEN event_kind = 'UPDATE_PUBLISHED' THEN 1 ELSE 0 END) AS update_published_count,
  SUM(CASE WHEN event_kind = 'ADD_DRAFT_TO_PUBLISHED' THEN 1 ELSE 0 END) AS add_draft_to_published_count,
  SUM(CASE WHEN event_kind = 'DISCARD_DRAFT_ON_PUBLISHED' THEN 1 ELSE 0 END) AS discard_draft_on_published_count,
  SUM(CASE WHEN event_kind = 'UNPUBLISH_TO_DRAFT' THEN 1 ELSE 0 END) AS unpublish_to_draft_count,
  SUM(CASE WHEN event_kind = 'UNPUBLISH_TO_CLOSED' THEN 1 ELSE 0 END) AS unpublish_to_closed_count,
  SUM(CASE WHEN event_kind = 'REOPEN_TO_DRAFT' THEN 1 ELSE 0 END) AS reopen_to_draft_count,
  SUM(CASE WHEN event_kind = 'REPUBLISH_FROM_CLOSED' THEN 1 ELSE 0 END) AS republish_from_closed_count,
  SUM(CASE WHEN event_kind = 'DELETE_DRAFT' THEN 1 ELSE 0 END) AS delete_draft_count,
  SUM(CASE WHEN event_kind = 'DELETE_PUBLISHED' THEN 1 ELSE 0 END) AS delete_published_count,
  SUM(CASE WHEN event_kind = 'DELETE_CLOSED' THEN 1 ELSE 0 END) AS delete_closed_count,
  COUNT(*) AS total_count
FROM read_parquet(
  '<EVENTS_PATH>',
  hive_partitioning = true,
  union_by_name = true
)
WHERE dt >= CAST(CAST(current_timestamp AS TIMESTAMP) + INTERVAL '9 HOURS' AS DATE) - INTERVAL 29 DAY
GROUP BY api
ORDER BY total_count DESC, api;
```

## 7.3 Publish Action Summary

```sql
WITH bounds AS (
  SELECT
    CAST(CAST(current_timestamp AS TIMESTAMP) + INTERVAL '9 HOURS' AS DATE) - INTERVAL 0 DAY AS start_date,
    CAST(CAST(current_timestamp AS TIMESTAMP) + INTERVAL '9 HOURS' AS DATE) AS end_date
),
summary AS (
  SELECT
    COALESCE(SUM(CASE WHEN event_kind IN ('PUBLISH_FROM_DRAFT', 'INITIAL_PUBLISH', 'REPUBLISH_FROM_CLOSED') THEN 1 ELSE 0 END), 0) AS publish_count,
    COALESCE(SUM(CASE WHEN event_kind IN ('PUBLISH_FROM_DRAFT', 'INITIAL_PUBLISH', 'UPDATE_PUBLISHED', 'REPUBLISH_FROM_CLOSED') THEN 1 ELSE 0 END), 0) AS published_state_count,
    COALESCE(SUM(CASE WHEN event_kind IN ('INITIAL_DRAFT', 'SAVE_DRAFT', 'PUBLISH_FROM_DRAFT', 'INITIAL_PUBLISH', 'UPDATE_PUBLISHED', 'UNPUBLISH_TO_DRAFT', 'UNPUBLISH_TO_CLOSED', 'REOPEN_TO_DRAFT', 'REPUBLISH_FROM_CLOSED') THEN 1 ELSE 0 END), 0) AS state_arrival_count
  FROM read_parquet(
    '<EVENTS_PATH>',
    hive_partitioning = true,
    union_by_name = true
  )
  WHERE
    dt >= (SELECT start_date FROM bounds)
    AND dt <= (SELECT end_date FROM bounds)
)
SELECT
  publish_count,
  published_state_count,
  state_arrival_count,
  CASE
    WHEN state_arrival_count = 0 THEN NULL
    ELSE published_state_count::DOUBLE / state_arrival_count
  END AS published_state_rate
FROM summary;
```

`days=1` の場合は `INTERVAL 0 DAY`、`days=30` の場合は `INTERVAL 29 DAY` のように、対象期間は JST の `dt` に対して開始日・終了日の両端を含める。

## 7.4 Publish Action Trend

```sql
WITH bounds AS (
  SELECT
    CAST(epoch_ms(<from_ms>) + INTERVAL '9 HOURS' AS DATE) AS start_date,
    CAST(epoch_ms(<to_ms>) + INTERVAL '9 HOURS' AS DATE) AS end_date
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
    SUM(CASE WHEN event_kind = 'PUBLISH_FROM_DRAFT' THEN 1 ELSE 0 END) AS publish_from_draft_count,
    SUM(CASE WHEN event_kind = 'INITIAL_PUBLISH' THEN 1 ELSE 0 END) AS initial_publish_count,
    SUM(CASE WHEN event_kind = 'REPUBLISH_FROM_CLOSED' THEN 1 ELSE 0 END) AS republish_from_closed_count
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
  CONCAT(CAST(calendar.dt AS VARCHAR), 'T00:00:00+09:00') AS time,
  COALESCE(daily.publish_from_draft_count, 0) AS publish_from_draft_count,
  COALESCE(daily.initial_publish_count, 0) AS initial_publish_count,
  COALESCE(daily.republish_from_closed_count, 0) AS republish_from_closed_count,
  COALESCE(daily.publish_from_draft_count, 0) + COALESCE(daily.initial_publish_count, 0) + COALESCE(daily.republish_from_closed_count, 0) AS publish_count
FROM calendar
LEFT JOIN daily ON daily.dt = calendar.dt
ORDER BY calendar.dt;
```

## 7.5 Top Updated Contents

```sql
SELECT
  api,
  content_id,
  COUNT(*) AS count,
  strftime(MAX(received_at), '%Y-%m-%dT%H:%M:%SZ') AS last_event_at
FROM read_parquet(
  '<EVENTS_PATH>',
  hive_partitioning = true,
  union_by_name = true
)
WHERE
  dt >= CAST(CAST(current_timestamp AS TIMESTAMP) + INTERVAL '9 HOURS' AS DATE) - INTERVAL 29 DAY
  AND event_type IN ('new', 'edit')
  AND content_id IS NOT NULL
GROUP BY api, content_id
ORDER BY count DESC, MAX(received_at) DESC
LIMIT 20;
```

## 7.6 Average Time to Publish by API

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
  dt >= CAST(CAST(current_timestamp AS TIMESTAMP) + INTERVAL '9 HOURS' AS DATE) - INTERVAL 29 DAY
  AND event_kind IN ('PUBLISH_FROM_DRAFT', 'INITIAL_PUBLISH', 'REPUBLISH_FROM_CLOSED')
  AND content_created_at IS NOT NULL
  AND content_published_at IS NOT NULL
  AND content_published_at >= content_created_at
GROUP BY api
ORDER BY avg_days DESC, api;
```

`unit=hours` の場合は、同じ秒差を `/ 3600.0` して `avg_hours` として返す。

## 7.7 Average Draft to Publish by API

```sql
WITH drafts AS (
  SELECT
    api,
    content_id,
    MIN(draft_created_at) AS draft_at
  FROM read_parquet(
    '<EVENTS_PATH>',
    hive_partitioning = true,
    union_by_name = true
  )
  WHERE
    event_kind = 'INITIAL_DRAFT'
    AND content_id IS NOT NULL
    AND draft_created_at IS NOT NULL
  GROUP BY api, content_id
),
publishes_from_draft AS (
  SELECT
    api,
    content_id,
    MIN(content_published_at) AS published_at
  FROM read_parquet(
    '<EVENTS_PATH>',
    hive_partitioning = true,
    union_by_name = true
  )
  WHERE
    dt >= CAST(CAST(current_timestamp AS TIMESTAMP) + INTERVAL '9 HOURS' AS DATE) - INTERVAL 29 DAY
    AND event_kind = 'PUBLISH_FROM_DRAFT'
    AND content_id IS NOT NULL
    AND content_published_at IS NOT NULL
  GROUP BY api, content_id
)
SELECT
  drafts.api,
  AVG(date_diff('second', drafts.draft_at, publishes_from_draft.published_at) / 86400.0) AS avg_days,
  COUNT(*) AS sample_count
FROM drafts
INNER JOIN publishes_from_draft
  ON drafts.api = publishes_from_draft.api
  AND drafts.content_id = publishes_from_draft.content_id
WHERE publishes_from_draft.published_at >= drafts.draft_at
GROUP BY drafts.api
ORDER BY avg_days DESC, drafts.api;
```

`unit=hours` の場合は、同じ秒差を `/ 3600.0` して `avg_hours` として返す。

## 8. Grafana 仕様

Grafana は `duckdb-query-api` に HTTP request を送り、JSON response をパネルとして可視化する。
ローカル Docker Compose では file provisioning を使い、AWS デプロイ後は既存の Grafana Cloud stack に対して HTTP API で同じ datasource / dashboard を反映できるようにする。
Grafana Cloud stack 自体の作成、plugin 自動 install、Cloud Access Policy token を使う plugin 管理は初期スコープ外とする。

初期ダッシュボードでは、次のパネルを想定する。

| パネル | API | 可視化形式 |
| --- | --- | --- |
| Calendar Heatmap | `/metrics/calendar-heatmap` | `tim012432-calendarheatmap-panel` |
| Today Publish Count | `/metrics/publish-action-summary` | Stat |
| Published State Rate | `/metrics/publish-action-summary` | Gauge |
| Publish Action Trend | `/metrics/publish-action-trend` | Time series |
| API Activity | `/metrics/api-activity` | Stacked Bar Chart |
| Top Updated Contents | `/metrics/top-updated-contents` | Table |
| Average Time to Publish by API | `/metrics/average-time-to-publish-by-api` | Horizontal Bar Chart |
| Average Draft to Publish by API | `/metrics/average-draft-to-publish-by-api` | Horizontal Bar Chart |

各 panel には Grafana 標準の `description` を設定し、パネルタイトル横の情報アイコンから指標定義を確認できるようにする。
`description` は次の内容と矛盾しないこと。

| パネル | description に含める指標定義 |
| --- | --- |
| Calendar Heatmap | Webhook 受信日（S3 パーティション `dt`、JST カレンダー日）ごとのイベント件数。ダッシュボードの time range（`${__timeFrom}` / `${__timeTo}`）で絞り込み、0 件の日も表示する。 |
| Today Publish Count | 今日の `PUBLISH_FROM_DRAFT` / `INITIAL_PUBLISH` / `REPUBLISH_FROM_CLOSED` の合計数。日付境界は JST の `dt` partition に揃える。 |
| Published State Rate | 今日の状態到達・維持イベントのうち公開状態に到達・維持した割合。分子は `PUBLISH_FROM_DRAFT` / `INITIAL_PUBLISH` / `UPDATE_PUBLISHED` / `REPUBLISH_FROM_CLOSED`、分母は `INITIAL_DRAFT` / `SAVE_DRAFT` / `PUBLISH_FROM_DRAFT` / `INITIAL_PUBLISH` / `UPDATE_PUBLISHED` / `UNPUBLISH_TO_DRAFT` / `UNPUBLISH_TO_CLOSED` / `REOPEN_TO_DRAFT` / `REPUBLISH_FROM_CLOSED` とする。`ADD_DRAFT_TO_PUBLISHED` / `DISCARD_DRAFT_ON_PUBLISHED` / `DELETE_*` は除外し、分母が 0 件の場合は `null` とする。 |
| Publish Action Trend | 日別の `PUBLISH_FROM_DRAFT` / `INITIAL_PUBLISH` / `REPUBLISH_FROM_CLOSED` 件数。ダッシュボードの time range（`${__timeFrom}` / `${__timeTo}`）で絞り込み、Time series の bar 表示で stacked series とし、0 件の日も表示する。 |
| API Activity | API ごとの `event_kind` 件数（直近 30 日）。既定は 4 カテゴリ（下書き操作 / 公開・更新 / 非公開・再開 / 削除）を horizontal stacked bar で表示する。`api_activity_view` で 14 種の詳細内訳に切り替えられる。 |
| Top Updated Contents | `event_type IN ('new', 'edit')` かつ `content_id` があるイベントを対象に、更新回数が多いコンテンツ上位 20 件（直近 30 日）を表示する。`updated_count` は API の `count`、`last_event_at` は最終イベント時刻。 |
| Average Time to Publish by API | API ごとに、コンテンツ作成（`publishValue.createdAt`）から公開到達（`publishValue.publishedAt`）までの平均所要時間を表示する。`PUBLISH_FROM_DRAFT` / `INITIAL_PUBLISH` / `REPUBLISH_FROM_CLOSED` を対象（直近 30 日）にし、`publish_duration_unit` で日数 / 時間を切り替える。 |
| Average Draft to Publish by API | API ごとに、下書き作成（`draftValue.createdAt`）から下書き経由公開（`publishValue.publishedAt`）までの平均所要時間を表示する。同一 `api` / `content_id` の `INITIAL_DRAFT` と `PUBLISH_FROM_DRAFT` を結合する。期間フィルタは `PUBLISH_FROM_DRAFT` 側の `dt` に適用する（直近 30 日）。 |

API Activity は `/metrics/api-activity?days=30` の 14 種 count を Infinity datasource で取得し、Grafana transformation で 4 カテゴリ（`draft_activity` / `publish_activity` / `unpublish_activity` / `delete_activity`）に集約して stacked series として表示する。dashboard variable `api_activity_view` が詳細表示のときは `initial_draft_count` から `delete_closed_count` までの 14 系列を stacked bar で表示する。集約ルールは §6.2.1 に従う。
Calendar Heatmap は `tim012432-calendarheatmap-panel` の Green カラースキームで日別件数を表示する。
Today Publish Count と Published State Rate は Calendar Heatmap の直下に横並びで配置し、その下に Publish Action Trend を全幅で配置する。
Today Publish Count は `/metrics/publish-action-summary?days=1` の `publish_count`、Published State Rate は同 API の `published_state_rate` を描画する。
Publish Action Trend は `/metrics/publish-action-trend?from=${__timeFrom}&to=${__timeTo}` の `publish_from_draft_count`、`initial_publish_count`、`republish_from_closed_count` を stacked bar として描画する。
Calendar Heatmap と Publish Action Trend では、ダッシュボードの time range（既定 `now-365d`）を Infinity datasource の backend-interpolated time macro `${__timeFrom}` / `${__timeTo}` として API に渡す。
ダッシュボード timezone は `Asia/Tokyo` とし、ヒートマップの日付バケットを S3 パーティション `dt`（Webhook 受信日の JST 日付）と一致させる。
Top Updated Contents は API response の `count` field を Table 上では `updated_count` として表示し、`last_event_at` は field override で `dateTimeAsLocal` 表示とする。
Average Time to Publish by API は dashboard variable `publish_duration_unit` により `days` / `hours` を切り替え、API には `unit=${publish_duration_unit}` を渡す。
初期値は `days` とする。
`days` 表示では `avg_days` を描画し、green `< 1日`、yellow `< 3日`、red `>= 3日` の threshold を使う。
`hours` 表示では `avg_hours` を描画し、green `< 24h`、yellow `< 72h`、red `>= 72h` の threshold を使う。
Average Draft to Publish by API は Average Time to Publish by API と並置し、dashboard variable `publish_duration_unit` により `days` / `hours` を切り替え、API には `unit=${publish_duration_unit}` を渡す。`days` 表示では `avg_days`、`hours` 表示では `avg_hours` を描画する。API response の `sample_count` は平均算出件数の確認用であり、duration chart では描画しない。
dashboard variable `api_activity_view` は API Activity パネルの表示列を切り替える。既定は 4 カテゴリ集約、詳細表示では 14 種の個別 series を表示する。

Grafana Cloud provisioning は次の contract とする。

- 既存 Grafana Cloud stack の URL と service account token を入力として使う。
- `QUERY_API_URL` が未指定の場合は `infra/aws` の OpenTofu output `query_api_url` を使う。
- datasource uid は `duckdb-query-api`、type は `yesoreyeram-infinity-datasource`、`access` は `proxy` とする。
- dashboard uid は既定で `microcms-content-ops` とし、`grafana/dashboards/microcms-content-ops-analytics.json` を冪等に upsert する。
- `yesoreyeram-infinity-datasource` と `tim012432-calendarheatmap-panel` は Grafana Cloud stack に事前インストール済みであることを前提にする。未インストール時は明確に失敗し、明示 opt-out の場合だけ確認を skip できる。

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
| コスト | S3 + Lambda + DuckDB Query API + ローカル Grafana または既存 Grafana Cloud を中心に低コストで構成する |
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
- Grafana 13 の新 dashboard API への移行
- ECS Fargate での `duckdb-query-api` 常時稼働
- Basic 認証または reverse proxy による API 保護
- S3 lifecycle policy による保存期間管理
