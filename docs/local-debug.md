# ローカルデバッグ手順

この手順では、Floci を AWS emulator、ngrok を microCMS から到達可能な公開 URL として使い、`webhook-ingest` から S3 Parquet 保存、`duckdb-query-api` から集計 API までをローカルで確認します。

## 前提

- Docker / Docker Compose
- OpenTofu
- AWS CLI
- just
- ngrok authtoken

Floci は Docker image で起動するため、ローカルに `floci` CLI がなくても検証できます。

## 1. 環境変数を用意する

```bash
cp .env.example .env
```

`.env` の `NGROK_AUTHTOKEN` を設定します。

```dotenv
FLOCI_PORT=4566
FLOCI_LAMBDA_ENDPOINT=http://floci:4566
NGROK_AUTHTOKEN=xxxx
MICROCMS_WEBHOOK_SECRET=local-webhook-secret
```

以降のコマンドで `.env` の値を使えるように読み込みます。

```bash
set -a
source .env
set +a
export TF_VAR_microcms_webhook_secret="$MICROCMS_WEBHOOK_SECRET"
```

## 2. ローカルデバッグ環境を起動する

`just debug` は次をまとめて実行します。

- `webhook-ingest:local` image の build
- Floci の起動
- `infra/local` の OpenTofu init/apply
- `duckdb-query-api`、Grafana、ngrok の起動
- OpenTofu output と ngrok tunnel 情報の表示

```bash
just debug
```

`4566` が使用済みの場合は、ホスト側の Floci port を変更して起動します。

```bash
FLOCI_PORT=4567 just debug
```

`FLOCI_PORT` は手元の OpenTofu、AWS CLI、curl が使う host 側 port です。
Lambda コンテナから Floci S3 を呼ぶ endpoint は Docker network 内の `FLOCI_LAMBDA_ENDPOINT=http://floci:4566` を使います。

初回は Rust crate と Docker base image の取得があるため時間がかかります。`duckdb-query-api` は DuckDB 本体の C++ build が重いため、ローカルでも release profile を使い、既定で `CARGO_BUILD_JOBS=1` にしています。Docker BuildKit の cargo cache を使うため、2回目以降の build は差分中心になります。

Docker Desktop のメモリ割り当てが少ない環境では、`CARGO_BUILD_JOBS=1` のまま実行してください。十分にメモリがある場合だけ、次のように並列度を上げられます。

```bash
CARGO_BUILD_JOBS=2 just debug
```

個別に実行したい場合は次の recipe を使います。

```bash
just debug-build
just debug-up
just debug-apply
just debug-outputs
```

## 3. AWS CLI から Floci を確認する

AWS CLI が Floci を見るようにします。`just` recipe 内では必要な環境変数を渡しますが、手元で `aws` を直接実行する場合は次を設定します。

```bash
export AWS_ENDPOINT_URL=http://localhost:${FLOCI_PORT:-4566}
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_REGION=ap-northeast-1
```

疎通確認:

```bash
aws sts get-caller-identity
```

## 4. Webhook URL を確認する

```bash
just debug-outputs
```

`local_webhook_url` は手元から直接 POST する URL です。
`local_webhook_path` は ngrok の `public_url` に付けて microCMS Webhook の送信先にする path です。

ngrok の tunnel URL だけを確認したい場合:

```bash
curl -s http://localhost:4040/api/tunnels
```

`public_url` に `local_webhook_path` を付けた URL を microCMS Webhook の送信先に設定します。

例:

```text
https://<ngrok-id>.ngrok-free.app/restapis/<api-id>/local/_user_request_/webhook
```

## 6. 署名付き sample webhook を送る

microCMS を使わずに手元から確認する場合は、sample payload を送ります。

```bash
just debug-webhook
```

成功時は `{"ok":true,"s3_key":"..."}` が返ります。

## 7. ダミー Parquet を生成して投入する

microCMS Webhook を送らずに、手元で Parquet を生成して Floci S3 に投入できます。

### 少量（smoke）

動作確認用に 8 件の固定 fixture を生成します。layout は本番 ingest と同じ **1 event = 1 file** です。fixture は duckdb-query-api の統合テスト基準値に揃えており、再実行時は固定 event ID で同じ S3 key を上書きします。

`just debug-parquet-seed` は生成前にローカルの `microcms_events/` をクリアし、Floci S3 へは `aws s3 sync --delete` で `microcms_events/` 配下の `.parquet` を同期します。S3 上に残っていた旧 seed や webhook 由来の Parquet は、ローカルに無い分は削除されます。

```bash
just debug-parquet-seed
```

webhook 由来のデータを残したい場合は、seed 実行前に `just debug-parquet-persist` でバックアップするか、seed 後に `just debug-webhook` で再送してください。Floci S3 とローカル保存先をまとめて空にしたい場合は次を使います。

```bash
just debug-parquet-delete
```

### 1 年分（bulk）

Grafana dashboard の既定 time range（過去 365 日）に合わせ、**50,000 件 / 365 日分**のダミーデータを生成します。layout は partition 単位の batched file（local seed 専用）です。

- Calendar Heatmap: 全カレンダー日の約 80% にイベントを配置し、平日寄り・月内キャンペーン日寄りの自然な山谷と少量の 0 件日を残します
- API Activity: `INITIAL_DRAFT` / `SAVE_DRAFT` / `PUBLISH_FROM_DRAFT` / `INITIAL_PUBLISH` / `UPDATE_PUBLISHED` / `ADD_DRAFT_TO_PUBLISHED` / `DISCARD_DRAFT_ON_PUBLISHED` / `UNPUBLISH_TO_DRAFT` / `UNPUBLISH_TO_CLOSED` / `REOPEN_TO_DRAFT` / `REPUBLISH_FROM_CLOSED` / `DELETE_DRAFT` / `DELETE_PUBLISHED` / `DELETE_CLOSED` を realistic な比率で配分します
- Average Time to Publish / Average Draft to Publish: coordinated `metric-*` lifecycle ペアで API ごとの日数バラつきを持たせます。`blogs` / `authors` / `news` / `categories` / `pages` に加え、`advertisements` / `tags` / `labels` / `papers` / `cards` を生成対象にします
- Publish Action Trend: `PUBLISH_FROM_DRAFT` / `INITIAL_PUBLISH` / `REPUBLISH_FROM_CLOSED` は平均約 5 件/日、最大 20 件/日以下を目安に、365 日全体で後半に偏らない realistic schedule に沿って配置します

`--count` / `DEBUG_SEED_COUNT` で指定した件数どおりに生成します。smoke と同様、生成前にローカル `microcms_events/` をクリアし、S3 sync は `--delete` で prefix を置き換えます。

```bash
just debug-parquet-seed-large
```

件数や期間を変える場合:

```bash
DEBUG_SEED_COUNT=30000 DEBUG_SEED_DAYS=365 just debug-parquet-seed-large
```

## 8. S3 Parquet を確認する

```bash
just debug-s3-ls
```

`.parquet` ファイルが作成されていれば `webhook-ingest` の保存処理は成功です。
まだ `just debug-webhook` や `just debug-parquet-seed` を実行していない場合は、`No objects found under ...` と表示されます。

`just debug-webhook` は送信後に Floci S3 の `.parquet` ファイルを `.debug/parquet/microcms_events/` に同期します。
手動で同期したい場合は次を実行します。

```bash
just debug-parquet-persist
```

保存先を変える場合は `DEBUG_PARQUET_DIR` を指定します。

```bash
DEBUG_PARQUET_DIR=.debug/custom-parquet just debug-parquet-persist
```

debug で生成した Parquet を削除する場合は次を実行します。
このコマンドは Floci S3 の `microcms_events/` 以下の `.parquet` と、ローカルに永続化した directory を削除します。

```bash
just debug-parquet-delete
```

## 9. Query API と Grafana を確認する

```bash
just debug-metrics
```

Grafana:

```text
http://localhost:3000
```

## 10. 停止と削除

OpenTofu 管理リソースを削除します。

```bash
just debug-destroy
```

コンテナを停止します。

```bash
just debug-down
```

## トラブルシュート

- `401 Unauthorized`: `MICROCMS_WEBHOOK_SECRET` と署名計算に使った secret が一致しているか確認します。
- `500` で `failed to put object to s3: dispatch failure`: `just debug-apply` を実行して、Lambda 環境変数の `AWS_ENDPOINT_URL` が `http://floci:4566` になるように更新します。host 側の `http://localhost:${FLOCI_PORT:-4566}` は Lambda コンテナ内からは使えません。
- `ngrok tunnels:` に何も表示されない: `docker compose -f docker-compose.local.yml ps` で `ngrok` が起動中か確認し、`docker compose -f docker-compose.local.yml logs ngrok` で原因を見ます。
- ngrok URL に POST しても届かない: `curl -fsS http://localhost:4040/api/tunnels` で tunnel が `http://floci:4566` を向いているか確認します。
- Query API が S3 を読めない: `DUCKDB_S3_ENDPOINT=floci:4566`, `DUCKDB_S3_URL_STYLE=path`, `DUCKDB_S3_USE_SSL=false` が `duckdb-query-api` に渡っているか確認します。
- Floci の起動に `Bind for 0.0.0.0:4566 failed: port is already allocated` が出る: `FLOCI_PORT=4567 just debug` のようにホスト側 port を変更します。
- `duckdb-query-api` の Docker build で `cannot allocate memory` が出る: `.env` または実行時の `CARGO_BUILD_JOBS=1` を確認し、Docker Desktop の Memory 上限を増やしてから再実行します。途中で失敗した build cache が残っている場合は、再実行前に Docker Desktop 側で停止中の build がないことも確認します。
- Floci 上の S3 が空: まず `just debug-webhook` または `just debug-parquet-seed` / `just debug-parquet-seed-large` でサンプルイベントを投入します。直接確認する場合は `aws s3api list-objects-v2 --endpoint-url=http://localhost:${FLOCI_PORT:-4566} --bucket "$BUCKET" --prefix microcms_events/` で endpoint を明示します。
