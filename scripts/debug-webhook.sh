#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <local_dir>" >&2
  exit 2
fi

LOCAL_DIR="$1"
PAYLOAD="$(
  printf '%s' \
    '{"service":"example-service","api":"blogs","id":"content-id","type":"edit",' \
    '"contents":{"old":{"status":["DRAFT"],"updatedAt":"2026-06-28T12:00:00Z"},' \
    '"new":{"status":["PUBLISH"],"updatedAt":"2026-06-29T12:00:00Z","publishValue":' \
    '{"createdAt":"2026-06-27T12:00:00Z","publishedAt":"2026-06-29T12:00:00Z"}}}}'
)"
SECRET="${MICROCMS_WEBHOOK_SECRET:-local-webhook-secret}"
SIGNATURE="$(printf '%s' "$PAYLOAD" | openssl dgst -sha256 -hmac "$SECRET" -binary | xxd -p -c 256)"
WEBHOOK_URL="$(tofu -chdir="$LOCAL_DIR" output -raw local_webhook_url)"

curl -i "$WEBHOOK_URL" \
  -H "content-type: application/json" \
  -H "x-microcms-signature: $SIGNATURE" \
  --data "$PAYLOAD"
