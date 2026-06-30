---
name: tdd-implement
description: Implement, fix, or refactor microcms-content-ops-analytics-platform code with a test-driven workflow as the default. Use for feature work, bug fixes, refactors, and regression-test additions that change Rust code, tests, configuration, scripts, SQL, Docker, OpenTofu, Grafana, or infrastructure. Do not use for documentation-only changes to requirements, docs, AGENTS.md, README, or other prose-only files.
---

# TDD Implement

## Overview

`microcms-content-ops-analytics-platform` の implement / fix / refactor は、原則として focused test first で進める。まず requirements / docs / existing code を確認し、期待する behavior と failure mode を明確にする。次に test を追加または更新し、意図した理由で fail することを確認してから、必要十分な production change を行う。

共通 guardrails は `../references/microcms-content-ops-analytics-guardrails.md` を参照する。

## Out Of Scope

以下はこの skill の対象外とする。TDD や failing test は要求しない。

- `requirements/`、`docs/`、`AGENTS.md`、`README`、その他 prose-only の documentation edits
- typo fix、link fix、wording update など、実行時 behavior を変えない docs-only changes
- `$docs-staleness-review` や通常の documentation workflow で扱うべき docs review / update

code と docs を同一 task で変更する場合は、この skill を使う。docs 更新は accompanying change として扱い、behavior を固定する test を先に書く対象は code / config / scripts 側とする。

## Inputs and Outputs

Inputs:

- 実装または修正する behavior を説明する user request。
- 関連する requirements、docs、existing code、test files、configuration。
- repository から確認した test runner、`just` recipe、manual validation workflow。

Outputs:

- stack が tests をサポートしている場合、failing-then-passing focused test に裏付けられた narrow code change。
- 実行した focused checks、repository checks、未実行 checks を明記した verification results。
- local / cloud で実行できなかった manual validation や residual risk の明示。

## Workflow

1. Inspect the repository before choosing tools.
   - `git status --short --branch` を確認し、user の unrelated changes を壊さない。
   - 関連する `README.md`、`requirements/`、`docs/`、nearby implementation、existing tests、configuration を読む。
   - `justfile`、Docker Compose、OpenTofu、Grafana dashboard、`.env.example` に影響するか確認する。

2. Define the narrow behavior under test.
   - 編集前に expected behavior、inputs、outputs、failure mode、acceptance criteria を短く整理する。
   - deterministic logic には focused unit tests を優先する。
   - AWS / S3 / DuckDB / Docker / OpenTofu が絡む場合も、まず fixture、local path、adapter、validation command で確認できる最小境界を選ぶ。

3. Write the failing test first.
   - requested behavior を表す test を追加または更新する。
   - focused test を実行し、意図した理由で fail することを確認する。
   - 実装前に test が pass する場合は、behavior が既に存在する可能性を確認し、必要なら user に結果を報告する。

4. Implement the necessary and sufficient production change.
   - `../references/microcms-content-ops-analytics-guardrails.md` の product boundary、data/API contracts、security に沿う。
   - public API、S3 key layout、Parquet schema、environment variables、`just` commands を変える場合は docs への影響を同時に確認する。

5. Refactor only after green.
   - focused test を再実行し、pass を確認する。
   - refactor は green 後に行い、変更対象または必要な local duplication に限定する。
   - unrelated cleanup、metadata churn、広い rename、unrequested abstraction は避ける。

6. Run verification.
   - changed component の focused checks を実行する。
   - 変更範囲に応じて `just fmt`、`just test`、`just clippy`、`just validate`、`just check` を選ぶ。
   - 実行できなかった command と残 risk を明記する。

## Test Targets

変更内容に応じて、以下のような focused tests を優先する。

- Webhook ingest: HMAC-SHA256 signature verification、base64 body handling、payload normalization、status/title extraction、error response。
- Parquet / S3: Arrow RecordBatch generation、Parquet conversion、Hive partition-compatible S3 key construction、prefix handling。
- Query API: request parameter validation、fixed metric SQL behavior、DuckDB local Parquet query、JSON response shape、error conversion。
- Infrastructure / operations: Docker Compose config、OpenTofu validate、Grafana dashboard JSON validation、environment variable wiring。

## Trigger Evaluation

典型的な trigger phrases:

- "実装して"
- "この bug を直して"
- "リファクタして"
- "TDD で実装して"
- "先に failing test を書いてから直して"
- "red-green-refactor で進めて"
- "回帰テストを追加して修正して"

典型的な non-trigger phrases:

- "docs を更新して"
- "README を直して"
- "requirements を書いて"
- "ドキュメントの文言を修正して"
