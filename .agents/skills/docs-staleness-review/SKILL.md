---
name: docs-staleness-review
description: Review microcms-content-ops-analytics-platform documentation for stale, contradictory, or drifting guidance. Use when the user asks to review docs, prevent documentation rot, check docs against requirements or implementation, update stale docs, or verify consistency across README, requirements, docs, code, scripts, justfile, infrastructure, Grafana, and AGENTS.md.
---

# Docs Staleness Review

## Overview

`microcms-content-ops-analytics-platform` の documentation を、requirements、実装、local debug、AWS deploy、agent workflow の一貫性 gate として review する。stale guidance、矛盾、scope drift、missing boundary、検証手順の不足、code-doc drift を、実装者・reviewer・運用者を誤誘導する前に見つける。

共通 guardrails は `../references/microcms-content-ops-analytics-guardrails.md` を参照する。

## Inputs and Outputs

Inputs:

- review 対象の documentation file、PR、branch、または repository area。
- 関連する `README.md`、requirements、docs、`AGENTS.md` guidance、implementation files、configuration、scripts、tests。
- user が指定した scope。例: full-doc review、PR-doc review、targeted stale-doc check。

Outputs:

- severity 順に並べた actionable staleness findings。可能な限り file と line reference を含める。
- stale、contradictory、incomplete な doc statement ごとの impact。
- 最小限の recommended documentation update と、残る review limits。

## Review Workflow

1. Identify the documentation surface.
   - task が full-doc review、PR-doc review、targeted stale-doc check のどれかを判断する。
   - 関連する requirements、docs、config、scripts、tests、変更された implementation を読む。
   - doc-only change でも、accepted behavior、data contracts、operator workflow、coding-agent workflow に影響するか確認する。

2. Check requirement consistency.
   - microCMS Webhook event log、S3 Parquet、DuckDB fixed metrics API、Grafana visualization という product boundary と一致するか確認する。
   - Management API 全件同期、任意 SQL API、複数テナント、厳密な監査ログ、リアルタイム分析などの non-scope が current scope として書かれていないか確認する。

3. Check technical consistency.
   - components の説明が current repository と一致することを確認する: `webhook-ingest`、`duckdb-query-api`、`grafana`、`infra/bootstrap`、`infra/aws`、`infra/local`。
   - AWS / local debug の用語が docs と実装に一致することを確認する: API Gateway、Lambda、S3、ECS Fargate、ALB、ECR、Floci、ngrok、Docker Compose、OpenTofu。
   - public API、environment variables、S3 key layout、Parquet schema、Grafana dashboard の説明が実装と一致するか確認する。

4. Check freshness against implementation.
   - commands、file paths、environment variables、APIs、generated artifact names を current repository と比較する。
   - docs 内の `just` commands が actual `justfile` と一致することを確認する。
   - Docker Compose files、OpenTofu directories、Grafana dashboard paths、Rust workspace members が current repository と一致することを確認する。
   - code が user-visible behavior、data contract、operator-visible workflow、environment variable、manual validation step を追加しているのに docs が未更新なら flag する。

5. Check verification guidance.
   - docs が automated checks、local debug validation、AWS deploy validation を区別していることを確認する。
   - Rust code 変更では `just fmt`、`just test`、`just clippy`、infrastructure / Compose / Grafana 変更では `just validate`、広範囲変更では `just check` が案内されているか確認する。
   - destructive / external actions である deploy、destroy、ECR push、S3 deletion の注意が十分か確認する。

6. Check documentation ownership, links, and language.
   - `README.md`、`requirements/`、`docs/`、`AGENTS.md` 間の links が解決でき、正しい source of truth を指していることを確認する。
   - repository docs は project policy に従い、簡潔な日本語 prose を基本にする。technical terms、commands、file paths、env vars、API paths、crate names、SQL names、S3 prefixes、provider names は英語のままでよい。

## Output Format

severity 順の actionable findings から始める。各 finding には以下を含める。

- 可能な場合は file と line reference
- current text が stale、contradictory、または incomplete である理由
- implementers、reviewers、operators への impact
- 最小限の recommended documentation update

findings の後に open questions または assumptions を置き、最後に短い summary を書く。issues がない場合は、その旨を明確に述べ、残る review limits を列挙する。

## Freshness Guardrails

- broad rewrites よりも、truth を回復する最小限の doc edits を優先する。
- speculative future work を current requirements、architecture、data contracts、operating instructions として正規化しない。
- command guidance は `justfile` と一致させる。
- deploy / destroy / secret / state / generated artifacts の扱いを曖昧にしない。
- user が明示的に別 locale を求めない限り、repository documentation は Japanese-language project policy に合わせる。

## Trigger Evaluation

典型的な trigger phrases:

- "docs が古くないか review して"
- "requirements と docs の矛盾を見て"
- "実装変更に対して docs 更新漏れがないか確認して"
- "AGENTS.md と README の整合性を確認して"
- "local debug / AWS deploy docs が実装と合っているか見て"
