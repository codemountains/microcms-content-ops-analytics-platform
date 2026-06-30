---
name: code-review
description: Review microcms-content-ops-analytics-platform code changes for correctness, architecture fit, security, data handling, tests, documentation impact, and operational risk. Use when the user asks for code review, PR review, owner review, maintainer review, final approval review, risk-focused review, or pre-commit review in this repository.
---

# Code Review

## Overview

`microcms-content-ops-analytics-platform` の coding agent / maintainer として、変更を correctness、architecture fit、data handling、security、verification、operational risk の観点で review する。style preference よりも、実際の bug、regression、scope drift、missing verification、docs drift を優先して指摘する。

共通 guardrails は `../references/microcms-content-ops-analytics-guardrails.md` を参照する。

## Inputs and Outputs

Inputs:

- review 対象の diff、branch、PR、commit、または files。
- 関連する requirements、docs、tests、verification evidence。
- user が指定した review scope、severity bar、specific concerns。

Outputs:

- findings first。severity 順に並べ、可能な限り file と line reference を含める。
- review confidence に影響する open questions または assumptions。
- brief summary と、残る test / manual verification gaps。

## Review Workflow

1. Establish the review target.
   - changed files、diff、PR context、user-provided scope を確認する。
   - `git diff` や PR diff を読む前に、既存の uncommitted changes を user の変更として扱い、unrelated changes を review 対象へ混ぜない。
   - relevant requirements、docs、tests、configuration を確認してから behavior を判断する。

2. Review correctness and architecture fit.
   - requested behavior、requirements、acceptance criteria と実装が一致しているか確認する。
   - `webhook-ingest`、`duckdb-query-api`、Grafana、OpenTofu の責務が混ざっていないか確認する。
   - S3 Parquet path、Hive partition、fixed metrics API、environment variables の contract を壊していないか確認する。

3. Review security and data handling.
   - Webhook signature verification が弱まっていないか確認する。
   - raw payload、secret、AWS credentials、ngrok token、private endpoint が commit / log / error response に漏れないか確認する。
   - SQL injection、unbounded scan、任意 SQL API 化、過剰な raw data exposure を確認する。

4. Review tests and verification.
   - signature verification、payload normalization、Parquet conversion、S3 key construction、DuckDB metrics、parameter validation には focused tests を期待する。
   - infrastructure / Compose / Grafana 変更では `just validate` 相当の確認を期待する。
   - 実行済み checks と未実行 checks が変更リスクに見合っているか確認する。

5. Review documentation impact.
   - public API、schema、S3 key layout、environment variables、local debug、AWS deploy、`just` commands が変わるなら README / requirements / docs の更新要否を確認する。
   - docs-only changes でも、実装・config・scripts と drift していないか確認する。

6. Review maintainability and scope.
   - requested boundary を超える broad refactor、new framework、premature abstraction を flag する。
   - error handling、idempotency、partial failure、observability が Lambda ingest / Query API / deploy workflow の risk に見合っているか確認する。
   - style-only comments は、correctness、maintenance、readability、future defect risk に結びつく場合だけ出す。

## Output Format

findings から始め、severity 順に並べる。可能な限り file と line reference を使う。

以下の形で出力する。

- Findings
- Open Questions or Assumptions
- Brief Summary

blocking findings がない場合は、その旨を明確に述べる。残る test / manual verification gaps があれば併記する。

## Severity Guidance

- Blocker: likely broken core behavior、credential leak、data loss、unsafe S3 / AWS behavior、impossible deployment / validation path。
- High: requirements mismatch、Parquet / S3 contract corruption risk、signature verification regression、missing tests for risky deterministic logic、docs and implementation conflict that misleads operators。
- Medium: maintainability、observability、verification、performance、scope drift の gap により near-term failure が起きうるもの。
- Low: clarity、small cleanup、minor docs/test improvement。重要だが merge 判断を単独では止めにくいもの。

non-actionable comments で review を水増ししない。指摘は具体的な failure mode と最小修正案に結びつける。

## Trigger Evaluation

典型的な trigger phrases:

- "この PR を review して"
- "変更差分を owner 目線で見て"
- "merge 前に bug / risk がないか確認して"
- "architecture に合っているか review して"
- "pre-commit review をして"
