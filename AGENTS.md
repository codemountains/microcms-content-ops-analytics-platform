# Agent Development Guide

このファイルは、`microcms-content-ops-analytics-platform` で作業する coding agent 向けの入口です。
思考、説明、plan、final response は日本語を基本にしてください。技術用語、コード、コマンド、API 名、ブランチ名、PR タイトルでは英語を使って構いません。

## Read First

作業内容に応じて、詳細は以下を参照してください。

- Product scope、API、schema、acceptance criteria: `requirements/microcms-content-ops-analytics.spec.md`
- Project overview、環境変数、主要 `just` commands: `README.md`
- Local debug: `docs/local-debug.md`
- AWS deploy: `docs/aws-deploy.md`
- Repo-local agent guardrails: `.agents/skills/references/microcms-content-ops-analytics-guardrails.md`
- Command definitions: `justfile`

`README.md`、`requirements/`、`docs/`、`.agents/skills/`、実装が矛盾する場合は、勝手にどちらかへ寄せず、差分を明示してから方針を決めてください。

## Agent Rules

- 作業前に `git status --short --branch` を確認し、既存の未コミット変更を壊さない。
- user が作成した可能性のある変更は revert しない。必要なら、その変更を前提に作業する。
- 変更範囲を task に必要な範囲へ絞り、無関係な refactor、format churn、metadata 変更を混ぜない。
- `deploy`、`destroy`、ECR push、S3 bucket deletion、OpenTofu state 変更など外部環境や state に影響する操作は、user の明示指示なしに実行しない。
- 変更後は `.agents/skills/references/microcms-content-ops-analytics-guardrails.md` の verification guidance に従い、実行できなかった検証と残リスクを final response または PR description に明記する。

## Repo Skills

リポジトリ固有の workflow は `.agents/skills/`、Codex subagent は `.codex/agents/` に定義しています。

| Skill | Codex subagent | 用途 |
| --- | --- | --- |
| `tdd-implement` | `tdd-implementer` | code / config / infra 変更の test-first implementation workflow |
| `code-review` | `code-reviewer` | correctness、security、data contract、verification、operational risk review |
| `docs-staleness-review` | `docs-staleness-reviewer` | README / requirements / docs / implementation / command guidance の drift review |
| `grill-me` | なし | plan や design の意思決定を質問で詰める汎用 skill |

`implement`、`fix`、`refactor` は code behavior を変える場合に `$tdd-implement` を既定とします。
`requirements/`、`docs/`、`AGENTS.md`、`README` など prose-only の documentation edits は `$tdd-implement` の対象外です。

## Documentation Ownership

- Product behavior、scope、API、schema、acceptance criteria は `requirements/microcms-content-ops-analytics.spec.md` を更新する。
- Local debug 手順や Floci/ngrok の扱いが変わる場合は `docs/local-debug.md` を更新する。
- AWS deploy 手順、OpenTofu variable、resource 構成が変わる場合は `docs/aws-deploy.md` を更新する。
- Agent workflow、review workflow、verification guardrails は `.agents/skills/` を更新する。
- Local command が変わる場合は `README.md` と `justfile` の整合を取る。
