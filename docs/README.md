# docs

`docs/` は、system をどのように構築し運用するかを説明する technical documentation を格納するディレクトリです。

Product requirements は [`../requirements/`](../requirements/)。

## Ownership

- technical design、operations、AWS placement、runbooks はここに置きます。
- product behavior、in/out of scope、demo acceptance criteria は [`../requirements/`](../requirements/) に置きます。

## Documents

| ファイル | 内容 |
| --- | --- |
| [`microcms/`](microcms/) | [microCMS 公式ドキュメント](https://document.microcms.io/) の一部をマークダウンで格納するディレクトリ。本リポジトリの実装・運用で参照するページをローカルに保持する |
| [`local-debug.md`](local-debug.md) | ローカルデバッグ手順。Floci、ngrok、webhook 受信から Parquet 保存、Query API 集計までの確認方法 |
| [`aws-deploy.md`](aws-deploy.md) | AWS デプロイ手順。OpenTofu と `just deploy-all` による初回デプロイと運用の流れ |

プロダクト仕様は [requirements/](../requirements/) を参照する。
coding agent 向けの workflow / guardrails は [`../AGENTS.md`](../AGENTS.md) と [`../.agents/skills/`](../.agents/skills/) を参照する。
