#!/usr/bin/env ruby
# frozen_string_literal: true

require "yaml"

workflow_path = ".github/workflows/ci.yml"

abort "Missing #{workflow_path}" unless File.file?(workflow_path)

workflow = YAML.safe_load(File.read(workflow_path), aliases: true)
raw = File.read(workflow_path)

def assert(condition, message)
  abort message unless condition
end

jobs = workflow.fetch("jobs")

required_jobs = %w[
  gitleaks
  rust-fmt
  webhook-ingest-test
  duckdb-query-api-test
  rust-clippy
  infra-validate
  iac-fmt
  docker-build
]

required_jobs.each do |job|
  assert jobs.key?(job), "Missing CI job: #{job}"
end

assert raw.match?(/^\s*pull_request:\s*$/), "CI must run on pull_request"
assert raw.match?(/^\s*push:\s*$/), "CI must run on push"
assert raw.match?(/^\s*branches:\s*\[\s*main\s*\]\s*$/), "CI push trigger must target main"
assert workflow.dig("concurrency", "cancel-in-progress") == true,
       "CI concurrency must cancel in-progress runs"

uses_entries = raw.scan(/^\s*uses:\s*([^\s#]+)(?:\s+#.*)?$/).flatten
assert uses_entries.any?, "CI workflow must use pinned actions"

uses_entries.each do |entry|
  next if entry.match?(/\A[^@\s]+@[0-9a-f]{40}\z/)

  abort "Action is not pinned to a 40-character SHA: #{entry}"
end

required_commands = [
  "gitleaks/gitleaks-action@e0c47f4f8be36e29cdc102c57e68cb5cbf0e8d1e",
  "GITLEAKS_VERSION: \"8.30.1\"",
  "Verify dummy secret detection",
  "gitleaks_bin=\"/tmp/gitleaks-${GITLEAKS_VERSION}/gitleaks\"",
  "\"$gitleaks_bin\" dir \"$tmpdir\" --redact --no-banner --exit-code 2",
  "rustup component add rustfmt",
  "rustup component add clippy",
  "cargo fmt --all --check",
  "cargo test -p webhook-ingest",
  "cargo test -p duckdb-query-api",
  "cargo clippy --workspace --all-targets -- -D warnings",
  "just validate-ci",
  "tofu fmt -check -recursive infra",
  "docker build --build-arg CARGO_PROFILE=debug --build-arg CARGO_BUILD_JOBS=1 -f webhook-ingest/Dockerfile -t webhook-ingest:ci .",
  "docker build --build-arg CARGO_PROFILE=debug --build-arg CARGO_BUILD_JOBS=1 -f duckdb-query-api/Dockerfile -t duckdb-query-api:ci ."
]

required_commands.each do |command|
  assert raw.include?(command), "Missing CI command: #{command}"
end
