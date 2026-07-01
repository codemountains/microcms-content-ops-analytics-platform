#!/usr/bin/env ruby
# frozen_string_literal: true

require "yaml"

def assert(condition, message)
  abort message unless condition
end

workflow_paths = Dir[".github/workflows/*.yml"].sort
expected_paths = %w[
  .github/workflows/docker-build.yml
  .github/workflows/infra.yml
  .github/workflows/rust.yml
  .github/workflows/security.yml
]

assert workflow_paths == expected_paths,
       "Workflow files must be split by concern: #{expected_paths.join(', ')}"

raw_by_path = workflow_paths.to_h { |path| [path, File.read(path)] }
workflow_by_path = raw_by_path.transform_values { |raw| YAML.safe_load(raw, aliases: true) }

workflow_by_path.each do |path, workflow|
  raw = raw_by_path.fetch(path)

  assert raw.match?(/^\s*pull_request:\s*$/), "#{path} must run on pull_request"
  assert raw.match?(/^\s*push:\s*$/), "#{path} must run on push"
  assert raw.match?(/^\s*branches:\s*\[\s*main\s*\]\s*$/), "#{path} push trigger must target main"
  assert workflow.dig("concurrency", "cancel-in-progress") == true,
         "#{path} concurrency must cancel in-progress runs"
  assert workflow.fetch("permissions").fetch("contents") == "read",
         "#{path} must use read-only contents permission"
end

combined_raw = raw_by_path.values.join("\n")

uses_entries = combined_raw.scan(/^\s*uses:\s*([^\s#]+)(?:\s+#.*)?$/).flatten
assert uses_entries.any?, "CI workflow must use pinned actions"

uses_entries.each do |entry|
  next if entry.match?(/\A[^@\s]+@[0-9a-f]{40}\z/)

  abort "Action is not pinned to a 40-character SHA: #{entry}"
end

required_jobs_by_path = {
  ".github/workflows/security.yml" => %w[gitleaks],
  ".github/workflows/rust.yml" => %w[fmt webhook-ingest-test-and-clippy duckdb-query-api-test-and-clippy],
  ".github/workflows/infra.yml" => %w[validate fmt],
  ".github/workflows/docker-build.yml" => %w[smoke]
}

required_jobs_by_path.each do |path, required_jobs|
  jobs = workflow_by_path.fetch(path).fetch("jobs")

  required_jobs.each do |job|
    assert jobs.key?(job), "Missing job #{job} in #{path}"
  end
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
  "cargo clippy -p webhook-ingest --all-targets -- -D warnings",
  "cargo clippy -p duckdb-query-api --all-targets -- -D warnings",
  "just validate-ci",
  "tofu fmt -check -recursive infra",
  "docker build --build-arg CARGO_PROFILE=debug --build-arg CARGO_BUILD_JOBS=1 -f webhook-ingest/Dockerfile -t webhook-ingest:ci .",
  "docker build --build-arg CARGO_PROFILE=debug --build-arg CARGO_BUILD_JOBS=1 -f duckdb-query-api/Dockerfile -t duckdb-query-api:ci ."
]

required_commands.each do |command|
  assert combined_raw.include?(command), "Missing CI command: #{command}"
end
