use lambda_http::{run, service_fn};

#[tokio::main]
async fn main() -> Result<(), lambda_runtime::Error> {
    run(service_fn(webhook_ingest::handler)).await
}
