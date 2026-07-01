use lambda_http::{run, service_fn};
use webhook_ingest::AppState;

#[tokio::main]
async fn main() -> Result<(), lambda_runtime::Error> {
    let state = AppState::from_env().await?;
    run(service_fn(move |request| {
        webhook_ingest::handler(request, state.clone())
    }))
    .await
}
