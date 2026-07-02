use std::sync::Arc;

use lambda_http::{run, service_fn};
use webhook_ingest::AppState;

#[tokio::main]
async fn main() -> Result<(), lambda_runtime::Error> {
    let state = AppState::from_env().await.map_err(Arc::new);
    run(service_fn(move |request| {
        webhook_ingest::handler_from_init(request, state.clone())
    }))
    .await
}
