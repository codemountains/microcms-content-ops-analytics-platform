use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    http::{HeaderMap, HeaderValue, Method, Request, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use rmcp::{
    ErrorData, ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    model::{CallToolResult, ServerCapabilities, ServerInfo},
    schemars, tool, tool_handler, tool_router,
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    },
};
use serde::{Deserialize, Serialize};

use super::{
    AppState, PublishDurationUnit, validate_limit, validate_publish_duration_unit,
    validate_time_range,
};
use crate::ApiError;
use crate::config::McpConfig;
use crate::query::{
    query_api_activity_rows, query_average_draft_to_publish_rows,
    query_average_time_to_publish_rows, query_calendar_heatmap_rows,
    query_publish_action_summary_rows, query_publish_action_trend_rows,
    query_top_updated_contents_rows,
};

#[derive(Clone)]
struct McpAccess {
    bearer_token: String,
    allowed_origins: Vec<String>,
}

#[derive(Clone)]
pub(crate) struct MetricsMcpServer {
    state: AppState,
    tool_router: ToolRouter<Self>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct TimeRangeParams {
    from: Option<i64>,
    to: Option<i64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct LimitedParams {
    from: Option<i64>,
    to: Option<i64>,
    limit: Option<u32>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct PublishDurationParams {
    from: Option<i64>,
    to: Option<i64>,
    unit: Option<String>,
}

impl MetricsMcpServer {
    fn new(state: AppState) -> Self {
        Self {
            state,
            tool_router: Self::tool_router(),
        }
    }
}

#[tool_router]
impl MetricsMcpServer {
    #[tool(
        description = "Return daily event counts for the calendar heatmap metric.",
        annotations(read_only_hint = true)
    )]
    async fn calendar_heatmap(
        &self,
        Parameters(params): Parameters<TimeRangeParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let (from_ms, to_ms) = validate_time_range(params.from, params.to).map_err(to_mcp_error)?;
        let rows = self
            .state
            .duckdb
            .query(move |connection, events_sql| {
                query_calendar_heatmap_rows(connection, events_sql, from_ms, to_ms)
            })
            .await
            .map_err(to_mcp_error)?;

        structured(rows)
    }

    #[tool(
        description = "Return publish count and published-state rate summary.",
        annotations(read_only_hint = true)
    )]
    async fn publish_action_summary(
        &self,
        Parameters(params): Parameters<TimeRangeParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let (from_ms, to_ms) = validate_time_range(params.from, params.to).map_err(to_mcp_error)?;
        let rows = self
            .state
            .duckdb
            .query(move |connection, events_sql| {
                query_publish_action_summary_rows(connection, events_sql, from_ms, to_ms)
            })
            .await
            .map_err(to_mcp_error)?;

        structured(rows)
    }

    #[tool(
        description = "Return daily publish action trend rows.",
        annotations(read_only_hint = true)
    )]
    async fn publish_action_trend(
        &self,
        Parameters(params): Parameters<TimeRangeParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let (from_ms, to_ms) = validate_time_range(params.from, params.to).map_err(to_mcp_error)?;
        let rows = self
            .state
            .duckdb
            .query(move |connection, events_sql| {
                query_publish_action_trend_rows(connection, events_sql, from_ms, to_ms)
            })
            .await
            .map_err(to_mcp_error)?;

        structured(rows)
    }

    #[tool(
        description = "Return event-kind activity counts grouped by API.",
        annotations(read_only_hint = true)
    )]
    async fn api_activity(
        &self,
        Parameters(params): Parameters<TimeRangeParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let (from_ms, to_ms) = validate_time_range(params.from, params.to).map_err(to_mcp_error)?;
        let rows = self
            .state
            .duckdb
            .query(move |connection, events_sql| {
                query_api_activity_rows(connection, events_sql, from_ms, to_ms)
            })
            .await
            .map_err(to_mcp_error)?;

        structured(rows)
    }

    #[tool(
        description = "Return the most frequently updated contents.",
        annotations(read_only_hint = true)
    )]
    async fn top_updated_contents(
        &self,
        Parameters(params): Parameters<LimitedParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let (from_ms, to_ms) = validate_time_range(params.from, params.to).map_err(to_mcp_error)?;
        let limit = validate_limit(params.limit).map_err(to_mcp_error)?;
        let rows = self
            .state
            .duckdb
            .query(move |connection, events_sql| {
                query_top_updated_contents_rows(connection, events_sql, from_ms, to_ms, limit)
            })
            .await
            .map_err(to_mcp_error)?;

        structured(rows)
    }

    #[tool(
        description = "Return average time from content creation to publish by API.",
        annotations(read_only_hint = true)
    )]
    async fn average_time_to_publish_by_api(
        &self,
        Parameters(params): Parameters<PublishDurationParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let (from_ms, to_ms) = validate_time_range(params.from, params.to).map_err(to_mcp_error)?;
        let unit = publish_duration_unit(params.unit.as_deref())?;
        let rows = self
            .state
            .duckdb
            .query(move |connection, events_sql| {
                query_average_time_to_publish_rows(connection, events_sql, from_ms, to_ms, unit)
            })
            .await
            .map_err(to_mcp_error)?;

        structured(rows)
    }

    #[tool(
        description = "Return average time from draft creation to first publish by API.",
        annotations(read_only_hint = true)
    )]
    async fn average_draft_to_publish_by_api(
        &self,
        Parameters(params): Parameters<PublishDurationParams>,
    ) -> Result<CallToolResult, ErrorData> {
        let (from_ms, to_ms) = validate_time_range(params.from, params.to).map_err(to_mcp_error)?;
        let unit = publish_duration_unit(params.unit.as_deref())?;
        let rows = self
            .state
            .duckdb
            .query(move |connection, events_sql| {
                query_average_draft_to_publish_rows(connection, events_sql, from_ms, to_ms, unit)
            })
            .await
            .map_err(to_mcp_error)?;

        structured(rows)
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for MetricsMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
    }
}

pub(crate) fn router(state: AppState, config: McpConfig) -> Router {
    let access = McpAccess {
        bearer_token: config.bearer_token,
        allowed_origins: config.allowed_origins.clone(),
    };
    let service_state = state.clone();
    let service: StreamableHttpService<MetricsMcpServer, LocalSessionManager> =
        StreamableHttpService::new(
            move || Ok(MetricsMcpServer::new(service_state.clone())),
            Arc::new(LocalSessionManager::default()),
            StreamableHttpServerConfig::default()
                .with_allowed_origins(config.allowed_origins.iter().map(String::as_str)),
        );

    Router::new()
        .nest_service("/mcp", service)
        .layer(middleware::from_fn_with_state(access, require_mcp_access))
}

async fn require_mcp_access(
    axum::extract::State(access): axum::extract::State<McpAccess>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let Some(origin) = allowed_origin(&request, &access) else {
        return StatusCode::FORBIDDEN.into_response();
    };

    if request.method() == Method::OPTIONS {
        return cors_response(StatusCode::NO_CONTENT, &origin);
    }

    let bearer_token = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "));
    if bearer_token != Some(access.bearer_token.as_str()) {
        return cors_response(StatusCode::UNAUTHORIZED, &origin);
    }

    let mut response = next.run(request).await;
    apply_cors_headers(response.headers_mut(), &origin);
    response
}

fn allowed_origin(request: &Request<Body>, access: &McpAccess) -> Option<String> {
    let origin = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())?;
    access
        .allowed_origins
        .iter()
        .any(|allowed| allowed == origin)
        .then(|| origin.to_owned())
}

fn cors_response(status: StatusCode, origin: &str) -> Response {
    let mut response = status.into_response();
    apply_cors_headers(response.headers_mut(), origin);
    response
}

fn apply_cors_headers(headers: &mut HeaderMap, origin: &str) {
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_str(origin).expect("origin should be a valid header value"),
    );
    headers.insert(header::VARY, HeaderValue::from_static("Origin"));
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_METHODS,
        HeaderValue::from_static("GET, POST, DELETE, OPTIONS"),
    );
    headers.insert(
        header::ACCESS_CONTROL_ALLOW_HEADERS,
        HeaderValue::from_static(
            "authorization, content-type, accept, mcp-protocol-version, mcp-session-id, last-event-id",
        ),
    );
    headers.insert(
        header::ACCESS_CONTROL_EXPOSE_HEADERS,
        HeaderValue::from_static("mcp-session-id"),
    );
    headers.insert(
        header::ACCESS_CONTROL_MAX_AGE,
        HeaderValue::from_static("600"),
    );
}

fn publish_duration_unit(unit: Option<&str>) -> Result<PublishDurationUnit, ErrorData> {
    validate_publish_duration_unit(unit).map_err(to_mcp_error)
}

fn structured<T: Serialize>(value: T) -> Result<CallToolResult, ErrorData> {
    serde_json::to_value(value)
        .map(CallToolResult::structured)
        .map_err(|error| ErrorData::internal_error(error.to_string(), None))
}

fn to_mcp_error(error: ApiError) -> ErrorData {
    match error {
        ApiError::InvalidQuery(_) => ErrorData::invalid_params(error.to_string(), None),
        ApiError::MissingEnv(_) | ApiError::DuckDb(_) => {
            ErrorData::internal_error(error.to_string(), None)
        }
    }
}
