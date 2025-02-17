use handlers::{IntoResponse, Response, ResponseErrorScope};

pub mod handlers;
pub mod in_memory;
pub mod in_memory_publisher_backend;
pub mod publisher;
pub mod registry;

#[cfg(all(feature = "in_memory", feature = "nats"))]
compile_error!("features `feature/in_memory` and `feature/nats` are mutually exclusive");

impl<R: Send + Sync + 'static> IntoResponse<R> for eyre::Report {
    fn into_response(self) -> Response<R> {
        Response {
            error_scope: Some(ResponseErrorScope::Preparation),
            success: false,
            report: Some(self),
            payload: None,
        }
    }
}

pub struct Req<T>(T);
