use handlers::{IntoResponse, Response, ResponseErrorScope};

pub mod handlers;
pub mod in_memory;
pub mod in_memory_publisher_backend;
pub mod publisher;
pub mod registry;

#[cfg(feature = "nats")]
pub mod nats;

#[cfg(all(feature = "in_memory", feature = "nats"))]
compile_error!("features `feature/in_memory` and `feature/nats` are mutually exclusive");

impl<R: Send + Sync + 'static, M> IntoResponse<M, R> for eyre::Report {
    fn into_response(self, metadata: &M) -> Response<R> {
        Response {
            error_scope: Some(ResponseErrorScope::Preparation),
            success: false,
            report: Some(self),
            payload: None,
            handler_name: None,
        }
    }
}

pub struct Req<T>(pub T);
