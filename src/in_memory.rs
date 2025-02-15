use std::any::Any;

use crate::handlers::{IntoResponse, Response, ResponseErrorScope};

// impl IntoResponsePayload<InMemoryResponse> for () {
//     fn into_response_payload(self) -> InMemoryResponse {
//         InMemoryResponse { response: None }
//     }
// }

impl<T: Send + Sync + 'static> IntoResponse<InMemoryResponse> for Result<T, eyre::Report> {
    fn into_response(self) -> Response<InMemoryResponse> {
        match self {
            Ok(p) => Response {
                error_scope: None,
                success: true,
                report: None,
                payload: Some(InMemoryResponse {
                    response: Some(Box::new(p)),
                }),
            },
            Err(x) => Response {
                // todo I really hope this extension is only used in execution scopes...
                error_scope: Some(ResponseErrorScope::Execution),
                success: false,
                report: Some(x),
                payload: None,
            },
        }
    }
}

pub struct InMemoryMetadata {}

pub struct InMemoryPayload {
    pub payload: Box<dyn Any + Send + Sync + 'static>,
}

pub struct InMemoryResponse {
    pub response: Option<Box<dyn Any + Send + Sync + 'static>>,
}
