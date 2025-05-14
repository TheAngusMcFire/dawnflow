use std::any::Any;

use crate::{
    handlers::{FromRequestBody, IntoResponse, Response, ResponseErrorScope},
    Req,
};

impl<T: Send + Sync + 'static> IntoResponse<InMemoryMetadata, InMemoryResponse>
    for Result<T, eyre::Report>
{
    fn into_response(self, metadata: &InMemoryMetadata) -> Response<InMemoryResponse> {
        match self {
            Ok(p) => Response {
                error_scope: None,
                success: true,
                report: None,
                payload: Some(InMemoryResponse {
                    response: Some(Box::new(p)),
                }),
                handler_name: None,
            },
            Err(x) => Response {
                // todo I really hope this extension is only used in execution scopes...
                error_scope: Some(ResponseErrorScope::Execution),
                success: false,
                report: Some(x),
                payload: None,
                handler_name: None,
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

#[async_trait::async_trait]
impl<T: Any, S> FromRequestBody<S, InMemoryPayload, InMemoryMetadata, InMemoryResponse> for Req<T> {
    type Rejection = Result<InMemoryResponse, eyre::Report>;
    async fn from_request(
        req: InMemoryPayload,
        _meta: &mut InMemoryMetadata,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        match req.payload.downcast::<T>() {
            Ok(x) => Ok(Req(*x)),
            Err(_) => Err(Err(eyre::eyre!(
                "Unable to downcast payload to the target type"
            ))),
        }
    }
}
