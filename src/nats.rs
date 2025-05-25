pub mod dispatcher;
pub mod publisher_backend;

use std::sync::Arc;

use async_nats::Subject;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    handlers::{FromRequestBody, IntoResponse, Response, ResponseErrorScope},
    Req,
};

impl<T: Serialize + Send + Sync + 'static> IntoResponse<NatsMetadata, NatsResponse>
    for Result<T, eyre::Report>
{
    fn into_response(self, metadata: &NatsMetadata) -> Response<NatsResponse> {
        match self {
            Ok(p) => Response {
                error_scope: None,
                success: true,
                report: None,
                payload: Some(NatsResponse {
                    response: rmp_serde::to_vec(&p).unwrap(),
                    subject: Some(metadata.subject.clone()),
                    reply: metadata.reply.clone(),
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

// impl IntoResponse<NatsResponse> for eyre::Report {
//     fn into_response(self) -> Response<NatsResponse> {
//         Response {
//             // todo I really hope this extension is only used in execution scopes...
//             error_scope: Some(ResponseErrorScope::Execution),
//             success: false,
//             report: Some(self),
//             payload: None,
//             handler_name: None,
//         }
//     }
// }

#[derive(Clone)]
pub struct NatsMetadata {
    pub subject: Arc<Subject>,
    pub reply: Option<Arc<Subject>>,
}

#[derive(Clone)]
pub struct NatsPayload {
    pub payload: Arc<Vec<u8>>,
}

pub struct NatsResponse {
    // todo maybe we wanne give that something to detect errors
    pub response: Vec<u8>,
    /// if we do not have a subject, we have no one to return the data to
    pub subject: Option<Arc<Subject>>,
    pub reply: Option<Arc<Subject>>,
}

#[async_trait::async_trait]
impl<T: DeserializeOwned, S> FromRequestBody<S, NatsPayload, NatsMetadata, NatsResponse>
    for Req<T>
{
    type Rejection = eyre::Report;
    async fn from_request(
        req: NatsPayload,
        _meta: &mut NatsMetadata,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        // dbg!(std::any::type_name::<T>());
        // dbg!(&req.payload.len());
        let resp = match rmp_serde::from_slice(req.payload.as_slice()) {
            Ok(x) => x,
            Err(err) => return Err(err.into()),
        };
        Ok(Req(resp))
    }
}
