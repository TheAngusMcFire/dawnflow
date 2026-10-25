pub mod connection;
pub mod dispatcher;
pub mod publisher_backend;

/// Header of an empty reply telling the requester that its request failed
/// (handler error, panic, undecodable request, unsendable response).
pub const ERROR_HEADER: &str = "Dawnflow-Error";

use async_nats::Subject;
use bytes::Bytes;
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    handlers::{FromRequestBody, IntoResponse, Response, ResponseErrorScope},
    Req,
};

impl<T: Serialize + Send + Sync + 'static> IntoResponse<NatsMetadata, NatsResponse>
    for Result<T, eyre::Report>
{
    fn into_response(self, metadata: &NatsMetadata) -> Response<NatsResponse> {
        let encoded = self.and_then(|p| rmp_serde::to_vec_named(&p).map_err(eyre::Report::from));
        match encoded {
            Ok(response) => Response {
                error_scope: None,
                success: true,
                report: None,
                payload: Some(NatsResponse {
                    response,
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

/// `Subject` is backed by `bytes::Bytes`, so cloning the metadata is cheap
/// (reference count bump, no allocation).
#[derive(Clone)]
pub struct NatsMetadata {
    pub subject: Subject,
    pub reply: Option<Subject>,
}

/// The payload keeps the `Bytes` received from the NATS client as-is: no copy
/// into an owned buffer, and fan-out clones only bump a reference count.
#[derive(Clone)]
pub struct NatsPayload {
    pub payload: Bytes,
}

pub struct NatsResponse {
    // todo maybe we wanne give that something to detect errors
    pub response: Vec<u8>,
    /// if we do not have a subject, we have no one to return the data to
    pub subject: Option<Subject>,
    pub reply: Option<Subject>,
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
        let resp = match rmp_serde::from_slice(req.payload.as_ref()) {
            Ok(x) => x,
            Err(err) => return Err(err.into()),
        };
        Ok(Req(resp))
    }
}
