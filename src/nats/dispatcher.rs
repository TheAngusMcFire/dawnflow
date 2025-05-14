use std::{collections::HashMap, sync::Arc};

use async_nats::Client;
use eyre::bail;
use tokio::task::{JoinError, JoinSet};

use crate::{
    handlers::Response,
    registry::{HandlerArc, HandlerRegistry},
};

use futures::stream::StreamExt;

use super::{NatsMetadata, NatsPayload, NatsResponse};

pub struct NatsDipatcher<S: Clone + Sync + Send + 'static> {
    state: S,
    client: Arc<Client>,
}

impl<S: Clone + Sync + Send + 'static> NatsDipatcher<S> {
    pub async fn new(
        state: S,
        reg: HandlerRegistry<NatsPayload, NatsMetadata, S, NatsResponse>,
        nats_url: &str,
    ) -> eyre::Result<Self> {
        let client = match async_nats::connect(nats_url).await {
            Ok(x) => x,
            Err(err) => bail!("{}", err),
        };

        Ok(Self {
            state,
            client: Arc::new(client),
        })
    }

    pub fn handle_join_result(
        res: Result<Response<NatsResponse>, JoinError>,
        // handler_name: &str,
        // message_name: &str,
    ) {
        // TODO do something with the error result e.g. publish to a error handler
        match res {
            Ok(Response {
                error_scope: _,
                success: false,
                report: Some(report),
                payload: None,
                handler_name,
            }) => {
                let handler_name = handler_name.unwrap_or("no-handler-name");
                tracing::error!("Error in handler: {handler_name}\nMessage:\n    {report:?}");
            }
            Ok(Response {
                error_scope: _,
                success: true,
                report: _,
                payload: _,
                handler_name: _,
            }) => {
                tracing::debug!("Request handled successfully");
            }
            Err(x) if x.is_cancelled() => {
                tracing::error!("Request was cancelled")
            }
            Err(x) if x.is_panic() => {
                tracing::error!("Panic processing of request")
            }
            _ => {
                tracing::error!("Unexpected Error during processing of request")
            }
        };
    }

    pub async fn start_dispatcher(
        self,
        consumers: HashMap<String, HandlerArc<NatsPayload, NatsMetadata, S, NatsResponse>>,
        subscribers: HashMap<String, Vec<HandlerArc<NatsPayload, NatsMetadata, S, NatsResponse>>>,
        handers: HashMap<String, Vec<HandlerArc<NatsPayload, NatsMetadata, S, NatsResponse>>>,
        mut join_set: JoinSet<()>,
    ) -> eyre::Result<JoinSet<()>> {
        for (name, handler) in consumers {
            let mut sub = self.client.subscribe(format!("consumer.{}", name)).await?;
            let state = self.state.clone();
            join_set.spawn(async move {
                loop {
                    let state = state.clone();
                    let next = sub.next().await.unwrap();
                    let payload = NatsPayload {
                        payload: next.payload.into(),
                    };
                    let resp = handler
                        .call(
                            crate::handlers::HandlerRequest {
                                metadata: NatsMetadata {},
                                payload,
                            },
                            state,
                        )
                        .await;
                }
            });
        }
        todo!()
    }
}
