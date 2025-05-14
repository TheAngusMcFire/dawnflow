use std::sync::Arc;

use async_nats::Client;
use eyre::bail;
use tokio::task::{JoinError, JoinSet};

use crate::{handlers::Response, registry::HandlerRegistry};

use futures::stream::StreamExt;

use super::{NatsMetadata, NatsPayload, NatsResponse};

pub struct NatsDipatcher<S: Clone + Sync + Send + 'static> {
    state: S,
    client: Arc<Client>,
}

impl<S: Clone + Sync + Send + 'static> NatsDipatcher<S> {
    pub async fn new(state: S, nats_url: &str) -> eyre::Result<Self> {
        todo!("move this to the start dispatcher funciton");
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
    ) -> Result<Response<NatsResponse>, JoinError> {
        // TODO do something with the error result e.g. publish to a error handler
        match &res {
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

        res
    }

    // todo implement include guard checks, for clean shutdown
    pub async fn start_dispatcher(
        self,
        reg: HandlerRegistry<NatsPayload, NatsMetadata, S, NatsResponse>,
        mut join_set: JoinSet<()>,
    ) -> eyre::Result<JoinSet<()>> {
        for (name, handler) in reg.consumers {
            let mut sub = self.client.subscribe(format!("consumer.{}", name)).await?;
            let state = self.state.clone();
            join_set.spawn(async move {
                let mut join_set = JoinSet::<Response<NatsResponse>>::new();
                loop {
                    while let Some(x) = join_set.try_join_next() {
                        let _ = Self::handle_join_result(x);
                    }

                    let next = match sub.next().await {
                        Some(x) => x,
                        None => {
                            todo!("this is probably a shutdown?");
                        }
                    };

                    let payload = NatsPayload {
                        payload: Arc::new(next.payload.into()),
                    };

                    let metadata = NatsMetadata {
                        subject: Arc::new(next.subject),
                    };

                    let state = state.clone();
                    let handler = handler.clone();

                    join_set.spawn(async move {
                        handler
                            .call(crate::handlers::HandlerRequest { metadata, payload }, state)
                            .await
                    });
                }
            });
        }
        for (name, handler) in reg.subscribers {
            let mut sub = self
                .client
                .subscribe(format!("subscriber.{}", name))
                .await?;
            let state = self.state.clone();
            join_set.spawn(async move {
                let mut join_set = JoinSet::<Response<NatsResponse>>::new();
                loop {
                    while let Some(x) = join_set.try_join_next() {
                        let _ = Self::handle_join_result(x);
                    }

                    let next = match sub.next().await {
                        Some(x) => x,
                        None => {
                            todo!("this is probably a shutdown?");
                        }
                    };

                    let payload = NatsPayload {
                        payload: Arc::new(next.payload.into()),
                    };

                    let metadata = NatsMetadata {
                        subject: Arc::new(next.subject),
                    };

                    for handler in &handler {
                        let state = state.clone();
                        let handler = handler.clone();
                        let payload = payload.clone();
                        let metadata = metadata.clone();
                        join_set.spawn(async move {
                            handler
                                .call(crate::handlers::HandlerRequest { metadata, payload }, state)
                                .await
                        });
                    }
                }
            });
        }

        for (name, handler) in reg.handlers {
            let mut sub = self.client.subscribe(format!("request.{}", name)).await?;
            let state = self.state.clone();
            let client = self.client.clone();
            join_set.spawn(async move {
                let mut join_set = JoinSet::<Response<NatsResponse>>::new();
                loop {
                    while let Some(x) = join_set.try_join_next() {
                        let res = match Self::handle_join_result(x) {
                            Ok(Response {
                                payload:
                                    Some(NatsResponse {
                                        response,
                                        subject: Some(sub),
                                    }),
                                ..
                            }) => client.publish(sub.as_ref().clone(), response.into()).await,
                            _ => {
                                continue;
                            }
                        };
                        match &res {
                            Err(x) => {
                                tracing::error!("while publishing nats response: {x:?}");
                            }
                            _ => continue,
                        }
                    }

                    let next = match sub.next().await {
                        Some(x) => x,
                        None => {
                            todo!("this is probably a shutdown?");
                        }
                    };

                    let payload = NatsPayload {
                        payload: Arc::new(next.payload.into()),
                    };

                    let metadata = NatsMetadata {
                        subject: Arc::new(next.subject),
                    };

                    let state = state.clone();
                    let handler = handler.clone();

                    join_set.spawn(async move {
                        handler
                            .call(crate::handlers::HandlerRequest { metadata, payload }, state)
                            .await
                    });
                }
            });
        }
        todo!()
    }
}
