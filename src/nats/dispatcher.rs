use std::{sync::Arc, time::Duration};

use async_nats::Message;
use eyre::bail;
use tokio::{
    select,
    task::{JoinError, JoinSet},
};

use crate::{handlers::Response, registry::HandlerRegistry};

use futures::stream::StreamExt;

use super::{NatsMetadata, NatsPayload, NatsResponse};

pub struct NatsDipatcher {}

pub enum MessageOrResponse {
    Message(Message),
    Response(Result<Response<NatsResponse>, JoinError>),
}

impl NatsDipatcher {
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
    pub async fn start_dispatcher<S: Clone + Sync + Send + 'static>(
        nats_url: &str,
        state: S,
        reg: HandlerRegistry<NatsPayload, NatsMetadata, S, NatsResponse>,
        mut join_set: JoinSet<()>,
    ) -> eyre::Result<JoinSet<()>> {
        let client = Arc::new(match async_nats::connect(nats_url).await {
            Ok(x) => x,
            Err(err) => bail!("{}", err),
        });
        let parallel_size = 10;

        for (name, handler) in reg.consumers {
            let mut sub = client.subscribe(format!("consumer.{}", name)).await?;
            let state = state.clone();
            join_set.spawn(async move {
                let guard = elegant_departure::get_shutdown_guard();
                let mut join_set = JoinSet::<Response<NatsResponse>>::new();
                loop {
                    while join_set.len() > parallel_size {
                        while let Some(x) = join_set.try_join_next() {
                            let _ = Self::handle_join_result(x);
                        }
                        tracing::warn!("Too many consumer jobs, waiting for consumer completions");
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    let next = if !join_set.is_empty() {
                        select! {
                            Some(r) = sub.next() => r,
                            jh = join_set.join_next() => {
                                let _ = Self::handle_join_result(jh.expect("can not happen here"));
                                continue;
                            },
                            _ = guard.wait() => {
                                while let Some(r) = join_set.join_next().await {
                                    let _ = Self::handle_join_result(r);
                                }
                                break;
                            },
                            else => {
                                break;
                            }
                        }
                    } else {
                        select! {
                            Some(r) = sub.next() => r,
                            _ = guard.wait() => {
                                while let Some(r) = join_set.join_next().await {
                                    let _ = Self::handle_join_result(r);
                                }
                                break;
                            },
                            else => {
                                break;
                            }
                        }
                    };

                    let payload = NatsPayload {
                        payload: Arc::new(next.payload.into()),
                    };

                    let metadata = NatsMetadata {
                        subject: Arc::new(next.subject),
                        reply: None,
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
            let mut sub = client.subscribe(format!("subscriber.{}", name)).await?;
            let state = state.clone();
            join_set.spawn(async move {
                let guard = elegant_departure::get_shutdown_guard();
                let mut join_set = JoinSet::<Response<NatsResponse>>::new();
                loop {
                    while join_set.len() > parallel_size {
                        while let Some(x) = join_set.try_join_next() {
                            let _ = Self::handle_join_result(x);
                        }
                        tracing::warn!(
                            "Too many subscriber jobs, waiting for subscriber completions"
                        );
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }

                    let next = if !join_set.is_empty() {
                        select! {
                            Some(r) = sub.next() => r,
                            jh = join_set.join_next() => {
                                let _ = Self::handle_join_result(jh.expect("can not happen here"));
                                continue;
                            },
                            _ = guard.wait() => {
                                while let Some(r) = join_set.join_next().await {
                                    let _ = Self::handle_join_result(r);
                                }
                                break;
                            },
                            else => {
                                break;
                            }
                        }
                    } else {
                        select! {
                            Some(r) = sub.next() => r,
                            _ = guard.wait() => {
                                while let Some(r) = join_set.join_next().await {
                                    let _ = Self::handle_join_result(r);
                                }
                                break;
                            },
                            else => {
                                break;
                            }
                        }
                    };

                    let payload = NatsPayload {
                        payload: Arc::new(next.payload.into()),
                    };

                    let metadata = NatsMetadata {
                        subject: Arc::new(next.subject),
                        reply: None,
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
            let mut sub = client.subscribe(format!("request.{}", name)).await?;
            let state = state.clone();
            let client = client.clone();
            join_set.spawn(async move {
                let guard = elegant_departure::get_shutdown_guard();
                let mut shutdown = false;
                let mut join_set = JoinSet::<Response<NatsResponse>>::new();
                loop {
                    while join_set.len() > parallel_size {
                    while let Some(x) = if shutdown {join_set.join_next().await} else {join_set.try_join_next() }{
                        let res = match Self::handle_join_result(x) {
                            Ok(Response {
                                payload:
                                    Some(NatsResponse {
                                        response,
                                        reply: Some(reply),
                                        ..
                                    }),
                                ..
                            }) => client.publish(reply.as_ref().clone(), response.into()).await,
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
                        tracing::warn!("Too many request jobs, waiting for request completions");
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }

                    if shutdown {
                        break;
                    }

                    let next = if !join_set.is_empty() {
                        select! {
                            Some(r) = sub.next() => MessageOrResponse::Message(r),
                            jh = join_set.join_next() => {
                                MessageOrResponse::Response(Self::handle_join_result(jh.expect("can not happen here")))
                            },
                            _ = guard.wait() => {
                                shutdown = true;
                                continue;
                            },
                            else => {
                                break;
                            }
                        }
                    } else {
                        select! {
                            Some(r) = sub.next() => MessageOrResponse::Message(r),
                            _ = guard.wait() => {
                                shutdown = true;
                                continue;
                            },
                            else => {
                                break;
                            }
                        }
                    };

                    match next {
                        MessageOrResponse::Message(next) => {
                            let payload = NatsPayload {
                                payload: Arc::new(next.payload.into()),
                            };

                            let metadata = NatsMetadata {
                                subject: Arc::new(next.subject),
                                reply: next.reply.map(Arc::new),
                            };

                            let state = state.clone();
                            let handler = handler.clone();

                            join_set.spawn(async move {
                                handler
                                    .call(crate::handlers::HandlerRequest { metadata, payload }, state)
                                    .await
                            });
                        }
                        MessageOrResponse::Response(response) => {
                            let res = match response {
                                Ok(Response {
                                    payload:
                                        Some(NatsResponse {
                                            response,
                                            reply: Some(reply),
                                            ..
                                        }),
                                    ..
                                }) => client.publish(reply.as_ref().clone(), response.into()).await,
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
                        },
                    }

                }
            });
        }
        Ok(join_set)
    }
}
