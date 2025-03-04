use std::{any::Any, collections::HashMap};

use tokio::{
    select,
    sync::mpsc,
    task::{JoinError, JoinSet},
};

use crate::{
    handlers::Response,
    in_memory::{InMemoryMetadata, InMemoryPayload, InMemoryResponse},
    publisher::PublisherError,
    registry::{HandlerArc, HandlerRegistry},
};

pub trait AnyCloneFactory: Send + Sync + 'static {
    fn get_any_clone(&self) -> Box<dyn Any + Send + Sync + 'static>;
}

pub(crate) struct SubscriberCloneFactory<T> {
    pub factory: fn(&SubscriberCloneFactory<T>) -> Box<dyn Any + Send + Sync + 'static>,
    pub obj: T,
}

impl<T: Send + Sync + 'static> AnyCloneFactory for SubscriberCloneFactory<T> {
    fn get_any_clone(&self) -> Box<dyn Any + Send + Sync + 'static> {
        (self.factory)(self)
    }
}

#[async_trait::async_trait]
pub trait InMemoryPublisherBackend: Send + Sync {
    async fn pub_sub(
        &self,
        name: &str,
        msg: Box<dyn AnyCloneFactory>,
    ) -> Result<(), crate::publisher::PublisherError>;

    async fn pub_cons(
        &self,
        name: &str,
        msg: Box<dyn std::any::Any + Send + Sync + 'static>,
    ) -> Result<(), PublisherError>;

    async fn pub_req(
        &self,
        name: &str,
        msg: Box<dyn std::any::Any + Send + Sync + 'static>,
    ) -> Result<Box<dyn std::any::Any + Send + Sync + 'static>, PublisherError>;
}

pub struct DefaultInMemoryPublisherBackend<S: Clone + Sync + Send + 'static> {
    state: S,
    handlers: HashMap<String, HandlerArc<InMemoryPayload, InMemoryMetadata, S, InMemoryResponse>>,
    consumer_channel: tokio::sync::mpsc::Sender<(String, InMemoryPayload)>,
    subscriber_channel: tokio::sync::mpsc::Sender<(String, Box<dyn AnyCloneFactory>)>,
}

impl<S: Clone + Sync + Send + 'static> DefaultInMemoryPublisherBackend<S> {
    pub fn handle_join_result(
        res: Result<Response<InMemoryResponse>, JoinError>,
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
        state: S,
        consumers: HashMap<
            String,
            HandlerArc<InMemoryPayload, InMemoryMetadata, S, InMemoryResponse>,
        >,
        subscribers: HashMap<
            String,
            Vec<HandlerArc<InMemoryPayload, InMemoryMetadata, S, InMemoryResponse>>,
        >,
        mut sub_rx: tokio::sync::mpsc::Receiver<(String, Box<dyn AnyCloneFactory>)>,
        mut cons_rx: tokio::sync::mpsc::Receiver<(String, InMemoryPayload)>,
        mut join_set: JoinSet<()>,
    ) -> JoinSet<()> {
        // todo handle conjestions
        let con_state = state.clone();
        join_set.spawn(async move {
            let guard = elegant_departure::get_shutdown_guard();
            let mut join_set = JoinSet::<Response<InMemoryResponse>>::new();

            loop {
                while let Some(x) = join_set.try_join_next() {
                    DefaultInMemoryPublisherBackend::<S>::handle_join_result(x);
                }
                let (name, payload) = if !join_set.is_empty() {
                    select! {
                        Some(r) = cons_rx.recv() => r,
                        jh = join_set.join_next() => {
                            DefaultInMemoryPublisherBackend::<S>::handle_join_result(jh.expect("can not happen here"));
                            continue;
                        },
                        _ = guard.wait() => {
                            while let Some(r) = join_set.join_next().await {
                                DefaultInMemoryPublisherBackend::<S>::handle_join_result(r);
                            }
                            break;
                        },
                        else => {
                            break;
                        }
                    }
                } else {
                    select! {
                        Some(r) = cons_rx.recv() => r,
                        _ = guard.wait() => {
                            while let Some(r) = join_set.join_next().await {
                                DefaultInMemoryPublisherBackend::<S>::handle_join_result(r);
                            }
                            break;
                        },
                        else => {
                            break;
                        }
                    }
                };

                let Some(call) = consumers.get(&name) else {
                    // TODO do something with the error result e.g. publish to a error handler
                    tracing::error!("Consumer handler: {name} not found, ignore incoming request");
                    continue;
                };

                let state = con_state.clone();
                let call = call.clone();
                join_set.spawn(async move {
                    call.call(
                        crate::handlers::HandlerRequest {
                            metadata: InMemoryMetadata {},
                            payload,
                        },
                        state,
                    )
                    .await
                });
            }
            tracing::info!("shutdown the consumer dispatcher");
        });

        let sub_state = state.clone();
        join_set.spawn(async move {
            let guard = elegant_departure::get_shutdown_guard();
            let mut join_set = JoinSet::<Response<InMemoryResponse>>::new();

            loop {
                while let Some(x) = join_set.try_join_next() {
                    DefaultInMemoryPublisherBackend::<S>::handle_join_result(x);
                }
                let (name, payload) = if !join_set.is_empty() {
                    select! {
                        Some(r) = sub_rx.recv() => r,
                        jh = join_set.join_next() => {
                            DefaultInMemoryPublisherBackend::<S>::handle_join_result(jh.expect("can not happen here"));
                            continue;
                        },
                        _ = guard.wait() => {
                            while let Some(r) = join_set.join_next().await {
                                DefaultInMemoryPublisherBackend::<S>::handle_join_result(r);
                            }
                            break;
                        },
                        else => {
                            break;
                        }
                    }
                } else {
                    select! {
                        Some(r) = sub_rx.recv() => r,
                        _ = guard.wait() => {
                            while let Some(r) = join_set.join_next().await {
                                DefaultInMemoryPublisherBackend::<S>::handle_join_result(r);
                            }
                            break;
                        },
                        else => {
                            break;
                        }
                    }
                };

                let Some(calls) = subscribers.get(&name) else {
                    // TODO do something with the error result e.g. publish to a error handler
                    tracing::error!("Subscriber handler: {name} not found, ignore incoming request");
                    continue;
                };

                for call in calls {
                    let state = sub_state.clone();
                    let payload = payload.get_any_clone();
                    let call = call.clone();
                    join_set.spawn(async move {
                        call.call(
                            crate::handlers::HandlerRequest {
                                metadata: InMemoryMetadata {},
                                payload: InMemoryPayload { payload },
                            },
                            state,
                        )
                        .await
                    });
                }
            }

            tracing::info!("shutdown the subscriber dispatcher");
        });

        join_set
    }

    pub async fn new(
        state: S,
        reg: HandlerRegistry<InMemoryPayload, InMemoryMetadata, S, InMemoryResponse>,
    ) -> (Self, JoinSet<()>) {
        let (consumer_tx, consumer_rx) = mpsc::channel::<(String, InMemoryPayload)>(1000);
        let (subscriber_tx, subscriber_rx) =
            mpsc::channel::<(String, Box<dyn AnyCloneFactory>)>(1000);

        let root_join_set = JoinSet::<()>::new();
        let root_join_set = DefaultInMemoryPublisherBackend::start_dispatcher(
            state.clone(),
            reg.consumers,
            reg.subscribers,
            subscriber_rx,
            consumer_rx,
            root_join_set,
        )
        .await;

        (
            DefaultInMemoryPublisherBackend {
                state: state.clone(),
                handlers: reg.handlers,
                consumer_channel: consumer_tx,
                subscriber_channel: subscriber_tx,
            },
            root_join_set,
        )
    }
}

#[async_trait::async_trait]
impl<S: Clone + Sync + Send + 'static> InMemoryPublisherBackend
    for DefaultInMemoryPublisherBackend<S>
{
    async fn pub_sub(
        &self,
        name: &str,
        msg: Box<dyn AnyCloneFactory>,
    ) -> Result<(), crate::publisher::PublisherError> {
        self.subscriber_channel
            .send((name.to_string(), msg))
            .await
            .unwrap();
        Ok(())
    }

    async fn pub_cons(
        &self,
        name: &str,
        msg: Box<dyn std::any::Any + Send + Sync + 'static>,
    ) -> Result<(), PublisherError> {
        self.consumer_channel
            .send((name.to_string(), InMemoryPayload { payload: msg }))
            .await
            .unwrap();
        Ok(())
    }

    async fn pub_req(
        &self,
        name: &str,
        msg: Box<dyn std::any::Any + Send + Sync + 'static>,
    ) -> Result<Box<dyn std::any::Any + Send + Sync + 'static>, PublisherError> {
        let Some(call) = self.handlers.get(name) else {
            return Err(PublisherError::EndpointNotFound(name.to_string()));
        };
        let ret = call
            .call(
                crate::handlers::HandlerRequest {
                    metadata: InMemoryMetadata {},
                    payload: InMemoryPayload { payload: msg },
                },
                self.state.clone(),
            )
            .await;

        if let Some(report) = ret.report {
            return Err(PublisherError::Eyre(report));
        }

        if !ret.success {
            return Err(PublisherError::Eyre(eyre::eyre!(
                "The response did not indicate success, but also did not provide any Report"
            )));
        }

        let payload = ret.payload.unwrap();
        Ok(payload.response.unwrap())
    }
}
