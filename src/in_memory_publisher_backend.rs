use std::{any::Any, collections::HashMap};

use tokio::{
    select,
    sync::mpsc,
    task::{JoinError, JoinHandle, JoinSet},
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
    pub fn handle_join_result(res: Result<Response<InMemoryResponse>, JoinError>) {}

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
        // todo use joinsets to execute incoming requests in parallel
        // todo handle conjestions
        let con_state = state.clone();
        join_set.spawn(async move {
            let guard = elegant_departure::get_shutdown_guard();
            let mut join_set = JoinSet::<Response<InMemoryResponse>>::new();

            loop {
                let (name, payload) = if !join_set.is_empty() {
                    select! {
                        Some(r) = cons_rx.recv() => r,
                        Some(jh) = join_set.join_next() => {
                            DefaultInMemoryPublisherBackend::<S>::handle_join_result(jh);
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
                    todo!("handle error")
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
        });

        let sub_state = state.clone();
        join_set.spawn(async move {
            let guard = elegant_departure::get_shutdown_guard();
            let mut join_set = JoinSet::<Response<InMemoryResponse>>::new();

            loop {
                let (name, payload) = select! {
                    r = sub_rx.recv() => {
                        let Some(p) = r else {
                            tracing::debug!("stop");
                            break
                        };
                        p
                    },
                    Some(jh) = join_set.join_next() => {
                        // let Some(jh) = jh else {
                        //     tracing::debug!("stop");
                        //     continue;
                        // };
                        DefaultInMemoryPublisherBackend::<S>::handle_join_result(jh);
                        continue;
                    },
                    _ = guard.wait() => {
                        while let Some(r) = join_set.join_next().await {
                            DefaultInMemoryPublisherBackend::<S>::handle_join_result(r);
                        }
                        break;
                            tracing::debug!("stop");
                    },
                };

                let Some(calls) = subscribers.get(&name) else {
                    todo!("handle error")
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
