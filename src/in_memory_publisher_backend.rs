use std::{any::Any, collections::HashMap};

use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
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

pub struct DefaultInMemoryPublisherBackend<S: Sync + Send + 'static> {
    // pub consumers:
    //     HashMap<String, HandlerArc<InMemoryPayload, InMemoryMetadata, S, InMemoryResponse>>,
    // pub subscribers:
    //     HashMap<String, Vec<HandlerArc<InMemoryPayload, InMemoryMetadata, S, InMemoryResponse>>>,
    /// for the requests
    pub handlers:
        HashMap<String, HandlerArc<InMemoryPayload, InMemoryMetadata, S, InMemoryResponse>>,
    pub consumer_channel: tokio::sync::mpsc::Sender<(String, InMemoryPayload)>,
    pub subscriber_channel: tokio::sync::mpsc::Sender<(String, Box<dyn AnyCloneFactory>)>,
    pub sub_join_handle: JoinHandle<()>,
    pub cons_join_handle: JoinHandle<()>,
}

impl<S: Clone + Sync + Send + 'static> DefaultInMemoryPublisherBackend<S> {
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
    ) -> (JoinHandle<()>, JoinHandle<()>) {
        let con_state = state.clone();
        let consumer_join = tokio::spawn(async move {
            while let Some((name, payload)) = cons_rx.recv().await {
                let state = con_state.clone();
                let Some(call) = consumers.get(&name) else {
                    continue;
                };
                let res = call
                    .call(
                        crate::handlers::HandlerRequest {
                            metadata: InMemoryMetadata {},
                            payload,
                        },
                        state,
                    )
                    .await;
                // TODO error handling
            }
        });

        let sub_state = state.clone();
        let subscriber_join = tokio::spawn(async move {
            while let Some((name, payload)) = sub_rx.recv().await {
                let Some(calls) = subscribers.get(&name) else {
                    continue;
                };
                for call in calls {
                    let state = sub_state.clone();
                    let res = call
                        .call(
                            crate::handlers::HandlerRequest {
                                metadata: InMemoryMetadata {},
                                payload: InMemoryPayload {
                                    payload: payload.get_any_clone(),
                                },
                            },
                            state,
                        )
                        .await;
                }
                // println!("GOT = {}", message);
            }
        });

        (consumer_join, subscriber_join)
    }

    pub async fn new(
        state: S,
        reg: HandlerRegistry<InMemoryPayload, InMemoryMetadata, S, InMemoryResponse>,
    ) -> Self {
        let (consumer_tx, consumer_rx) = mpsc::channel::<(String, InMemoryPayload)>(1000);
        let (subscriber_tx, subscriber_rx) =
            mpsc::channel::<(String, Box<dyn AnyCloneFactory>)>(1000);

        let (con_join, sub_join) = DefaultInMemoryPublisherBackend::start_dispatcher(
            state,
            reg.consumers,
            reg.subscribers,
            subscriber_rx,
            consumer_rx,
        )
        .await;

        DefaultInMemoryPublisherBackend {
            handlers: reg.handlers,
            consumer_channel: consumer_tx,
            subscriber_channel: subscriber_tx,
            sub_join_handle: sub_join,
            cons_join_handle: con_join,
        }
    }
}

#[async_trait::async_trait]
impl<S: Sync + Send + 'static> InMemoryPublisherBackend for DefaultInMemoryPublisherBackend<S> {
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
        todo!()
    }

    async fn pub_req(
        &self,
        name: &str,
        msg: Box<dyn std::any::Any + Send + Sync + 'static>,
    ) -> Result<Box<dyn std::any::Any + Send + Sync + 'static>, PublisherError> {
        // self.handlers.
        todo!()
    }
}
