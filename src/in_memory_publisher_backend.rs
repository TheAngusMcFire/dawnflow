use std::{any::Any, collections::HashMap, sync::Arc};

use crate::{
    in_memory::{InMemoryMetadata, InMemoryPayload, InMemoryResponse},
    publisher::PublisherError,
    registry::{HandlerArc, HandlerCall},
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

pub struct DefaultInMemoryPublisherBackend<S> {
    pub consumers:
        HashMap<String, HandlerArc<InMemoryPayload, InMemoryMetadata, S, InMemoryResponse>>,
    pub subscribers:
        HashMap<String, Vec<HandlerArc<InMemoryPayload, InMemoryMetadata, S, InMemoryResponse>>>,
    /// for the requests
    pub handlers:
        HashMap<String, HandlerArc<InMemoryPayload, InMemoryMetadata, S, InMemoryResponse>>,
}

#[async_trait::async_trait]
impl<S> InMemoryPublisherBackend for DefaultInMemoryPublisherBackend<S> {
    async fn pub_sub(
        &self,
        name: &str,
        msg: Box<dyn AnyCloneFactory>,
    ) -> Result<(), crate::publisher::PublisherError> {
        // create payload and dispatch handler
        //
        // match msg.downcast::<dyn Clone>() {
        //     Ok(_) => todo!(),
        //     Err(_) => todo!(),
        // };

        todo!()
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
