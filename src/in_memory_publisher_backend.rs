use std::{collections::HashMap, sync::Arc};

use crate::{
    in_memory::{InMemoryMetadata, InMemoryPayload, InMemoryResponse},
    publisher::PublisherError,
    registry::HandlerCall,
};

#[async_trait::async_trait]
pub trait InMemoryPublisherBackend: Send + Sync {
    async fn pub_sub(
        &self,
        msg: Box<dyn std::any::Any + Send + Sync + 'static>,
    ) -> Result<(), crate::publisher::PublisherError>;

    async fn pub_cons(
        &self,
        msg: Box<dyn std::any::Any + Send + Sync + 'static>,
    ) -> Result<(), PublisherError>;

    async fn pub_req(
        &self,
        msg: Box<dyn std::any::Any + Send + Sync + 'static>,
    ) -> Result<Box<dyn std::any::Any + Send + Sync + 'static>, PublisherError>;
}

pub struct DefaultInMemoryPublisherBackend<S> {
    pub handlers: HashMap<
        String,
        Arc<dyn HandlerCall<InMemoryPayload, InMemoryMetadata, S, InMemoryResponse> + Send + Sync>,
    >,
}

#[async_trait::async_trait]
impl<S> InMemoryPublisherBackend for DefaultInMemoryPublisherBackend<S> {
    async fn pub_sub(
        &self,
        msg: Box<dyn std::any::Any + Send + Sync + 'static>,
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
        msg: Box<dyn std::any::Any + Send + Sync + 'static>,
    ) -> Result<(), PublisherError> {
        todo!()
    }

    async fn pub_req(
        &self,
        msg: Box<dyn std::any::Any + Send + Sync + 'static>,
    ) -> Result<Box<dyn std::any::Any + Send + Sync + 'static>, PublisherError> {
        todo!()
    }
}
