// features:
// change serializer
// different backends

use std::any::Any;

use crate::{
    in_memory::{InMemoryMetadata, InMemoryPayload, InMemoryResponse},
    in_memory_publisher_backend::{DefaultInMemoryPublisherBackend, InMemoryPublisherBackend},
    registry::HandlerRegistry,
};

#[derive(thiserror::Error, Debug)]
pub enum PublisherError {
    #[error("could not downcast response type to the actual type")]
    DowncastError,
}

#[async_trait::async_trait]
pub trait BytePublisherBackend: Send + Sync {}

pub enum PublisherBackend {
    InMemory {
        backend: Box<dyn InMemoryPublisherBackend>,
    },
    ByteBackend {
        backend: Box<dyn BytePublisherBackend>,
    },
}

pub struct Publisher {
    backend: PublisherBackend,
}

// #[async_trait::async_trait]
impl Publisher {
    #[cfg(not(feature = "in_memory_only"))]
    pub async fn pub_sub<T: serde::Serialize>(&self, msg: T) -> Result<(), PublisherError> {
        todo!()
    }

    #[cfg(not(feature = "in_memory_only"))]
    pub async fn pub_cons<T: serde::Serialize>(&self, msg: T) -> Result<(), PublisherError> {
        todo!()
    }

    #[cfg(not(feature = "in_memory_only"))]
    pub async fn pub_req<TReq: serde::Serialize, TResp: serde::de::DeserializeOwned>(
        &self,
        msg: TReq,
    ) -> Result<TResp, PublisherError> {
        todo!()
    }

    #[cfg(feature = "in_memory_only")]
    pub async fn pub_sub<T: Clone + Send + Sync + 'static>(
        &self,
        msg: T,
    ) -> Result<(), PublisherError> {
        use crate::in_memory_publisher_backend::SubscriberCloneFactory;

        let name = std::any::type_name::<T>()
            .split("::")
            .last()
            .expect("there shouln be at least one thingto the name");
        let PublisherBackend::InMemory { backend } = &self.backend else {
            panic!()
        };
        let sub_fact = SubscriberCloneFactory::<T> {
            factory: |x| Box::new(x.obj.clone()),
            obj: msg,
        };
        backend.pub_sub(name, Box::new(sub_fact)).await
    }

    #[cfg(feature = "in_memory_only")]
    pub async fn pub_cons<T: Send + Sync + 'static>(&self, msg: T) -> Result<(), PublisherError> {
        let PublisherBackend::InMemory { backend } = &self.backend else {
            panic!()
        };
        let name = std::any::type_name::<T>()
            .split("::")
            .last()
            .expect("there shouln be at least one thingto the name");
        backend.pub_cons(name, Box::new(msg)).await
    }

    #[cfg(feature = "in_memory_only")]
    pub async fn pub_req<TReq: Send + Sync + 'static, TResp: Send + Sync + 'static>(
        &self,
        msg: TReq,
    ) -> Result<TResp, PublisherError> {
        let PublisherBackend::InMemory { backend } = &self.backend else {
            panic!()
        };
        let name = std::any::type_name::<TReq>()
            .split("::")
            .last()
            .expect("there shouln be at least one thingto the name");
        let res = backend.pub_req(name, Box::new(msg)).await?;
        match res.downcast::<TResp>() {
            Ok(x) => Ok(*x),
            Err(_) => Err(PublisherError::DowncastError),
        }
    }

    pub fn new_in_memory<S: 'static>(
        reg: HandlerRegistry<InMemoryPayload, InMemoryMetadata, S, InMemoryResponse>,
    ) -> Self {
        Self {
            backend: PublisherBackend::InMemory {
                backend: Box::new(DefaultInMemoryPublisherBackend {
                    handlers: reg.handlers,
                    consumers: reg.consumers,
                    subscribers: reg.subscribers,
                }),
            },
        }
    }
}
