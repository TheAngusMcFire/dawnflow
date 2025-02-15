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

        let PublisherBackend::InMemory { backend } = &self.backend else {
            panic!()
        };
        let sub_fact = SubscriberCloneFactory::<T> {
            factory: |x| Box::new(x.obj.clone()),
            obj: msg,
        };
        backend.pub_sub(Box::new(sub_fact)).await
    }

    #[cfg(feature = "in_memory_only")]
    pub async fn pub_cons<T: Send + Sync + 'static>(&self, msg: T) -> Result<(), PublisherError> {
        let PublisherBackend::InMemory { backend } = &self.backend else {
            panic!()
        };
        backend.pub_cons(Box::new(msg)).await
    }

    #[cfg(feature = "in_memory_only")]
    pub async fn pub_req<TReq: Send + Sync + 'static, TResp: Send + Sync + 'static>(
        &self,
        msg: TReq,
    ) -> Result<TResp, PublisherError> {
        let PublisherBackend::InMemory { backend } = &self.backend else {
            panic!()
        };
        let res = backend.pub_req(Box::new(msg)).await?;
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
                    subscriber: reg.subscribers,
                }),
            },
        }
    }
}
