// features:
// change serializer
// different backends

use tokio::sync::RwLock;

use crate::{
    in_memory::{InMemoryMetadata, InMemoryPayload, InMemoryResponse},
    in_memory_publisher_backend::{DefaultInMemoryPublisherBackend, InMemoryPublisherBackend},
    registry::HandlerRegistry,
};

#[derive(thiserror::Error, Debug)]
pub enum PublisherError {
    #[error("could not downcast response type to the actual type")]
    DowncastError,
    #[error("endpoint not found: {0}")]
    EndpointNotFound(String),
    #[error("eyre report: {0}")]
    Eyre(eyre::Report),
}

#[async_trait::async_trait]
pub trait BytePublisherBackend: Send + Sync {}

pub enum PublisherBackend {
    InMemory {
        backend: RwLock<Option<Box<dyn InMemoryPublisherBackend>>>,
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
        backend
            .read()
            .await
            .as_ref()
            .unwrap()
            .pub_sub(name, Box::new(sub_fact))
            .await
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
        backend
            .read()
            .await
            .as_ref()
            .unwrap()
            .pub_cons(name, Box::new(msg))
            .await
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
        let res = backend
            .read()
            .await
            .as_ref()
            .unwrap()
            .pub_req(name, Box::new(msg))
            .await?;
        match res.downcast::<TResp>() {
            Ok(x) => Ok(*x),
            Err(_) => Err(PublisherError::DowncastError),
        }
    }

    pub async fn new_in_memory() -> Self {
        Self {
            backend: PublisherBackend::InMemory {
                backend: RwLock::new(None),
            },
        }
    }

    pub async fn register_in_memory_backend(
        &self,
        new_backend: impl InMemoryPublisherBackend + 'static,
    ) {
        let PublisherBackend::InMemory { backend } = &self.backend else {
            panic!("This function can only be used for Publishers with in memory backend");
        };

        let mut lock = backend.write().await;
        *lock = Some(Box::new(new_backend));
    }
}
