use tokio::sync::RwLock;

use crate::in_memory_publisher_backend::InMemoryPublisherBackend;

#[derive(thiserror::Error, Debug)]
pub enum PublisherError {
    #[error("could not downcast response type to the actual type")]
    DowncastError,
    #[error("endpoint not found: {0}")]
    EndpointNotFound(String),
    #[error("eyre report: {0}")]
    Eyre(eyre::Report),
    #[error("serialize error: {0}")]
    Serde(String),
}

#[async_trait::async_trait]
pub trait BytePublisherBackend: Send + Sync {
    async fn pub_sub(
        &self,
        name: &str,
        msg: Vec<u8>,
    ) -> Result<(), crate::publisher::PublisherError>;

    async fn pub_cons(&self, name: &str, msg: Vec<u8>) -> Result<(), PublisherError>;

    async fn pub_req(&self, name: &str, msg: Vec<u8>) -> Result<Vec<u8>, PublisherError>;
}

pub enum PublisherBackend {
    InMemory {
        backend: RwLock<Option<Box<dyn InMemoryPublisherBackend>>>,
    },
    ByteBackend {
        backend: Box<dyn BytePublisherBackend>,
    },
}

pub enum PublisherSerializer {
    InMemory,
    Binary,
    Json,
}

pub struct Publisher {
    backend: PublisherBackend,
    serializer: PublisherSerializer,
}

impl Publisher {
    pub fn get_obj_name<T>() -> &'static str {
        std::any::type_name::<T>()
            .split("::")
            .last()
            .expect("there shouln be at least one thingto the name")
    }

    pub fn serialize_req<T: serde::Serialize>(&self, req: &T) -> Result<Vec<u8>, PublisherError> {
        // https://lib.rs/crates/serde_bytes
        match self.serializer {
            PublisherSerializer::InMemory => panic!(),
            PublisherSerializer::Binary => {
                let mut data = Vec::new();
                req.serialize(&mut rmp_serde::Serializer::new(&mut data))
                    .map_err(|x| PublisherError::Serde(x.to_string()))?;
                Ok(data)
            }
            PublisherSerializer::Json => {
                let data =
                    serde_json::to_vec(req).map_err(|x| PublisherError::Serde(x.to_string()))?;
                Ok(data)
            }
        }
    }

    pub fn de_serialize_req<T: serde::de::DeserializeOwned>(
        &self,
        data: &[u8],
    ) -> Result<T, PublisherError> {
        // https://lib.rs/crates/serde_bytes
        match self.serializer {
            PublisherSerializer::InMemory => panic!(),
            PublisherSerializer::Binary => Ok(
                rmp_serde::from_slice(data).map_err(|x| PublisherError::Serde(x.to_string()))?
            ),
            PublisherSerializer::Json => {
                Ok(serde_json::from_slice(data)
                    .map_err(|x| PublisherError::Serde(x.to_string()))?)
            }
        }
    }

    #[cfg(not(feature = "in_memory_only"))]
    pub async fn pub_sub<T: Clone + serde::Serialize + Send + Sync + 'static>(
        &self,
        msg: T,
    ) -> Result<(), PublisherError> {
        use crate::in_memory_publisher_backend::SubscriberCloneFactory;

        let name = Self::get_obj_name::<T>();
        match &self.backend {
            PublisherBackend::InMemory { backend } => {
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
                    .await?;
            }
            PublisherBackend::ByteBackend { backend } => {
                let data = self.serialize_req(&msg)?;
                backend.pub_sub(name, data).await?;
            }
        }
        Ok(())
    }

    #[cfg(not(feature = "in_memory_only"))]
    pub async fn pub_cons<T: serde::Serialize + Send + Sync + 'static>(
        &self,
        msg: T,
    ) -> Result<(), PublisherError> {
        let name = Self::get_obj_name::<T>();
        match &self.backend {
            PublisherBackend::InMemory { backend } => {
                backend
                    .read()
                    .await
                    .as_ref()
                    .unwrap()
                    .pub_cons(name, Box::new(msg))
                    .await?;
            }
            PublisherBackend::ByteBackend { backend } => {
                let data = self.serialize_req(&msg)?;
                backend.pub_cons(name, data).await?;
            }
        }
        Ok(())
    }

    #[cfg(not(feature = "in_memory_only"))]
    pub async fn pub_req<
        TReq: serde::Serialize + Send + Sync + 'static,
        TResp: serde::de::DeserializeOwned + Send + Sync + 'static,
    >(
        &self,
        msg: TReq,
    ) -> Result<TResp, PublisherError> {
        let name = Self::get_obj_name::<TReq>();
        match &self.backend {
            PublisherBackend::InMemory { backend } => {
                let res = backend
                    .read()
                    .await
                    .as_ref()
                    .unwrap()
                    .pub_req(name, Box::new(msg))
                    .await?;
                return match res.downcast::<TResp>() {
                    Ok(x) => Ok(*x),
                    Err(_) => Err(PublisherError::DowncastError),
                };
            }
            PublisherBackend::ByteBackend { backend } => {
                let data = self.serialize_req(&msg)?;
                let res = backend.pub_req(name, data).await?;
                Ok(self.de_serialize_req::<TResp>(res.as_slice())?)
            }
        }
    }

    #[cfg(feature = "in_memory_only")]
    pub async fn pub_sub<T: Clone + Send + Sync + 'static>(
        &self,
        msg: T,
    ) -> Result<(), PublisherError> {
        use crate::in_memory_publisher_backend::SubscriberCloneFactory;

        let name = Self::get_obj_name::<T>();
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
        let name = Self::get_obj_name::<T>();
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
        let name = Self::get_obj_name::<TReq>();
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
            serializer: PublisherSerializer::InMemory,
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
