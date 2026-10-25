use tokio::sync::RwLock;

use crate::in_memory_publisher_backend::InMemoryPublisherBackend;
#[cfg(feature = "nats")]
use crate::nats::publisher_backend::NatsPublisherBackend;

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
    #[cfg(feature = "nats")]
    #[error("nats publish error: {0}")]
    NatsPublishingError(#[from] async_nats::PublishError),
    #[cfg(feature = "nats")]
    #[error("nats request error: {0}")]
    NatsRequestError(#[from] async_nats::RequestError),
    #[cfg(feature = "nats")]
    #[error("nats flush error: {0}")]
    NatsFlushError(#[from] async_nats::client::FlushError),
    #[error("operation timed out")]
    Timeout,
    #[error("`{0}` can't be part of a NATS subject (no `.`, `*`, `>`, whitespace)")]
    InvalidSubject(String),
    /// the request reached a handler, which failed (or its reply couldn't be sent)
    #[error("the handler failed: {0}")]
    Remote(String),
}

/// Whether `token` can be one segment of a NATS subject: not empty, not the
/// wildcard `*` or `>`, no `.`, whitespace or control characters. Always true
/// for the in-memory backend, which has no subjects.
pub fn is_subject_token(token: &str) -> bool {
    !cfg!(feature = "nats")
        || (!token.is_empty()
            && token != "*"
            && token != ">"
            && !token
                .chars()
                .any(|c| c == '.' || c.is_whitespace() || c.is_control()))
}

pub struct PubSubOptions {
    /// Optional target receiver name. When set, the message is routed to the
    /// composite key `{type}.{target}` so it reaches only handlers registered
    /// for that target (see `HandlerRegistry::register_subscriber_with_target`).
    /// The target becomes part of a NATS subject, so it must not contain NATS
    /// subject separators/wildcards (` `, `.`, `*`, `>`).
    pub target: Option<String>,
}

impl Default for PubSubOptions {
    fn default() -> Self {
        Self { target: None }
    }
}

impl PubSubOptions {
    pub fn target(mut self, t: impl Into<String>) -> Self {
        self.target = Some(t.into());
        self
    }
}

pub struct PubConsOptions {
    /// Optional target receiver name. When set, the message is routed to the
    /// composite key `{type}.{target}` so it reaches only the consumer
    /// registered for that target (see `HandlerRegistry::register_consumer_with_target`).
    /// The target becomes part of a NATS subject, so it must not contain NATS
    /// subject separators/wildcards (` `, `.`, `*`, `>`).
    pub target: Option<String>,
}

impl Default for PubConsOptions {
    fn default() -> Self {
        Self { target: None }
    }
}

impl PubConsOptions {
    pub fn target(mut self, t: impl Into<String>) -> Self {
        self.target = Some(t.into());
        self
    }
}

pub struct PubReqOptions {
    pub timeout: Option<std::time::Duration>,
    /// Optional target receiver name. When set, the request is routed to the
    /// composite key `{type}.{target}` so it reaches only the handler
    /// registered for that target (see `HandlerRegistry::register_handler_with_target`).
    /// The target becomes part of a NATS subject, so it must not contain NATS
    /// subject separators/wildcards (` `, `.`, `*`, `>`).
    pub target: Option<String>,
}

impl Default for PubReqOptions {
    fn default() -> Self {
        Self {
            timeout: None,
            target: None,
        }
    }
}

impl PubReqOptions {
    pub fn with_timeout(mut self, d: std::time::Duration) -> Self {
        self.timeout = Some(d);
        self
    }

    pub fn timeout_secs(mut self, secs: u64) -> Self {
        self.timeout = Some(std::time::Duration::from_secs(secs));
        self
    }

    pub fn target(mut self, t: impl Into<String>) -> Self {
        self.target = Some(t.into());
        self
    }
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
    async fn flush(&self) -> Result<(), PublisherError>;

    async fn pub_sub_with_opts(
        &self,
        name: &str,
        msg: Vec<u8>,
        opts: &PubSubOptions,
    ) -> Result<(), PublisherError>;

    async fn pub_cons_with_opts(
        &self,
        name: &str,
        msg: Vec<u8>,
        opts: &PubConsOptions,
    ) -> Result<(), PublisherError>;

    async fn pub_req_with_opts(
        &self,
        name: &str,
        msg: Vec<u8>,
        opts: &PubReqOptions,
    ) -> Result<Vec<u8>, PublisherError>;
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

    /// Computes the routing key for a message. Without a target this is just the
    /// type name (no allocation); with a target it is the composite `{type}.{target}`.
    fn routing_name<T>(
        target: &Option<String>,
    ) -> Result<std::borrow::Cow<'static, str>, PublisherError> {
        let base = Self::get_obj_name::<T>();
        if !is_subject_token(base) {
            return Err(PublisherError::InvalidSubject(base.to_string()));
        }
        match target {
            Some(t) if !is_subject_token(t) => Err(PublisherError::InvalidSubject(t.clone())),
            Some(t) => Ok(std::borrow::Cow::Owned(format!("{base}.{t}"))),
            None => Ok(std::borrow::Cow::Borrowed(base)),
        }
    }

    pub fn serialize_req<T: serde::Serialize>(&self, req: &T) -> Result<Vec<u8>, PublisherError> {
        // https://lib.rs/crates/serde_bytes
        match self.serializer {
            PublisherSerializer::InMemory => panic!(),
            PublisherSerializer::Binary => {
                let mut data = Vec::new();
                req.serialize(&mut rmp_serde::Serializer::new(&mut data).with_struct_map())
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

    pub async fn flush(&self) -> Result<(), PublisherError> {
        match &self.backend {
            PublisherBackend::InMemory { .. } => Ok(()),
            PublisherBackend::ByteBackend { backend } => backend.flush().await,
        }
    }

    // --- non-in_memory_only variants ---

    #[cfg(not(feature = "in_memory_only"))]
    pub async fn pub_sub<T: Clone + serde::Serialize + Send + Sync + 'static>(
        &self,
        msg: T,
    ) -> Result<(), PublisherError> {
        self.pub_sub_with_opts(msg, |x| x).await
    }

    #[cfg(not(feature = "in_memory_only"))]
    pub async fn pub_cons<T: serde::Serialize + Send + Sync + 'static>(
        &self,
        msg: T,
    ) -> Result<(), PublisherError> {
        self.pub_cons_with_opts(msg, |x| x).await
    }

    #[cfg(not(feature = "in_memory_only"))]
    pub async fn pub_req<
        TReq: serde::Serialize + Send + Sync + 'static,
        TResp: serde::de::DeserializeOwned + Send + Sync + 'static,
    >(
        &self,
        msg: TReq,
    ) -> Result<TResp, PublisherError> {
        self.pub_req_with_opts(msg, |x| x).await
    }

    #[cfg(not(feature = "in_memory_only"))]
    pub async fn pub_sub_with_opts<T: Clone + serde::Serialize + Send + Sync + 'static>(
        &self,
        msg: T,
        f: impl FnOnce(PubSubOptions) -> PubSubOptions,
    ) -> Result<(), PublisherError> {
        use crate::in_memory_publisher_backend::SubscriberCloneFactory;

        let opts = f(PubSubOptions::default());
        let name = Self::routing_name::<T>(&opts.target)?;
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
                    .pub_sub_with_opts(&name, Box::new(sub_fact), &opts)
                    .await?;
            }
            PublisherBackend::ByteBackend { backend } => {
                let data = self.serialize_req(&msg)?;
                backend.pub_sub_with_opts(&name, data, &opts).await?;
            }
        }
        Ok(())
    }

    #[cfg(not(feature = "in_memory_only"))]
    pub async fn pub_cons_with_opts<T: serde::Serialize + Send + Sync + 'static>(
        &self,
        msg: T,
        f: impl FnOnce(PubConsOptions) -> PubConsOptions,
    ) -> Result<(), PublisherError> {
        let opts = f(PubConsOptions::default());
        let name = Self::routing_name::<T>(&opts.target)?;
        match &self.backend {
            PublisherBackend::InMemory { backend } => {
                backend
                    .read()
                    .await
                    .as_ref()
                    .unwrap()
                    .pub_cons_with_opts(&name, Box::new(msg), &opts)
                    .await?;
            }
            PublisherBackend::ByteBackend { backend } => {
                let data = self.serialize_req(&msg)?;
                backend.pub_cons_with_opts(&name, data, &opts).await?;
            }
        }
        Ok(())
    }

    #[cfg(not(feature = "in_memory_only"))]
    pub async fn pub_req_with_opts<
        TReq: serde::Serialize + Send + Sync + 'static,
        TResp: serde::de::DeserializeOwned + Send + Sync + 'static,
    >(
        &self,
        msg: TReq,
        f: impl FnOnce(PubReqOptions) -> PubReqOptions,
    ) -> Result<TResp, PublisherError> {
        let opts = f(PubReqOptions::default());
        let name = Self::routing_name::<TReq>(&opts.target)?;
        match &self.backend {
            PublisherBackend::InMemory { backend } => {
                let res = backend
                    .read()
                    .await
                    .as_ref()
                    .unwrap()
                    .pub_req_with_opts(&name, Box::new(msg), &opts)
                    .await?;
                match res.downcast::<TResp>() {
                    Ok(x) => Ok(*x),
                    Err(_) => Err(PublisherError::DowncastError),
                }
            }
            PublisherBackend::ByteBackend { backend } => {
                let data = self.serialize_req(&msg)?;
                let res = backend.pub_req_with_opts(&name, data, &opts).await?;
                Ok(self.de_serialize_req::<TResp>(res.as_slice())?)
            }
        }
    }

    // --- in_memory_only variants ---

    #[cfg(feature = "in_memory_only")]
    pub async fn pub_sub<T: Clone + Send + Sync + 'static>(
        &self,
        msg: T,
    ) -> Result<(), PublisherError> {
        self.pub_sub_with_opts(msg, |x| x).await
    }

    #[cfg(feature = "in_memory_only")]
    pub async fn pub_cons<T: Send + Sync + 'static>(&self, msg: T) -> Result<(), PublisherError> {
        self.pub_cons_with_opts(msg, |x| x).await
    }

    #[cfg(feature = "in_memory_only")]
    pub async fn pub_req<TReq: Send + Sync + 'static, TResp: Send + Sync + 'static>(
        &self,
        msg: TReq,
    ) -> Result<TResp, PublisherError> {
        self.pub_req_with_opts::<TReq, TResp>(msg, |x| x).await
    }

    #[cfg(feature = "in_memory_only")]
    pub async fn pub_sub_with_opts<T: Clone + Send + Sync + 'static>(
        &self,
        msg: T,
        f: impl FnOnce(PubSubOptions) -> PubSubOptions,
    ) -> Result<(), PublisherError> {
        use crate::in_memory_publisher_backend::SubscriberCloneFactory;

        let opts = f(PubSubOptions::default());
        let name = Self::routing_name::<T>(&opts.target)?;
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
            .pub_sub_with_opts(&name, Box::new(sub_fact), &opts)
            .await
    }

    #[cfg(feature = "in_memory_only")]
    pub async fn pub_cons_with_opts<T: Send + Sync + 'static>(
        &self,
        msg: T,
        f: impl FnOnce(PubConsOptions) -> PubConsOptions,
    ) -> Result<(), PublisherError> {
        let opts = f(PubConsOptions::default());
        let PublisherBackend::InMemory { backend } = &self.backend else {
            panic!()
        };
        let name = Self::routing_name::<T>(&opts.target)?;
        backend
            .read()
            .await
            .as_ref()
            .unwrap()
            .pub_cons_with_opts(&name, Box::new(msg), &opts)
            .await
    }

    #[cfg(feature = "in_memory_only")]
    pub async fn pub_req_with_opts<TReq: Send + Sync + 'static, TResp: Send + Sync + 'static>(
        &self,
        msg: TReq,
        f: impl FnOnce(PubReqOptions) -> PubReqOptions,
    ) -> Result<TResp, PublisherError> {
        let opts = f(PubReqOptions::default());
        let PublisherBackend::InMemory { backend } = &self.backend else {
            panic!()
        };
        let name = Self::routing_name::<TReq>(&opts.target)?;
        let res = backend
            .read()
            .await
            .as_ref()
            .unwrap()
            .pub_req_with_opts(&name, Box::new(msg), &opts)
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

    #[cfg(feature = "nats")]
    pub async fn new_nats_publisher(nats_url: &str) -> Result<Self, async_nats::Error> {
        Ok(Self {
            backend: PublisherBackend::ByteBackend {
                backend: Box::new(NatsPublisherBackend::new(nats_url).await?),
            },
            serializer: PublisherSerializer::Binary,
        })
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    #[test]
    fn subject_tokens() {
        assert!(is_subject_token("ModelTestRequest"));
        assert!(is_subject_token("stack-1_a"));
        for invalid in ["", "a.b", "a b", "*", ">", "a\r\nPUB x 1", "X<A, B>"] {
            assert!(!is_subject_token(invalid), "{invalid:?}");
        }
        assert!(is_subject_token("Foo>"), "a generic type's last segment");
    }

    #[test]
    fn invalid_targets_and_type_names_are_refused() {
        struct Plain;
        struct Generic<A, B>(A, B);
        assert!(Publisher::routing_name::<Plain>(&None).is_ok());
        assert_eq!(
            Publisher::routing_name::<Plain>(&Some("t1".into())).unwrap(),
            "Plain.t1"
        );
        assert!(matches!(
            Publisher::routing_name::<Plain>(&Some("a b".into())),
            Err(PublisherError::InvalidSubject(_))
        ));
        assert!(matches!(
            Publisher::routing_name::<Generic<u8, u16>>(&None),
            Err(PublisherError::InvalidSubject(_))
        ));
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct V1 {
        limit: u64,
        spent: u64,
    }

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct V2 {
        spent: u64,
        limit: u64,
        #[serde(default)]
        note: Option<String>,
    }

    fn named<T: Serialize>(value: &T) -> Vec<u8> {
        let mut data = Vec::new();
        value
            .serialize(&mut rmp_serde::Serializer::new(&mut data).with_struct_map())
            .unwrap();
        data
    }

    #[test]
    fn named_fields_survive_added_and_reordered_fields() {
        let v1 = V1 { limit: 100, spent: 5 };
        let v2: V2 = rmp_serde::from_slice(&named(&v1)).unwrap();
        assert_eq!((v2.limit, v2.spent, v2.note.as_deref()), (100, 5, None));
        let back: V1 = rmp_serde::from_slice(&named(&v2)).unwrap();
        assert_eq!(back, v1);
        let from_old_sender: V1 = rmp_serde::from_slice(&rmp_serde::to_vec(&v1).unwrap()).unwrap();
        assert_eq!(from_old_sender, v1, "arrays of older senders still decode");
    }
}
