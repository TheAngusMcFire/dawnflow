use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use crate::{
    handlers::{Handler, HandlerRequest, Response},
    publisher::{is_subject_token, Publisher},
};

/// The routing key of a message type, `{type}` or `{type}.{target}`. Panics
/// at registration when a part can't be a NATS subject token: a target with
/// `.`, whitespace or a wildcard would subscribe to other subjects (or break
/// the protocol line), as would a generic type name like `X<A, B>`.
fn routing_key<T>(target: Option<&str>) -> String {
    let base = Publisher::get_obj_name::<T>();
    assert!(
        is_subject_token(base),
        "dawnflow: the type name `{base}` can't be part of a NATS subject"
    );
    match target {
        Some(target) => {
            assert!(
                is_subject_token(target),
                "dawnflow: the target `{target}` can't be part of a NATS subject (no `.`, `*`, `>`, whitespace)"
            );
            format!("{base}.{target}")
        }
        None => base.to_string(),
    }
}

pub type HandlerArc<P, M, S, R> = Arc<dyn HandlerCall<P, M, S, R> + Send + Sync>;

pub struct HandlerRegistry<P, M, S, R> {
    pub consumers: HashMap<String, HandlerArc<P, M, S, R>>,
    pub subscribers: HashMap<String, Vec<HandlerArc<P, M, S, R>>>,
    /// for the requests
    pub handlers: HashMap<String, HandlerArc<P, M, S, R>>,
}

impl<P, M, S, R> Default for HandlerRegistry<P, M, S, R> {
    fn default() -> Self {
        Self {
            handlers: Default::default(),
            consumers: Default::default(),
            subscribers: Default::default(),
        }
    }
}

#[derive(Clone)]
pub struct HandlerEndpoint<T, H: Send> {
    handler: H,
    pd: PhantomData<T>,
}

#[async_trait::async_trait]
impl<
        T: Send + Sync,
        S: Send + 'static,
        P: Send + 'static,
        M: Send + 'static,
        R: Send + 'static,
        H: Handler<T, S, P, M, R> + Send + Sync,
    > HandlerCall<P, M, S, R> for HandlerEndpoint<T, H>
{
    async fn call(&self, req: HandlerRequest<P, M>, state: S) -> Response<R> {
        self.handler.clone().call(req, state).await
    }
}

impl<P: Send + 'static, M: Send + 'static, S: Send + 'static, R: Send + 'static>
    HandlerRegistry<P, M, S, R>
{
    pub fn register_consumer<
        TSub,
        T: Send + Sync + 'static,
        H: Handler<T, S, P, M, R> + Send + Sync + 'static,
    >(
        self,
        handler: H,
    ) -> Self {
        self.register_consumer_with_name(routing_key::<TSub>(None), handler)
    }

    pub fn register_subscriber<
        TSub,
        T: Send + Sync + 'static,
        H: Handler<T, S, P, M, R> + Send + Sync + 'static,
    >(
        self,
        handler: H,
    ) -> Self {
        self.register_subscriber_with_name(routing_key::<TSub>(None), handler)
    }

    pub fn register_handler<
        TSub,
        T: Send + Sync + 'static,
        H: Handler<T, S, P, M, R> + Send + Sync + 'static,
    >(
        self,
        handler: H,
    ) -> Self {
        self.register_handler_with_name(routing_key::<TSub>(None), handler)
    }

    /// Registers a consumer for a specific target receiver. The handler is keyed
    /// under the composite name `{type}.{target}` so it only receives messages
    /// published with the matching target (see `Publisher::pub_cons_with_opts`
    /// and `PubConsOptions::target`).
    pub fn register_consumer_with_target<
        TSub,
        T: Send + Sync + 'static,
        H: Handler<T, S, P, M, R> + Send + Sync + 'static,
    >(
        self,
        target: impl AsRef<str>,
        handler: H,
    ) -> Self {
        self.register_consumer_with_name(routing_key::<TSub>(Some(target.as_ref())), handler)
    }

    /// Registers a subscriber for a specific target receiver. The handler is keyed
    /// under the composite name `{type}.{target}` so it only receives messages
    /// published with the matching target (see `Publisher::pub_sub_with_opts`
    /// and `PubSubOptions::target`).
    pub fn register_subscriber_with_target<
        TSub,
        T: Send + Sync + 'static,
        H: Handler<T, S, P, M, R> + Send + Sync + 'static,
    >(
        self,
        target: impl AsRef<str>,
        handler: H,
    ) -> Self {
        self.register_subscriber_with_name(routing_key::<TSub>(Some(target.as_ref())), handler)
    }

    /// Registers a request handler for a specific target receiver. The handler is
    /// keyed under the composite name `{type}.{target}` so it only receives
    /// requests published with the matching target (see `Publisher::pub_req_with_opts`
    /// and `PubReqOptions::target`).
    pub fn register_handler_with_target<
        TSub,
        T: Send + Sync + 'static,
        H: Handler<T, S, P, M, R> + Send + Sync + 'static,
    >(
        self,
        target: impl AsRef<str>,
        handler: H,
    ) -> Self {
        self.register_handler_with_name(routing_key::<TSub>(Some(target.as_ref())), handler)
    }

    pub fn register_consumer_with_name<
        N: Into<String>,
        T: Send + Sync + 'static,
        H: Handler<T, S, P, M, R> + Send + Sync + 'static,
    >(
        mut self,
        name: N,
        handler: H,
    ) -> Self {
        self.consumers.insert(
            name.into(),
            Arc::new(HandlerEndpoint {
                handler,
                pd: Default::default(),
            }),
        );
        self
    }

    pub fn register_subscriber_with_name<
        N: Into<String>,
        T: Send + Sync + 'static,
        H: Handler<T, S, P, M, R> + Send + Sync + 'static,
    >(
        mut self,
        name: N,
        handler: H,
    ) -> Self {
        let name = name.into();
        let entry = self.subscribers.entry(name).or_default();
        entry.push(Arc::new(HandlerEndpoint {
            handler,
            pd: Default::default(),
        }));
        self
    }

    pub fn register_handler_with_name<
        N: Into<String>,
        T: Send + Sync + 'static,
        H: Handler<T, S, P, M, R> + Send + Sync + 'static,
    >(
        mut self,
        name: N,
        handler: H,
    ) -> Self {
        self.handlers.insert(
            name.into(),
            Arc::new(HandlerEndpoint {
                handler,
                pd: Default::default(),
            }),
        );
        self
    }
}

#[async_trait::async_trait]
pub trait HandlerCall<P, M, S, R>: Send + Sync {
    async fn call(&self, req: HandlerRequest<P, M>, state: S) -> Response<R>;
}
