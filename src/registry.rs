use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use crate::handlers::{Handler, HandlerRequest, Response};

pub struct HandlerRegistry<P, M, S, R> {
    pub handlers: HashMap<String, Arc<dyn HandlerCall<P, M, S, R> + Send + Sync>>,
}

impl<P, M, S, R> Default for HandlerRegistry<P, M, S, R> {
    fn default() -> Self {
        Self {
            handlers: Default::default(),
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
    pub fn register<T: Send + Sync + 'static, H: Handler<T, S, P, M, R> + Send + Sync + 'static>(
        self,
        handler: H,
    ) -> Self {
        let name = std::any::type_name::<H>()
            .split("::")
            .last()
            .expect("there shouln be at least one thingto the name");
        self.register_with_name(name, handler)
    }

    pub fn register_with_name<
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
