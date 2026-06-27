//! Integration with the [`dawnjection`](https://github.com/TheAngusMcFire/dawnjection)
//! dependency-injection container.
//!
//! This lives behind the `dawnjection` feature so that dawnflow can ship the
//! integration in lock-step with its own trait changes, without dawnjection
//! needing to depend on (and be re-released against) dawnflow.
//!
//! It wires dawnjection's `I<T>` (injected instance) and `R<T>` (resolver)
//! extractors into dawnflow's [`FromRequestMetadata`] trait, so they can be
//! used as handler parameters whenever the handler state implements
//! [`ServiceProviderAccess`].

use crate::handlers::FromRequestMetadata;

use dawnjection::{ServiceProviderAccess, ServiceProviderContainer, I, R};

#[async_trait::async_trait]
impl<S, M, T: Send + Sync + 'static, Resp: Send + Sync + 'static> FromRequestMetadata<S, M, Resp>
    for I<T>
where
    S: ServiceProviderAccess + Sync + Send,
{
    type Rejection = eyre::Report;
    async fn from_request_metadata(_meta: &mut M, state: &S) -> Result<Self, Self::Rejection> {
        let ret = state.get_sp_arc().get::<T>()?;
        Ok(I(ret))
    }
}

#[async_trait::async_trait]
impl<S, M, T: Send + Sync + 'static, Resp: Send + Sync + 'static> FromRequestMetadata<S, M, Resp>
    for R<T>
where
    S: ServiceProviderAccess + Sync + Send,
{
    type Rejection = eyre::Report;
    async fn from_request_metadata(_meta: &mut M, state: &S) -> Result<Self, Self::Rejection> {
        Ok(R::new(ServiceProviderContainer(state.get_sp_arc().clone())))
    }
}
