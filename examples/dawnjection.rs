//! Demonstrates using the `dawnjection` DI container as dawnflow handler state.
//!
//! `I<T>` injects a resolved instance and `R<T>` injects a resolver; both are
//! `FromRequestMetadata` extractors (see `src/dawnjection.rs`), so they can be
//! used as handler parameters. dawnflow requires the *last* handler parameter to
//! be a body extractor, so a `Req<Work>` is taken last.
//!
//! Run with:
//! `cargo run --example dawnjection --no-default-features --features "dawnjection,in_memory_only"`

use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use dawnflow::{
    handlers::FromRequestMetadata,
    in_memory::{InMemoryMetadata, InMemoryPayload, InMemoryResponse},
    in_memory_publisher_backend::DefaultInMemoryPublisherBackend,
    publisher::Publisher,
    registry::HandlerRegistry,
    Req,
};
use dawnjection::{ServiceCollection, ServiceProviderAccess, ServiceProviderContainer, I, R};

pub struct SomeSingleton(pub String);

// The message body that the handler consumes.
pub struct Work {
    pub id: usize,
}

// `I<Arc<AtomicU32>>` and `R<SomeSingleton>` are injected from the DI container;
// `Req<Work>` (the last param) is the deserialized message body.
async fn handler(
    I(counter): I<Arc<AtomicU32>>,
    singleton: R<SomeSingleton>,
    Req(work): Req<Work>,
) -> eyre::Result<()> {
    let n = counter.fetch_add(1, Ordering::SeqCst) + 1;
    tracing::info!(
        "handled Work id={} (count={}), singleton says: {}",
        work.id,
        n,
        singleton.get().0
    );
    Ok(())
}

// The handler state must expose the service provider so the `I`/`R` extractors
// can resolve services. `ServiceProviderContainer` already implements
// `ServiceProviderAccess`, but a handler also needs to extract the state itself.
#[derive(Clone)]
pub struct AppState {
    provider: ServiceProviderContainer,
    publisher: Arc<Publisher>,
}

impl ServiceProviderAccess for AppState {
    fn get_sp_arc(&self) -> &Arc<dawnjection::ServiceProvider> {
        self.provider.get_sp_arc()
    }
}

#[async_trait::async_trait]
impl FromRequestMetadata<AppState, InMemoryMetadata, InMemoryResponse> for AppState {
    type Rejection = eyre::Report;
    async fn from_request_metadata(
        _meta: &mut InMemoryMetadata,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        Ok(state.clone())
    }
}

#[tokio::main]
async fn main() {
    let tr_sub = tracing_subscriber::fmt()
        .compact()
        .with_max_level(tracing::Level::DEBUG)
        .with_target(false)
        .finish();
    tracing::subscriber::set_global_default(tr_sub).unwrap();

    let provider = ServiceProviderContainer(
        ServiceCollection::default()
            .reg_cloneable(Arc::new(AtomicU32::new(0)))
            .reg_singleton(SomeSingleton("singleton message".to_string()))
            .build_service_provider_arc(),
    );

    let handlers = HandlerRegistry::<
        InMemoryPayload,
        InMemoryMetadata,
        AppState,
        InMemoryResponse,
    >::default()
    .register_consumer::<Work, _, _>(handler);

    let state = AppState {
        provider,
        publisher: Arc::new(Publisher::new_in_memory().await),
    };

    let (backend, join_set) = DefaultInMemoryPublisherBackend::new(state.clone(), handlers).await;
    state.publisher.register_in_memory_backend(backend).await;

    state.publisher.pub_cons(Work { id: 1 }).await.unwrap();
    state.publisher.pub_cons(Work { id: 2 }).await.unwrap();

    elegant_departure::tokio::depart().on_termination().await;
    tracing::warn!("shutting down service");
    join_set.join_all().await;
}
